require "active_record/connection_adapters/redshift_adapter"
require "activerecord-import/adapters/redshift_adapter"

class ActiveRecord::ConnectionAdapters::RedshiftAdapter
  include ActiveRecord::Import::RedshiftAdapter
end

class ActiveRecord::Base
  class << self
    def import_helper( *args )
      options = { validate: true, timestamps: true }
      options.merge!( args.pop ) if args.last.is_a? Hash
      # making sure that current model's primary key is used
      options[:primary_key] = primary_key
      options[:locking_column] = locking_column if attribute_names.include?(locking_column)

      is_validating = options[:validate_with_context].present? ? true : options[:validate]
      validator = ActiveRecord::Import::Validator.new(self, options)

      # assume array of model objects
      if args.last.is_a?( Array ) && args.last.first.is_a?(ActiveRecord::Base)
        if args.length == 2
          models = args.last
          column_names = args.first.dup
        else
          models = args.first
          column_names = if connection.respond_to?(:supports_virtual_columns?) && connection.supports_virtual_columns?
            columns.reject(&:virtual?).map(&:name)
          else
            self.column_names.dup
          end
        end

        if models.first.id.nil?
          Array(primary_key).each do |c|
            if column_names.include?(c) && columns_hash[c].type == :uuid
              column_names.delete(c)
            end
          end
        end

        # Redshift does not support identity inserts
        if connection.adapter_name.downcase.to_sym == :redshift
          column_names.delete(primary_key)
        end

        update_attrs = if record_timestamps && options[:timestamps]
          if respond_to?(:timestamp_attributes_for_update, true)
            send(:timestamp_attributes_for_update).map(&:to_sym)
          else
            new.send(:timestamp_attributes_for_update_in_model)
          end
        end

        array_of_attributes = []

        models.each do |model|
          if supports_setting_primary_key_of_imported_objects?
            load_association_ids(model)
          end

          if is_validating && !validator.valid_model?(model)
            raise(ActiveRecord::RecordInvalid, model) if options[:raise_error]
            next
          end

          array_of_attributes << column_names.map do |name|
            if model.persisted? &&
               update_attrs && update_attrs.include?(name.to_sym) &&
               !model.send("#{name}_changed?")
              nil
            else
              model.read_attribute(name.to_s)
            end
          end
        end
        # supports array of hash objects
      elsif args.last.is_a?( Array ) && args.last.first.is_a?(Hash)
        if args.length == 2
          array_of_hashes = args.last
          column_names = args.first.dup
          allow_extra_hash_keys = true
        else
          array_of_hashes = args.first
          column_names = array_of_hashes.first.keys
          allow_extra_hash_keys = false
        end

        array_of_attributes = array_of_hashes.map do |h|
          error_message = validate_hash_import(h, column_names, allow_extra_hash_keys)

          raise ArgumentError, error_message if error_message

          column_names.map do |key|
            h[key]
          end
        end
        # supports empty array
      elsif args.last.is_a?( Array ) && args.last.empty?
        return ActiveRecord::Import::Result.new([], 0, [])
        # supports 2-element array and array
      elsif args.size == 2 && args.first.is_a?( Array ) && args.last.is_a?( Array )

        unless args.last.first.is_a?(Array)
          raise ArgumentError, "Last argument should be a two dimensional array '[[]]'. First element in array was a #{args.last.first.class}"
        end

        column_names, array_of_attributes = args

        # dup the passed args so we don't modify unintentionally
        column_names = column_names.dup
        array_of_attributes = array_of_attributes.map(&:dup)
      else
        raise ArgumentError, "Invalid arguments!"
      end

      # Force the primary key col into the insert if it's not
      # on the list and we are using a sequence and stuff a nil
      # value for it into each row so the sequencer will fire later
      symbolized_column_names = Array(column_names).map(&:to_sym)
      symbolized_primary_key = Array(primary_key).map(&:to_sym)

      if !symbolized_primary_key.to_set.subset?(symbolized_column_names.to_set) && connection.prefetch_primary_key? && sequence_name
        column_count = column_names.size
        column_names.concat(Array(primary_key)).uniq!
        columns_added = column_names.size - column_count
        new_fields = Array.new(columns_added)
        array_of_attributes.each { |a| a.concat(new_fields) }
      end

      # Don't modify incoming arguments
      on_duplicate_key_update = options[:on_duplicate_key_update]
      if on_duplicate_key_update
        updatable_columns = symbolized_column_names.reject { |c| symbolized_primary_key.include? c }
        options[:on_duplicate_key_update] = if on_duplicate_key_update.is_a?(Hash)
          on_duplicate_key_update.each_with_object({}) do |(k, v), duped_options|
            duped_options[k] = if k == :columns && v == :all
              updatable_columns
            elsif v.duplicable?
              v.dup
            else
              v
            end
          end
        elsif on_duplicate_key_update == :all
          updatable_columns
        elsif on_duplicate_key_update.duplicable?
          on_duplicate_key_update.dup
        else
          on_duplicate_key_update
        end
      end

      timestamps = {}

      # record timestamps unless disabled in ActiveRecord::Base
      if record_timestamps && options[:timestamps]
        timestamps = add_special_rails_stamps column_names, array_of_attributes, options
      end

      return_obj = if is_validating
        import_with_validations( column_names, array_of_attributes, options ) do |failed_instances|
          if models
            models.each { |m| failed_instances << m if m.errors.any? }
          else
            # create instances for each of our column/value sets
            arr = validations_array_for_column_names_and_attributes( column_names, array_of_attributes )

            # keep track of the instance and the position it is currently at. if this fails
            # validation we'll use the index to remove it from the array_of_attributes
            arr.each_with_index do |hsh, i|
              model = new
              hsh.each_pair { |k, v| model[k] = v }
              next if validator.valid_model?(model)
              raise(ActiveRecord::RecordInvalid, model) if options[:raise_error]
              array_of_attributes[i] = nil
              failure = model.dup
              failure.errors.send(:initialize_dup, model.errors)
              failed_instances << failure
            end
            array_of_attributes.compact!
          end
        end
      else
        import_without_validations_or_callbacks( column_names, array_of_attributes, options )
      end

      if options[:synchronize]
        sync_keys = options[:synchronize_keys] || Array(primary_key)
        synchronize( options[:synchronize], sync_keys)
      end
      return_obj.num_inserts = 0 if return_obj.num_inserts.nil?

      # if we have ids, then set the id on the models and mark the models as clean.
      if models && supports_setting_primary_key_of_imported_objects?
        set_attributes_and_mark_clean(models, return_obj, timestamps, options)

        # if there are auto-save associations on the models we imported that are new, import them as well
        import_associations(models, options.dup) if options[:recursive]
      end

      return_obj
    end
  end
end
