module ActiveRecord::Import::RedshiftAdapter
  include ActiveRecord::Import::ImportSupport

  def insert_many(sql, values, options = {}, *args) # :nodoc:
    number_of_inserts = 1
    returned_values = []
    ids = []
    results = []

    base_sql, post_sql = if sql.is_a?( String )
                           [sql, '']
                         elsif sql.is_a?( Array )
                           [sql.shift, sql.join( ' ' )]
                         end

    sql2insert = base_sql + values.join( ',' ) + post_sql
    insert( sql2insert, *args )

    if options[:returning].blank?
      ids = returned_values
    elsif options[:primary_key].blank?
      results = returned_values
    else
      # split primary key and returning columns
      ids, results = split_ids_and_results(returned_values, columns, options)
    end

    ActiveRecord::Import::Result.new([], number_of_inserts, ids, results)
  end

  def pre_sql_statements(options)
    sql = []
    sql << options[:pre_sql] if options[:pre_sql]
    sql << options[:command] if options[:command]

    # Add keywords like IGNORE or DELAYED
    if options[:keywords].is_a?(Array)
      sql.concat(options[:keywords])
    elsif options[:keywords]
      sql << options[:keywords].to_s
    end

    sql
  end

  def split_ids_and_results(values, columns, options)
    ids = []
    results = []
    id_indexes = Array(options[:primary_key]).map { |key| columns.index(key) }
    returning_indexes = Array(options[:returning]).map { |key| columns.index(key) }

    values.each do |value|
      value_array = Array(value)
      ids << id_indexes.map { |i| value_array[i] }
      results << returning_indexes.map { |i| value_array[i] }
    end

    ids.map!(&:first) if id_indexes.size == 1
    results.map!(&:first) if returning_indexes.size == 1

    [ids, results]
  end

  def next_value_for_sequence(sequence_name)
    nil
  end

  def post_sql_statements(table_name, options) # :nodoc:
    sql = []

    if logger && options[:on_duplicate_key_ignore] && !options[:on_duplicate_key_update]
      logger.warn "Ignoring on_duplicate_key_ignore because it is not supported by the database."
    end

    sql
  end

  def supports_on_duplicate_key_update?
    false
  end

  def supports_setting_primary_key_of_imported_objects?
    false
  end
end
