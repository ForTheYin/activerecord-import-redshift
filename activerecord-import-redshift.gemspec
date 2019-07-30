require File.expand_path('../lib/activerecord-import-redshift/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Andy Yin"]
  gem.email         = ["dev@fortheyin.com"]
  gem.summary       = "Bulk insert extension for Redshift"
  gem.description   = "A library for bulk inserting data to Redshift. Requires the activerecord-import gem."
  gem.homepage      = "https://github.com/ForTheYin/activerecord-import-redshift"
  gem.license       = "MIT"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map { |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "activerecord-import-redshift"
  gem.require_paths = ["lib"]
  gem.version       = ActiveRecord::Import::Redshift::VERSION

  gem.required_ruby_version = ">= 2.3"

  gem.add_runtime_dependency "activerecord-import", ">= 0.18"
  gem.add_development_dependency "rake"
end
