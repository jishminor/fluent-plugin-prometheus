Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-prometheus-smarter"
  spec.version       = "1.8.3"
  spec.authors       = ["Masahiro Sano", "Josh Minor"]
  spec.email         = ["sabottenda@gmail.com"]
  spec.summary       = %q{A fluent plugin that collects metrics and exposes for Prometheus.}
  spec.description   = %q{A fluent plugin that collects metrics and exposes for Prometheus.}
  spec.homepage      = "https://github.com/jishminor/fluent-plugin-prometheus"
  spec.license       = "Apache-2.0"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "fluentd", ">= 1.9.1", "< 2"
  spec.add_dependency "prometheus-client", "< 0.10"
  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "rspec"
  spec.add_development_dependency "test-unit"
end
