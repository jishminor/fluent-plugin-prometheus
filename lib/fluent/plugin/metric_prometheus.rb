require 'fluent/plugin/output'
require 'fluent/plugin/prometheus'

module Fluent::Plugin
  class PrometheusOutput < Fluent::Plugin::Output
    Fluent::Plugin.register_output('prometheus_metric', self)
    include Fluent::Plugin::PrometheusLabelParser
    include Fluent::Plugin::Prometheus

    def initialize
      super
      @registry = ::Prometheus::Client.registry
    end

    def multi_workers_ready?
      true
    end

    config_param :key, :string

    def labels(record, expander)
      label = {}
      labels.each do |k, v|
        if v.is_a?(String)
          label[k] = expander.expand(v)
        else
          label[k] = v.call(record)
        end
      end
      label
    end

    def configure(conf)
      super
      @labels = parse_labels_elements(conf)
      @placeholder_values = {}
      @placeholder_expander_builder = Fluent::Plugin::Prometheus.placeholder_expander(log)
      @hostname = Socket.gethostname
    end

    def process(tag, es)
      placeholder_values = {
        'tag' => tag,
        'hostname' => @hostname,
        'worker_id' => fluentd_worker_id,
      }
      
      # Create metric if not exists
      begin
        gauge = registry.gauge(tag.to_sym)
      rescue ::Prometheus::Client::Registry::AlreadyRegisteredError
        gauge = registry.get(tag.to_sym)
      end

      # Write out values in event stream to Registry
      es.each do |time, record|
        placeholders = record.merge(placeholder_values)
        expander = @placeholder_expander_builder.build(placeholders)
        if @key.is_a?(String)
          value = record[@key]
        else
          value = @key.call(record)
        end
        if value
          gauge.set(labels(record, expander), value)
        end
      end
    end

  end
end
