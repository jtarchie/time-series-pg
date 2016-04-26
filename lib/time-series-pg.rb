require 'time-series-pg/version'
require 'time-series-pg/metric'

module TimeSeriesPg
  def self.metric(name, &block)
    Metric.create(name, &block)
  end
end
