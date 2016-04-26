require_relative 'calculations'
require_relative 'fields'

module TimeSeriesPg
  class Metric < Struct.new(:name)
    def self.create(name, &block)
      metric = new(name)
      metric.instance_eval(&block)
      metric
    end

    def fields(&block)
      @fields ||= Fields.create(&block)
    end

    def calculations
      @calculations ||= Calculations.new self, fields
    end

    def to_sql
      %{
          CREATE OR REPLACE FUNCTION round_timestamp(rounder interval, ts timestamptz)
            RETURNS timestamptz AS $$
            DECLARE
                    _mystamp timestamp;
                    _round_secs decimal;
            BEGIN
              _round_secs := EXTRACT(EPOCH FROM rounder)::decimal;
              _mystamp := timestamptz 'epoch'
                + FLOOR((EXTRACT(EPOCH FROM ts))::int / _round_secs) * _round_secs
                * INTERVAL '1 second';

              RETURN _mystamp;
        END; $$ LANGUAGE plpgsql IMMUTABLE;

        CREATE TABLE IF NOT EXISTS #{table_name} (
          #{fields.to_column_sql}
        );

        CREATE UNIQUE INDEX IF NOT EXISTS #{table_name}_index ON #{table_name}(happened_at);

        CREATE OR REPLACE FUNCTION upsert_#{table_name}(#{fields.to_arg})
        RETURNS void AS $$
        BEGIN
          INSERT INTO #{table_name} (#{fields.names.join(', ')}) values (#{fields.to_param})
          ON CONFLICT (happened_at) DO UPDATE SET times = #{table_name}.times + 1,
          #{calculations.to_sql};
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION aggregate_#{table_name}(start_p timestamp(0), stop_p timestamp(0), timespan_p interval)
        RETURNS TABLE(#{fields.to_sql}) AS $$
          SELECT times, #{fields.without(:happened_at).names.join(', ')},
            t as happened_at
            FROM generate_series(start_p, stop_p, timespan_p) AS t
            LEFT OUTER JOIN (
              SELECT #{calculations.to_sql_fun}, SUM(times)::INTEGER as times,
                round_timestamp(timespan_p, happened_at) AS happened_at
                FROM #{table_name}
                GROUP BY round_timestamp(timespan_p, happened_at)
            ) as aggregation ON happened_at = t;
        $$ LANGUAGE sql;
      }
    end

    def table_name
      "#{name}_metrics"
    end
  end
end
