module TimeSeriesPg
  class Calculations < Struct.new(:metric)
    include Enumerable

    def initialize(metric, fields)
      @columns = fields.map do |f|
        next unless f.args[:calc]
        Column.new metric, f
      end.compact
    end

    Column = Struct.new(:metric, :field) do
      def name
        field.name
      end

      def operation
        field.args[:calc]
      end

      def to_sql
        case operation
        when :avg
          "#{name} = (#{to_param} + #{metric.table_name}.#{name}) / 2"

        when :sum
          "#{name} = (#{to_param} + #{metric.table_name}.#{name}) "
        end
      end

      def to_param
        "#{name}_p"
      end

      def to_sql_fun
        "#{operation}(#{name})::#{field.type} AS #{name}"
      end
    end

    def to_sql
      collect(&:to_sql).join(", \n")
    end

    def to_sql_fun
      collect(&:to_sql_fun).join(',')
    end

    private

    def each(&block)
      @columns.each &block
    end
  end
end

