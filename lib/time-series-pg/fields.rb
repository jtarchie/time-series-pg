module TimeSeriesPg
  class Fields
    include Enumerable

    def initialize(columns = [])
      @columns = columns
    end

    class Column < Struct.new(:name, :type, :args)
      def to_column_sql
        sql = "#{name} #{type}"
        sql += " DEFAULT #{args[:default]}" if args[:default]
        sql
      end

      def to_sql
        "#{name} #{type}"
      end

      def to_arg
        "#{to_param} #{type}"
      end

      def to_param
        "#{name}_p"
      end

      def args
        super || { private: false }
      end
    end

    def self.create(&block)
      fields = new
      fields.define_field :times, :integer, default: 1, private: true
      fields.instance_eval &block
      fields.happened_at 'timestamp(0)'
      fields
    end

    def method_missing(name, *args)
      @columns ||= []
      @columns << Column.new(name, *args)
    end
    alias define_field method_missing

    def names
      filter.collect(&:name)
    end

    def to_param
      filter.collect(&:to_param).join(', ')
    end

    def to_arg
      filter.collect(&:to_arg).join(', ')
    end

    def to_sql
      collect(&:to_sql).join(", \n")
    end

    def to_column_sql
      collect(&:to_column_sql).join(", \n")
    end

    def without(*names)
      self.class.new reject { |f| names.include?(f.name) }
    end

    private

    def each(&block)
      @columns.each &block
    end

    def filter(include_private: false)
      select do |f|
        include_private || !f.args[:private]
      end
    end
  end
end

