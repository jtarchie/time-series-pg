require 'spec_helper'

RSpec.describe 'With multiple fields' do
  let(:timestamp) { Time.new(2000, 1, 1) }

  def metric
    TimeSeriesPg.metric :visitors do
      fields do
        sum     :integer, default: 0,  calc: :sum
        average :float,   default: 0,  calc: :avg
      end
    end
  end

  def create_metric(sum:, average:, timestamp:)
    execute("SELECT upsert_visitors_metrics(
      '#{sum}',
      '#{average}',
      '#{timestamp}'
    );")
  end

  after { cleanup_metric(metric) }
  before(:all) { init_metric(metric) }
  after(:all)  { destroy_metric(metric) }

  describe 'with multiple metrics' do
    it 'aggregates the total together' do
      create_metric sum: 1, average: 2, timestamp: timestamp
      create_metric sum: 2, average: 4, timestamp: timestamp

      execute("SELECT * FROM visitors_metrics LIMIT 1") do |results|
        row = results.first
        expect(row['sum']).to eq '3'
        expect(row['average']).to eq '3'
        expect(row['happened_at']).to eq '2000-01-01 00:00:00'
      end
    end

    it 'keeps a total of the metrics' do
      create_metric sum: 1, average: 2, timestamp: timestamp
      execute("SELECT * FROM visitors_metrics LIMIT 1") do |results|
        row = results.first
        expect(row['times']).to eq '1'
      end

      create_metric sum: 1, average: 2, timestamp: timestamp
      execute("SELECT * FROM visitors_metrics LIMIT 1") do |results|
        row = results.first
        expect(row['times']).to eq '2'
      end
    end
  end
end
