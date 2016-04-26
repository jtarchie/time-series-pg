require 'spec_helper'

RSpec.describe 'With a metric that counts' do
  let(:timestamp) { Time.new(2000, 1, 1) }
  def metric
    TimeSeriesPg.metric :visitors do
      fields do
        total :integer, default: 0, calc: :sum
      end
    end
  end

  def create_metric(total:, timestamp:)
    execute("SELECT upsert_visitors_metrics(
      '#{total}',
      '#{timestamp}'
    );")
  end

  after { cleanup_metric(metric) }
  before(:all) { init_metric(metric) }
  after(:all)  { destroy_metric(metric) }

  describe 'with metrics that come in the same second' do
    it 'sums the total together' do
      create_metric total: 1, timestamp: timestamp
      create_metric total: 2, timestamp: timestamp

      execute("SELECT * FROM visitors_metrics LIMIT 1") do |results|
        row = results.first
        expect(row['total']).to eq '3'
        expect(row['happened_at']).to eq '2000-01-01 00:00:00'
      end
    end

    it 'keeps a total of the metrics' do
      create_metric total: 1, timestamp: timestamp
      execute("SELECT * FROM visitors_metrics LIMIT 1") do |results|
        row = results.first
        expect(row['times']).to eq '1'
      end

      create_metric total: 2, timestamp: timestamp
      execute("SELECT * FROM visitors_metrics LIMIT 1") do |results|
        row = results.first
        expect(row['times']).to eq '2'
      end
    end
  end

  context 'that come in a different second' do
    before do
      create_metric total: 1, timestamp: timestamp
      create_metric total: 1, timestamp: timestamp + 5
    end

    it 'creates two different metric' do
      execute('SELECT count(*) FROM visitors_metrics') do |results|
        row = results.first
        expect(row['count']).to eq '2'
      end
    end

    it 'assigns the stats to each metric' do
      execute('SELECT * FROM visitors_metrics') do |results|
        row = results[0]
        expect(row['total']).to eq '1'
        expect(row['happened_at']).to eq '2000-01-01 00:00:00'

        row = results[1]
        expect(row['total']).to eq '1'
        expect(row['happened_at']).to eq '2000-01-01 00:00:05'
      end
    end

    context 'when querying the data' do
      context 'in 1 second intervals' do
        it 'pads with nulls' do
          execute("SELECT * FROM aggregate_visitors_metrics('2000-01-01 00:00:00', '2000-01-01 00:00:10', '1 SECOND')") do |results|
            expect(results.to_a.length).to eq 11
            expect(results.to_a).to eq [
              {'times' => '1', 'total' => '1', 'happened_at' => '2000-01-01 00:00:00'},
              {'times' => nil, 'total' => nil, 'happened_at' => '2000-01-01 00:00:01'},
              {'times' => nil, 'total' => nil, 'happened_at' => '2000-01-01 00:00:02'},
              {'times' => nil, 'total' => nil, 'happened_at' => '2000-01-01 00:00:03'},
              {'times' => nil, 'total' => nil, 'happened_at' => '2000-01-01 00:00:04'},
              {'times' => '1', 'total' => '1', 'happened_at' => '2000-01-01 00:00:05'},
              {'times' => nil, 'total' => nil, 'happened_at' => '2000-01-01 00:00:06'},
              {'times' => nil, 'total' => nil, 'happened_at' => '2000-01-01 00:00:07'},
              {'times' => nil, 'total' => nil, 'happened_at' => '2000-01-01 00:00:08'},
              {'times' => nil, 'total' => nil, 'happened_at' => '2000-01-01 00:00:09'},
              {'times' => nil, 'total' => nil, 'happened_at' => '2000-01-01 00:00:10'},
            ]
          end
        end
      end

      context 'in 3 second intervals' do
        it 'averages out the data' do
          execute("SELECT * FROM aggregate_visitors_metrics('2000-01-01 00:00:00', '2000-01-01 00:00:10', '3 SECOND')") do |results|
            expect(results.to_a.length).to eq 4
            expect(results.to_a).to eq [
              {'times' => '1', 'total' => '1', 'happened_at' => '2000-01-01 00:00:00'},
              {'times' => '1', 'total' => '1', 'happened_at' => '2000-01-01 00:00:03'},
              {'times' => nil, 'total' => nil, 'happened_at' => '2000-01-01 00:00:06'},
              {'times' => nil, 'total' => nil, 'happened_at' => '2000-01-01 00:00:09'},
            ]
          end
        end
      end
    end
  end
end
