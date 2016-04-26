$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'time-series-pg'

require 'pg'

RSpec.configure do |config|
  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  def log(msg)
    puts msg if ENV['LOG']
  end

  def execute(sql, &block)
    log "SQL: #{sql}"
    conn.exec(sql, &block)
  end

  def conn
    @conn ||= PG.connect(host: '192.168.99.100', user: 'postgres', dbname: 'postgres')
  end

  def run(cmd)
    log "RUN: #{cmd}"
    system(cmd)
  end

  def init_metric(metric)
    run('docker run -p 5432:5432 --name postgres-test -d postgres:9.5')
    sleep 1 until run('nc -z 192.168.99.100 5432')

    execute(metric.to_sql)
  end

  def cleanup_metric(metric)
    execute("TRUNCATE TABLE #{metric.table_name}")
  end

  def destroy_metric(metric)
    run('docker stop postgres-test')
    run('docker rm postgres-test')
  end
end
