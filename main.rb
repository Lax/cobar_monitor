#!/usr/bin/env ruby

require 'yaml'
require 'net/http'
require 'uri'
require 'json'

def fetch_data(prev_data, is_init=true)
  prev_data_with_mask = []
  prev_data.each do |data|
    t = {
      'id' => data['id'],
      'netIn' => data['netIn'],
      'netOut' => data['netOut'],
      'reCount' => data['request'],
      'timestamp' => data['timestamp'],
      'flag' => data['flag']
    }
    prev_data_with_mask << t
    t = nil
  end

  last_data_json = JSON.dump(prev_data_with_mask)

  post_data = {
    'clusterId' => 2,
    'last' => last_data_json,
    'valueType' => 'cobarClusterLevelThroughput',
    'nowTime' => Time.now.gmtime
  }

  res = Net::HTTP.post_form($api_uri, post_data)
  response = res.body
  current_data = JSON.load(response)

  if current_data
    return [current_data, false]
  else
    return [nil, true]
  end
end

if __FILE__ == $0
  config = YAML.load_file('config.yml')
  $api_uri = URI(config['cobar']['uri'])
  $interval = config['monitor']['interval']

  $stdout.sync = true

  $init_last_data = [{"id" => -1, "netIn" => -1, "netOut" => -1, "reCount" => -1, "timestamp" => -1, "flag" => 'cluster'}]

  prev_data = $init_last_data
  init = true

  loop do
    current_data, will_init = fetch_data(prev_data, init)
    
    # 正常数据输出
    if not init and not will_init
      current_data.each_with_index do |data, idx|
        #puts "#{data['netIn_deriv']} #{data['netOut_deriv']} #{data['connection']} #{data['request_deriv']} #{data['flag']} #{data['id']}"

        ['netIn', 'netOut', 'request'].each do |metric|
          value_per_second = (data[metric] - prev_data[idx][metric]) * 1000 / (data['timestamp'] - prev_data[idx]['timestamp'])
          puts "dj.db.cobar.%s_%d.%s %s %d" % [data['flag'], data['id'], metric, value_per_second, data['timestamp']/1000]
        end
        ['connection'].each do |metric|
          puts "dj.db.cobar.%s_%d.%s %s %d" % [data['flag'], data['id'], metric, data[metric], data['timestamp']/1000]
        end
      end
    end

    prev_data = current_data
    init = will_init
    puts
    sleep $interval
  end
end