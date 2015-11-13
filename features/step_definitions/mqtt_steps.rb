require 'socket'
require 'timeout'
require 'rspec'

class MqttServer
  def initialize
    @proc = IO.popen('./mqtt')
  end

  def finalize
    unless @proc.nil?
      Process.kill('QUIT', @proc.pid)
      Process.wait(@proc.pid)
      @proc.each_line do |l|
        puts l
      end
    end
  end
end

class MqttClient
  def initialize(port = 1883)
    Timeout.timeout(1) do
      while @socket.nil?
        begin
          @socket = TCPSocket.new('localhost', port)
        rescue Errno::ECONNREFUSED
          # keep trying until the server is up or we time out
        end
      end
    end
  end

  def finalize
    @socket.nil? || @socket.close
  end

  def recv(num_bytes)
    @socket.recvfrom(num_bytes)
  end

  def send_bytes(bytes)
    @socket.sendmsg(bytes.pack('C*'))
  end

  def assert_recv(bytes)
    @socket.recv(bytes.length).unpack('C*').should == bytes
  end

  def connect_to_broker_mqtt
    send_mqtt_connect
    expect_mqtt_connack
  end

  def send_mqtt_connect
    send_bytes [0x10, 0x2a, # fixed header
                0x00, 0x06] + 'MQIsdp'.unpack('C*') + \
    [0x03, # protocol version
     0xcc, # connection flags 1100111x user, pw, !wr, w(01), w, !c, x
     0x00, 0x0a, # keepalive of 10
     0x00, 0x03, 'c'.ord, 'i'.ord, 'd'.ord, # client ID
     0x00, 0x04, 'w'.ord, 'i'.ord, 'l'.ord, 'l'.ord, # will topic
     0x00, 0x04, 'w'.ord, 'm'.ord, 's'.ord, 'g'.ord, # will msg
     0x00, 0x07, 'g'.ord, 'l'.ord, 'i'.ord, 'f'.ord, 't'.ord, 'e'.ord, 'l'.ord, # username
     0x00, 0x02, 'p'.ord, 'w'.ord] # password
  end

  def send_mqtt_ping_req
    send_bytes [0xc0, 0]
  end

  def expect_mqtt_connack
    assert_recv [0x20, 0x2, 0x0, 0x0]
  end

  def expect_mqtt_ping_resp
    assert_recv [0xd0, 0]
  end

  def subscribe(topic, msg_id, qos = 0)
    send_bytes [0x8c, 5 + topic.length, # fixed header
                0, msg_id, # message ID
                0, topic.length] + string_to_ints(topic)  + [qos]
  end

  def recv_suback(msg_id, qos = 0)
    assert_recv [0x90, 3, 0, msg_id.to_i, qos.to_i]
  end

  def successfully_subscribe(topic, msg_id, qos = 0)
    subscribe(topic, msg_id, qos)
    recv_suback(msg_id, qos)
  end

  def expect_mqtt_publish(topic, payload)
    Timeout.timeout(1) do
      remaining_length = topic.length + 2 + payload.length
      assert_recv [0x30, remaining_length, 0, topic.length] + \
      string_to_ints(topic) + string_to_ints(payload)
    end
  end
end

Before do
  @mqtt = MqttServer.new
end

After do
  @clients.each { |c| c.finalize }
  @mqtt.finalize
end

def connect_to_broker_tcp(port = 1883)
  @clients ||= []
  @clients << MqttClient.new(port)
end

Given(/^I have established a TCP connection to the broker on port (\d+)$/) do |port|
  connect_to_broker_tcp(port)
end

When(/^I send a CONNECT MQTT message$/) do
  @clients[0].send_mqtt_connect
end

Then(/^I should receive a CONNACK MQTT message$/) do
  @clients[0].expect_mqtt_connack
end

def connect_to_broker_mqtt(port)
  connect_to_broker_tcp(port)
  @clients[-1].connect_to_broker_mqtt
end

Given(/^I have connected to the broker on port (\d+)$/) do |port|
  connect_to_broker_mqtt(port)
end

When(/^I subscribe to one topic with msgId (\d+)$/) do |msg_id|
  @clients[0].subscribe('topic', msg_id.to_i)
end

Then(/^I should receive a SUBACK message with qos (\d+) and msgId (\d+)$/) do |qos, msg_id|
  @clients[0].recv_suback(msg_id.to_i, qos.to_i)
end

def publish(client, topic, payload)
  remaining_length = topic.length + 2 + payload.length
  client.send_bytes [0x30, remaining_length, 0, topic.length] + \
    string_to_ints(topic) + string_to_ints(payload)
end

When(/^I publish on topic "(.*?)" with payload "(.*?)"$/) do |topic, payload|
  publish(@clients[0], topic, payload)
end

def string_to_ints(str)
  str.chars.map { |x| x.ord }
end

Then(/^I should not receive any messages$/) do
  # FIXME: do something
end

msg_id = 1
When(/^I subscribe to topic "(.*?)"$/) do |topic|
  remaining_length = topic.length + 4
  qos = 1
  @clients[0].send_bytes [0x8c, remaining_length,
                          0, msg_id,
                          0, topic.length] + string_to_ints(topic) + [qos]
  msg_id += 1
end

Then(/^I should receive a message with topic "(.*?)" and payload "(.*?)"$/) do |topic, payload|
  @clients[0].expect_mqtt_publish(topic, payload)
end

When(/^I successfully subscribe to topic "(.*?)"$/) do |topic|
  @clients[0].successfully_subscribe(topic, msg_id)
end

Given(/^another client has connected to the broker on port (\d+)$/) do |port|
  connect_to_broker_mqtt(port)
end

When(/^the other client successfully subscribes to topic "(.*?)"$/) do |topic|
  @clients[1].successfully_subscribe(topic, msg_id)
end

Then(/^the other client should receive a message with topic "(.*?)" and payload "(.*?)"$/) do |topic, payload|
  @clients[1].expect_mqtt_publish(topic, payload)
end

When(/^the third client successfully subscribes to topic "(.*?)"$/) do |topic|
  @clients[2].successfully_subscribe(topic, msg_id)
end

Then(/^the third client should receive a message with topic "(.*?)" and payload "(.*?)"$/) do |topic, payload|
  @clients[2].expect_mqtt_publish(topic, payload)
end

When(/^I send a DISCONNECT MQTT message$/) do
  @clients[0].send_bytes [0xe0, 0]
end

Then(/^the server should close the connection$/) do
  @clients[0].recv(10).should == ['', nil]
  @clients.pop
end

When(/^I send a PINGREQ MQTT message$/) do
  @clients[0].send_mqtt_ping_req
end

Then(/^I should receive a PINGRESP MQTT message$/) do
  @clients[0].expect_mqtt_ping_resp
end

When(/^the other client publishes on topic "(.*?)" with payload "(.*?)"$/) do |topic, payload|
  publish(@clients[1], topic, payload)
end

Then(/^I should receive a message with topic "(.*?)" with payload "(.*?)"$/) do |topic, payload|
  @clients[0].expect_mqtt_publish(topic, payload)
end
