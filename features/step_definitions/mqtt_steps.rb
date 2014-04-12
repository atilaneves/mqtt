require 'socket'
require 'timeout'

After do
  @socket.close
  Process.kill("INT", @mqtt.pid)
  Process.wait(@mqtt.pid)
end

Given(/^I have established a TCP connection to the broker on port (\d+)$/) do |port|
  @mqtt = IO.popen("./mqtt")
  Timeout.timeout(1) do
    while @socket.nil?
      begin
        @socket = TCPSocket.new('localhost', port)
      rescue Errno::ECONNREFUSED
        #keep trying until the server is up
      end
    end
  end
end

When(/^I send a CONNECT MQTT message$/) do
  bytes = [ 0x10, 0x2a, #fixed header
            0x00, 0x06] + 'MQIsdp'.unpack('C*') + \
          [ 0x03, #protocol version
            0xcc, #connection flags 1100111x username, pw, !wr, w(01), w, !c, x
            0x00, 0x0a, #keepalive of 10
            0x00, 0x03, 'c'.ord, 'i'.ord, 'd'.ord, #client ID
            0x00, 0x04, 'w'.ord, 'i'.ord, 'l'.ord, 'l'.ord, #will topic
            0x00, 0x04, 'w'.ord, 'm'.ord, 's'.ord, 'g'.ord, #will msg
            0x00, 0x07, 'g'.ord, 'l'.ord, 'i'.ord, 'f'.ord, 't'.ord, 'e'.ord, 'l'.ord, #username
            0x00, 0x02, 'p'.ord, 'w'.ord, #password
        ]
  @socket.sendmsg(bytes.pack("C*"))
end

Then(/^I should receive a CONNACK MQTT message$/) do
  ret = @socket.recv(3).unpack("C*")
  ret.should == [0x20, 0x2, 0x0]
end
