Feature: Connect
  In order to use an MQTT broker, a client must be able to connect

  Scenario: Connect to broker
    Given I have established a TCP connection to the broker on port 1883
    When I send a CONNECT MQTT message
    Then I should receive a CONNACK MQTT message
