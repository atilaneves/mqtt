Feature: Connect
  As an MQTT client,
  I want to be able to connect to the broker
  So that I may use other MQTT features

  Scenario: Connect to broker
    Given I have established a TCP connection to the broker on port 1883
    When I send a CONNECT MQTT message
    Then I should receive a CONNACK MQTT message
