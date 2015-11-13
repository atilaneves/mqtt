Feature: Ping
  As an MQTT client,
  I want to be able to ping the broker
  So that I may know the broker is still online

  Scenario: Ping
    Given I have connected to the broker on port 1883
    When I send a PINGREQ MQTT message
    Then I should receive a PINGRESP MQTT message
