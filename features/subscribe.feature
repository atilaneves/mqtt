Feature: Subscribe
  As an MQTT client,
  I want to be able to subscribe to a topic
  So that I can receive messages on that topic

  Scenario: Subscribe to one topic
    Given I have connected to the broker on port 1883
    When I subscribe to one topic with msgId 42
    Then I should receive a SUBACK message with qos 0 and msgId 42
