Feature: Subscribe
  A client must be able to subscribe to a topic.

  Scenario: Subscribe to a topic
    Given I have connected to the broker on port 1883
    When I subscribe to a topic with msgId 42
    Then I should receive a SUBACK message with qos 0 and msgId 42
