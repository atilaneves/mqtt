Feature: Publish
  As an MQTT client
  I want to receive messages that I have subscribed for
  So that I can act on those messages

  Scenario: Publish with no subscriptions
    Given I have connected to the broker on port 1883
    When I publish on topic "/foo/bar" with payload "ohnoes"
    Then I should not receive any messages

  Scenario: Publish with one subscription
    Given I have connected to the broker on port 1883
    When I successfully subscribe to topic "/foo/bar"
    And I publish on topic "/foo/bar" with payload "ohnoes"
    Then I should receive a message with topic "/foo/bar" and payload "ohnoes"

  Scenario: Publish with two clients
    Given I have connected to the broker on port 1883
    And another client has connected to the broker on port 1883
    When I successfully subscribe to topic "/foo/bar"
    And the other client successfully subscribes to topic "/bar/foo"
    When I publish on topic "/bar/foo" with payload "hello"
    Then the other client should receive a message with topic "/bar/foo" and payload "hello"
    And I should not receive any messages

  Scenario: Publish with three clients
    Given I have connected to the broker on port 1883
    And another client has connected to the broker on port 1883
    And another client has connected to the broker on port 1883
    When I successfully subscribe to topic "/foo/bar"
    And the other client successfully subscribes to topic "/bar/foo"
    And the third client successfully subscribes to topic "/bar/foo"
    When I publish on topic "/bar/foo" with payload "hello"
    Then the other client should receive a message with topic "/bar/foo" and payload "hello"
    And the third client should receive a message with topic "/bar/foo" and payload "hello"
    And I should not receive any messages

  Scenario: Publish with wildcards
    Given I have connected to the broker on port 1883
    And another client has connected to the broker on port 1883
    When I successfully subscribe to topic "/foo/+"
    And the other client successfully subscribes to topic "/stocks/broker/#"
    When I publish on topic "/stocks/broker/ibm/today" with payload "3.14"
    Then the other client should receive a message with topic "/stocks/broker/ibm/today" and payload "3.14"
    When the other client publishes on topic "/foo/bar" with payload "hello"
    Then I should receive a message with topic "/foo/bar" with payload "hello"
