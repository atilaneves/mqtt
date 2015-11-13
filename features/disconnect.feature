Feature: Disconnect
  As an MQTT client,
  I want to be able to disconnect cleanly

  Scenario: Disconnect message
    Given I have connected to the broker on port 1883
    When I send a DISCONNECT MQTT message
    Then the server should close the connection

  Scenario: Disconnect with one subscription
    Given I have connected to the broker on port 1883
    When I successfully subscribe to topic "/foo/bar"
    And I publish on topic "/foo/bar" with payload "ohnoes"
    Then I should receive a message with topic "/foo/bar" and payload "ohnoes"
    When I send a DISCONNECT MQTT message
    Then the server should close the connection

  Scenario: Disconnect then connect again
    Given I have connected to the broker on port 1883
    When I successfully subscribe to topic "/foo/bar"
    And I publish on topic "/foo/bar" with payload "ohnoes"
    Then I should receive a message with topic "/foo/bar" and payload "ohnoes"
    When I send a DISCONNECT MQTT message
    Then the server should close the connection

    Given I have connected to the broker on port 1883
    When I publish on topic "/foo/bar" with payload "ohnoes"
    Then I should not receive any messages

    When I successfully subscribe to topic "/foo/bar"
    And I publish on topic "/foo/bar" with payload "ohnoes"
    Then I should receive a message with topic "/foo/bar" and payload "ohnoes"

    When I send a DISCONNECT MQTT message
    Then the server should close the connection
