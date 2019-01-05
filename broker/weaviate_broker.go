/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN: Bob van Luijt (bob@k10y.co)
 */

// Package restapi with all rest API functions.
package weaviateBroker

import (
	"github.com/creativesoftwarefdn/weaviate/messages"
	MQTT "github.com/eclipse/paho.mqtt.golang"
)

var MqttEnabled bool
var MqttClient MQTT.Client
var messaging *messages.Messaging

// ConnectToMqtt connects to Weaviate Mqtt Broker
func ConnectToMqtt(Host string, Port int32) {

	// Validate if host is set, if not, don't connect
	if Host != "" {
		messaging.InfoMessage("Connecting to MQTT broker...")

		opts := MQTT.NewClientOptions()
		opts.AddBroker("tcp://127.0.0.1:1883")
		opts.SetUsername("b9b908ee-0039-4f6a-a2c0-b51ebbd982f1")
		opts.SetPassword("05d64e63-e464-40ac-8e55-4ccedb0a60f7")
		opts.SetClientID("WEAVIATE-SERVER")

		// Connect client
		MqttClient = MQTT.NewClient(opts)

		if token := MqttClient.Connect(); token.Wait() && token.Error() != nil {
			messaging.ErrorMessage("Could not connect to MQTT broker. Server needs to be restarted to reconnect to broker.")
			messaging.ErrorMessage(token.Error())
		} else {
			messaging.InfoMessage("Connected to MQTT broker.")
		}

		// Mqtt enabled
		MqttEnabled = true
	} else {
		// Mqtt disabled
		MqttEnabled = false
	}

}

// Publish a message to the right channel
func Publish(channel string, message string) {
	if MqttEnabled == true {
		if token := MqttClient.Publish(channel, 0, false, message); token.Wait() && token.Error() != nil {
			messaging.ErrorMessage(token.Error())
		} else {
			messaging.DebugMessage("Published message to MQTT broker.")
		}
	}
}
