//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

// Test if the loop is working by asserting whether the log is reset after <interval> seconds
func TestLoop(t *testing.T) {
	t.Parallel()

	// setup
	calledFunctions := NewLog()
	calledFunctions.Disabled = false
	calledFunctions.PeerName = "soggy-whale-bread"
	calledFunctions.Register("REST", "weaviate.something.or.other")

	logger, _ := test.NewNullLogger()

	interval := 1
	url := "http://www.example.com"

	clientConf := clientv3.Config{}
	client, _ := clientv3.New(clientConf)
	ctx := context.Background()
	reporter := NewReporter(ctx, calledFunctions, interval, url, false, true, client, logger)
	reporter.UnitTest = true

	go reporter.Start()

	time.Sleep(time.Duration(3) * time.Second)

	logsAfterReporting := calledFunctions.ExtractLoggedRequests()

	// test
	assert.Equal(t, 0, len(*logsAfterReporting))
}

// Register a single request and call the minimization function, then assert whether all fields have been minimized correctly
func TestMinimize(t *testing.T) {
	t.Parallel()

	// setup
	calledFunctions := NewLog()
	calledFunctions.Disabled = false
	calledFunctions.PeerName = "tiny-grey-chainsword"
	calledFunctions.Register("REST", "weaviate.something.or.other")

	calledFunctions.Log["[REST]weaviate.something.or.other"].Name = "tiny-grey-chainsword"
	calledFunctions.Log["[REST]weaviate.something.or.other"].When = int64(1550745544)

	transformer := NewOutputTransformer(true)

	minimizedLogs := transformer.Minimize(&calledFunctions.Log)

	assert.Equal(t, 1, len(*minimizedLogs))

	for _, log := range *minimizedLogs {

		name := log["n"].(string)
		logType := log["t"].(string)
		identifier := log["i"].(string)
		amount := log["a"].(int)
		when := log["w"].(int64)

		assert.Equal(t, "tiny-grey-chainsword", name)
		assert.Equal(t, "REST", logType)
		assert.Equal(t, "weaviate.something.or.other", identifier)
		assert.Equal(t, 1, int(amount))
		assert.Equal(t, int64(1550745544), int64(when))
	}
}

// Create a log containing two logged function types and call the AddTimeStamps function,
// then assert whether both logs now contain the same plausible timestamp.
func TestAddTimestamps(t *testing.T) {
	t.Parallel()

	// setup
	calledFunctions := NewLog()
	calledFunctions.Disabled = false
	calledFunctions.PeerName = "iridiscent-damp-bagel"
	calledFunctions.Register("REST", "weaviate.something.or.other1")
	calledFunctions.Register("REST", "weaviate.something.or.other2")

	past := time.Now().Unix()

	time.Sleep(time.Duration(1) * time.Second)

	clientConf := clientv3.Config{}
	client, _ := clientv3.New(clientConf)
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	reporter := NewReporter(ctx, nil, 0, "", true, true, client, logger)
	reporter.UnitTest = true

	reporter.AddTimeStamps(&calledFunctions.Log)

	time.Sleep(time.Duration(1) * time.Second)

	future := time.Now().Unix()

	log1Timestamp := calledFunctions.Log["[REST]weaviate.something.or.other1"].When
	log2Timestamp := calledFunctions.Log["[REST]weaviate.something.or.other2"].When

	notZero := log1Timestamp != 0
	notFromThePast := log1Timestamp > past
	notFromTheFuture := log1Timestamp < future

	// test
	assert.Equal(t, log1Timestamp, log2Timestamp)
	assert.Equal(t, true, notZero)
	assert.Equal(t, true, notFromThePast)
	assert.Equal(t, true, notFromTheFuture)
}

// Create a minimized request log and encode it to CBOR then assert whether the result matches the expected value.
func TestCborEncode(t *testing.T) {
	t.Parallel()

	// setup
	minimizedLogs := make([]map[string]interface{}, 0)
	log := make(map[string]interface{})
	log["n"] = "upbeat-aquatic-pen"
	log["t"] = "REST"
	log["i"] = "weaviate.something.or.other"
	log["a"] = 1
	log["w"] = 1550745544
	minimizedLogs = append(minimizedLogs, log)

	outputTransformer := NewOutputTransformer(true)

	encoded, err := outputTransformer.EncodeAsCBOR(&minimizedLogs)

	expected := []byte{0x81, 0xa5, 0x61, 0x61, 0x1, 0x61, 0x69, 0x78, 0x1b, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e, 0x73, 0x6f, 0x6d, 0x65, 0x74, 0x68, 0x69, 0x6e, 0x67, 0x2e, 0x6f, 0x72, 0x2e, 0x6f, 0x74, 0x68, 0x65, 0x72, 0x61, 0x6e, 0x72, 0x75, 0x70, 0x62, 0x65, 0x61, 0x74, 0x2d, 0x61, 0x71, 0x75, 0x61, 0x74, 0x69, 0x63, 0x2d, 0x70, 0x65, 0x6e, 0x61, 0x74, 0x64, 0x52, 0x45, 0x53, 0x54, 0x61, 0x77, 0x1a, 0x5c, 0x6e, 0x7f, 0xc8}

	assert.Equal(t, nil, err)

	assert.Equal(t, expected, *encoded)
}
