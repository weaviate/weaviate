//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

// TODO gh-1232 remove
// // This test covers the Telemetry process. Weaviate should log each time its endpoints are accessed,
// // and it should send these logs to an endpoint. In the testing config environment this endpoint is
// // the mock api's address. This test sends a request to Weaviate to ensure at least one log *should*
// // exist, waits a few seconds to ensure the log *should* be sent to the mock endpoint and then retrieves
// // the most recently received log from the mock endpoint. The test then decodes the response and
// // validates its structure.
// //
// // controlled by setup_test.go
// func createActionLogging(t *testing.T) {
// 	// send a request
// 	sendCreateActionRequest(t)

// 	// wait for the log to be posted
// 	time.Sleep(7 * time.Second)

// 	result := retrieveLogFromMockEndpoint(t)

// 	if result != nil {
// 		interpretedResult := interpretResult(t, result)
// 		if interpretedResult != nil {
// 			_, namePresent := interpretedResult["n"]
// 			_, typePresent := interpretedResult["t"]
// 			_, identifierPresent := interpretedResult["i"]
// 			_, amountPresent := interpretedResult["a"]
// 			_, whenPresent := interpretedResult["w"]

// 			assert.Equal(t, true, namePresent)
// 			assert.Equal(t, true, typePresent)
// 			assert.Equal(t, true, identifierPresent)
// 			assert.Equal(t, true, amountPresent)
// 			assert.Equal(t, true, whenPresent)

// 		}
// 	}
// }

// // The sendCreateActionRequest acceptance test is copied here to ensure at least one request should be
// // logged when we check the mock api's most recently received request. These assertions should pass
// // regardless of the rest of the Telemetry test passing.
// func sendCreateActionRequest(t *testing.T) {
// 	// Set all action values to compare
// 	actionTestString := "Test string"
// 	actionTestInt := 1
// 	actionTestBoolean := true
// 	actionTestNumber := 1.337
// 	actionTestDate := "2017-10-06T08:15:30+01:00"

// 	params := actions.NewActionsCreateParams().WithBody(
// 		&models.Action{
// 			Class: "MonitoringTestAction",
// 			Schema: map[string]interface{}{
// 				"testString":      actionTestString,
// 				"testWholeNumber": actionTestInt,
// 				"testTrueFalse":   actionTestBoolean,
// 				"testNumber":      actionTestNumber,
// 				"testDateTime":    actionTestDate,
// 			},
// 		})

// 	resp, err := helper.Client(t).Actions.ActionsCreate(params, nil)

// 	// Ensure that the response is OK.
// 	helper.AssertRequestOk(t, resp, err, func() {
// 		action := resp.Payload
// 		assert.Regexp(t, strfmt.UUIDPattern, action.ID)

// 		schema, ok := action.Schema.(map[string]interface{})
// 		if !ok {
// 			t.Fatal("The returned schema is not an JSON object")
// 		}

// 		testWholeNumber, _ := schema["testWholeNumber"].(json.Number).Int64()
// 		testNumber, _ := schema["testNumber"].(json.Number).Float64()

// 		// Check whether the returned information is the same as the data added.
// 		assert.Equal(t, actionTestString, schema["testString"])
// 		assert.Equal(t, actionTestInt, int(testWholeNumber))
// 		assert.Equal(t, actionTestBoolean, schema["testTrueFalse"])
// 		assert.Equal(t, actionTestNumber, testNumber)
// 		assert.Equal(t, actionTestDate, schema["testDateTime"])
// 	})
// }

// // retrieveLogFromMockEndpoint retrieves the most recently received log from the mock api.
// func retrieveLogFromMockEndpoint(t *testing.T) []byte {
// 	req, err := http.NewRequest("GET", "http://localhost:8087/mock/last", nil)
// 	req.Close = true
// 	assert.Equal(t, nil, err)

// 	client := &http.Client{}
// 	resp, err := client.Do(req)
// 	if err == nil {
// 		body, _ := io.ReadAll(resp.Body)
// 		defer resp.Body.Close()
// 		return body
// 	}
// 	if err != nil {
// 		urlError, ok := err.(*url.Error)
// 		if ok {
// 			assert.Equal(t, nil, urlError.Op)
// 		}
// 	}

// 	return nil
// }

// // interpretResult converts the received cbor-encoded log to a []map[string]interface.
// func interpretResult(t *testing.T, resultBody []byte) map[string]interface{} {
// 	decoded := make([]map[string]interface{}, 1)
// 	cborHandle := new(codec.CborHandle)
// 	encoder := codec.NewDecoderBytes(resultBody, cborHandle)
// 	err := encoder.Decode(&decoded)

// 	require.Equal(t, nil, err)
// 	return decoded[0]
// }
