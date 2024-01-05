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

// Package implements performance tracking examples

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/models"
)

type batch struct {
	Objects []*models.Object
}

type benchmarkResult map[string]map[string]int64

func main() {
	var benchmarkName string
	var numBatches, failPercentage, maxEntries int

	flag.StringVar(&benchmarkName, "name", "SIFT", "Which benchmark should be run. Currently only SIFT is available.")
	flag.IntVar(&maxEntries, "numberEntries", 100000, "Maximum number of entries read from the dataset")
	flag.IntVar(&numBatches, "numBatches", 1, "With how many parallel batches objects should be added")
	flag.IntVar(&failPercentage, "fail", -1, "Fail if regression is larger")
	flag.Parse()

	t := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 120 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   100,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	c := &http.Client{Transport: t}
	url := "http://localhost:8080/v1/"

	alreadyRunning := startWeaviate(c, url)

	var newRuntime map[string]int64
	var err error
	switch benchmarkName {
	case "SIFT":
		newRuntime, err = benchmarkSift(c, url, maxEntries, numBatches)
	default:
		panic("Unknown benchmark " + benchmarkName)
	}

	if err != nil {
		clearExistingObjects(c, url)
	}

	if !alreadyRunning {
		tearDownWeaviate()
	}

	if err != nil {
		panic(errors.Wrap(err, "Error occurred during benchmarking"))
	}

	FullBenchmarkName := benchmarkName + "-" + fmt.Sprint(maxEntries) + "_Entries-" + fmt.Sprint(numBatches) + "_Batch(es)"

	// Write results to file, keeping existing entries
	oldBenchmarkRunTimes := readCurrentBenchmarkResults()
	oldRuntime := oldBenchmarkRunTimes[FullBenchmarkName]
	oldBenchmarkRunTimes[FullBenchmarkName] = newRuntime
	benchmarkJSON, _ := json.MarshalIndent(oldBenchmarkRunTimes, "", "\t")
	if err := os.WriteFile("benchmark_results.json", benchmarkJSON, 0o666); err != nil {
		panic(err)
	}

	totalNewRuntime := int64(0)
	for _, runtime := range newRuntime {
		totalNewRuntime += runtime
	}
	totalOldRuntime := int64(0)
	for _, runtime := range oldRuntime {
		totalOldRuntime += runtime
	}

	fmt.Fprint(
		os.Stdout,
		"Runtime for benchmark "+FullBenchmarkName+
			": old total runtime: "+fmt.Sprint(totalOldRuntime)+"ms, new total runtime:"+fmt.Sprint(totalNewRuntime)+"ms.\n"+
			"This is a change of "+fmt.Sprintf("%.2f", 100*float32(totalNewRuntime-totalOldRuntime)/float32(totalNewRuntime))+"%.\n"+
			"Please update the benchmark results if necessary.\n\n",
	)
	fmt.Fprint(os.Stdout, "Runtime for individual steps:.\n")
	for name, time := range newRuntime {
		fmt.Fprint(os.Stdout, "Runtime for "+name+" is "+fmt.Sprint(time)+"ms.\n")
	}

	// Return with error code if runtime regressed and corresponding flag was set
	if failPercentage >= 0 &&
		totalOldRuntime > 0 && // don't report regression if no old entry exists
		float64(totalOldRuntime)*(1.0+0.01*float64(failPercentage)) < float64(totalNewRuntime) {
		fmt.Fprint(
			os.Stderr, "Failed due to performance regressions.\n",
		)
		os.Exit(1)
	}
}

// If there is already a schema present, clear it out
func clearExistingObjects(c *http.Client, url string) {
	checkSchemaRequest := createRequest(url+"schema", "GET", nil)
	checkSchemaResponseCode, body, _, err := performRequest(c, checkSchemaRequest)
	if err != nil {
		panic(errors.Wrap(err, "perform request"))
	}
	if checkSchemaResponseCode != 200 {
		return
	}

	var dump models.Schema
	if err := json.Unmarshal(body, &dump); err != nil {
		panic(errors.Wrap(err, "Could not unmarshal read response"))
	}
	for _, classObj := range dump.Classes {
		requestDelete := createRequest(url+"schema/"+classObj.Class, "DELETE", nil)
		responseDeleteCode, _, _, err := performRequest(c, requestDelete)
		if err != nil {
			panic(errors.Wrap(err, "Could delete schema"))
		}
		if responseDeleteCode != 200 {
			panic(fmt.Sprintf("Could not delete schema, code: %v", responseDeleteCode))
		}
	}
}

func command(app string, arguments []string, waitForCompletion bool) error {
	mydir, err := os.Getwd()
	if err != nil {
		return err
	}

	cmd := exec.Command(app, arguments...)
	execDir := mydir + "/../../"
	cmd.Dir = execDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if waitForCompletion {
		err = cmd.Run()
	} else {
		err = cmd.Start()
	}

	return err
}

func readCurrentBenchmarkResults() benchmarkResult {
	benchmarkFile, err := os.Open("benchmark_results.json")
	if err != nil {
		fmt.Print("No benchmark file present.")
		return make(benchmarkResult)
	}
	defer benchmarkFile.Close()

	var result benchmarkResult
	jsonParser := json.NewDecoder(benchmarkFile)
	if err = jsonParser.Decode(&result); err != nil {
		panic("Could not parse existing benchmark file.")
	}
	return result
}

func tearDownWeaviate() error {
	fmt.Print("Shutting down weaviate.\n")
	app := "docker-compose"
	arguments := []string{
		"down",
		"--remove-orphans",
	}
	return command(app, arguments, true)
}

// start weaviate in case it was not already started
//
// We want to benchmark the current state and therefore need to rebuild and then start a docker container
func startWeaviate(c *http.Client, url string) bool {
	requestReady := createRequest(url+".well-known/ready", "GET", nil)

	responseStartedCode, _, _, err := performRequest(c, requestReady)
	alreadyRunning := err == nil && responseStartedCode == 200

	if alreadyRunning {
		fmt.Print("Weaviate instance already running.\n")
		return alreadyRunning
	}

	fmt.Print("(Re-) build and start weaviate.\n")
	cmd := "./tools/test/run_ci_server.sh"
	if err := command(cmd, []string{}, true); err != nil {
		panic(errors.Wrap(err, "Command to (re-) build and start weaviate failed"))
	}
	return false
}

// createRequest creates requests
func createRequest(url string, method string, payload interface{}) *http.Request {
	var body io.Reader = nil
	if payload != nil {
		jsonBody, err := json.Marshal(payload)
		if err != nil {
			panic(errors.Wrap(err, "Could not marshal request"))
		}
		body = bytes.NewBuffer(jsonBody)
	}
	request, err := http.NewRequest(method, url, body)
	if err != nil {
		panic(errors.Wrap(err, "Could not create request"))
	}
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Accept", "application/json")

	return request
}

// performRequest runs requests
func performRequest(c *http.Client, request *http.Request) (int, []byte, int64, error) {
	timeStart := time.Now()
	response, err := c.Do(request)
	requestTime := time.Since(timeStart).Milliseconds()

	if err != nil {
		return 0, nil, requestTime, err
	}

	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		return 0, nil, requestTime, err
	}

	return response.StatusCode, body, requestTime, nil
}
