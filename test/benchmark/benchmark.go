//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// Package implements performance tracking examples

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/semi-technologies/weaviate/entities/models"
)

type batch struct {
	Objects []*models.Object
}

type benchmarkResult map[string](map[string]int64)

func main() {
	var benchmarkName string
	var failPercentage int
	var maxEntries int

	flag.StringVar(&benchmarkName, "name", "SIFT", "Which benchmark should be run. Currently only SIFT is available.")
	flag.IntVar(&maxEntries, "numberEntries", 100000, "Maximum number of entries read from the dataset")
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

	alreadyRunning, err := startWeavaite(c, url)
	if err != nil {
		panic("Could not start weaviate")
	}

	var newRuntime map[string]int64
	switch benchmarkName {
	case "SIFT":
		newRuntime = benchmarkSift(c, url, maxEntries)
	default:
		panic("Unknown benchmark " + benchmarkName)
	}

	if !alreadyRunning {
		tearDownWeavaite()
	}

	FullBenchmarkName := benchmarkName + "-" + fmt.Sprint(maxEntries)

	// Write results to file, keeping existing entries
	oldBenchmarkRunTimes := readCurrentBenchmarkResults()
	oldRuntime := oldBenchmarkRunTimes[FullBenchmarkName]
	oldBenchmarkRunTimes[FullBenchmarkName] = newRuntime
	benchmarkJSON, _ := json.MarshalIndent(oldBenchmarkRunTimes, "", "\t")
	if err := os.WriteFile("benchmark_results.json", benchmarkJSON, 0o666); err != nil {
		panic(err)
	}

	total_new_runtime := int64(0)
	for _, time := range newRuntime {
		total_new_runtime += time
	}
	total_old_runtime := int64(0)
	for _, time := range oldRuntime {
		total_old_runtime += time
	}

	fmt.Fprint(
		os.Stdout,
		"Runtime for benchmark "+FullBenchmarkName+
			": old total runtime: "+fmt.Sprint(total_old_runtime)+"ms, new total runtime:"+fmt.Sprint(total_new_runtime)+"ms.\n"+
			"This is a change of "+fmt.Sprintf("%.2f", 100*float32(total_new_runtime-total_old_runtime)/float32(total_new_runtime))+"%.\n"+
			"Please update the benchmark results if necessary.\n\n",
	)
	fmt.Fprint(os.Stdout, "Runtime for individual steps:.\n")
	for name, time := range newRuntime {
		fmt.Fprint(os.Stdout, "Runtime for "+name+" is "+fmt.Sprint(time)+"ms.\n")
	}

	// Return with error code if runtime regressed and corresponding flag was set
	if failPercentage >= 0 &&
		total_old_runtime > 0 && // don't report regression if no old entry exists
		float64(total_old_runtime)*(1.0+0.01*float64(failPercentage)) < float64(total_new_runtime) {
		fmt.Fprint(
			os.Stderr, "Failed due to performance regressions.\n",
		)
		os.Exit(1)
	}
}

func command(app string, arguments []string, wait_for_completion bool) error {
	mydir, err := os.Getwd()
	if err != nil {
		return err
	}

	cmd := exec.Command(app, arguments...)
	execDir := mydir + "/../../"
	cmd.Dir = execDir
	if wait_for_completion {
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

func tearDownWeavaite() error {
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
func startWeavaite(c *http.Client, url string) (bool, error) {
	requestReady, _ := http.NewRequest("GET", url+".well-known/ready", bytes.NewReader([]byte{}))
	requestReady.Header.Set("content-type", "application/json")
	response_started, err := c.Do(requestReady)
	if err == nil {
		response_started.Body.Close()
	}
	alreadyRunning := err == nil && response_started.StatusCode == 200

	if alreadyRunning {
		fmt.Print("Weaviate instance already running.\n")
		return alreadyRunning, nil
	}

	fmt.Print("(Re-) build and start weaviate.\n")
	cmd := "./tools/test/run_ci_server.sh"
	if err := command(cmd, []string{}, true); err != nil {
		panic("Command to (re-) build and start weaviate failed: " + err.Error())
	}
	return false, nil
}
