// Package implements performance tracking examples

package main

import (
	"bytes"
	"encoding/json"
	"errors"
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

type benchmarkResult map[string]int64

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

	var newRuntime int64
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
	currentBenchmarkRunTimes := readCurrentBenchmarkResults()
	currentRuntime := currentBenchmarkRunTimes[FullBenchmarkName]
	currentBenchmarkRunTimes[FullBenchmarkName] = newRuntime
	benchmarkJSON, _ := json.MarshalIndent(currentBenchmarkRunTimes, "", "\t")
	if err := os.WriteFile("benchmark_results.json", benchmarkJSON, 0o666); err != nil {
		panic(err)
	}

	fmt.Fprint(
		os.Stdout,
		"Runtime for benchmark "+FullBenchmarkName+
			": old runtime: "+fmt.Sprint(currentRuntime)+"ms, new runtime:"+fmt.Sprint(newRuntime)+"ms.\n"+
			"This is a change of "+fmt.Sprintf("%.2f", 100*float32(newRuntime-currentRuntime)/float32(newRuntime))+"%.\n"+
			"Please update the benchmark results if necessary.\n",
	)

	// Return with error code if runtime regressed and corresponding flag was set
	if failPercentage >= 0 &&
		currentRuntime > 0 && // don't report regression if no old entry exists
		float64(currentRuntime)*(1.0+0.01*float64(failPercentage)) < float64(newRuntime) {
		fmt.Fprint(
			os.Stderr, "Failed due to performance regressions.\n",
		)
		os.Exit(1)
	}
}

func command(app string, arguments []string) error {
	mydir, err := os.Getwd()
	if err != nil {
		return err
	}

	cmd := exec.Command(app, arguments...)
	execDir := mydir + "/../"
	cmd.Dir = execDir
	err = cmd.Start()
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
	return command(app, arguments)
}

// start weaviate in case it was not already started
func startWeavaite(c *http.Client, url string) (bool, error) {
	requestReady, _ := http.NewRequest("GET", url+".well-known/ready", bytes.NewReader([]byte{}))
	requestReady.Header.Set("content-type", "application/json")
	response_started, err := c.Do(requestReady)
	if err == nil {
		response_started.Body.Close()
	}
	alreadyRunning := err == nil && response_started.StatusCode == 200

	if alreadyRunning {
		return alreadyRunning, nil
	}

	app := "docker-compose"
	arguments := []string{
		"up",
		"-d",
	}

	if err := command(app, arguments); err != nil {
		panic("Command to start weaviate failed.")
	}

	for i := 0; i < 20; i++ {
		response, err := c.Do(requestReady)
		if err == nil {
			response.Body.Close()
		}
		if err == nil && response.StatusCode == 200 {
			return false, nil
		}

		fmt.Printf("Weaviate not yet up waiting another 3 seconds. Iteration: %v\n", i)
		time.Sleep(time.Second * 3)
	}
	return false, errors.New("could not start weaviate after 20 iterations")
}
