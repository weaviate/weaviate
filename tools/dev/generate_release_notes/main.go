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

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
)

type pullRequest struct {
	Number int64      `json:"number"`
	Title  string     `json:"title"`
	User   githubUser `json:"user"`
}

type githubUser struct {
	Login string `json:"login"`
}

func main() {
	githubToken := os.Getenv("GITHUB_TOKEN")
	if githubToken == "" {
		fmt.Println("You need to pass GitHub token using GITHUB_TOKEN environment variable.")
		fmt.Println("How to create a Personal Access Token:")
		fmt.Println(" 1. Go to GitHub settings (https://github.com/settings/tokens).")
		fmt.Println(" 2. Generate a new token with at least the repo scope for accessing pull request information.")
		printUsage()
		return
	}
	currentVersion := os.Getenv("CURRENT_VERSION")
	if currentVersion == "" {
		fmt.Println("You need to pass current version using CURRENT_VERSION environment variable.")
		printUsage()
		return
	}
	previousVersion := os.Getenv("PREVIOUS_VERSION")
	if previousVersion == "" {
		fmt.Println("You need to pass previous version using PREVIOUS_VERSION environment variable.")
		printUsage()
		return
	}

	printReleaseNotes(currentVersion, previousVersion, githubToken)
}

func printReleaseNotes(currentTag, previousTag, githubToken string) {
	fmt.Printf("\n## Breaking Changes\n*none*\n")
	fmt.Printf("\n## New Features\n*none*\n")
	fmt.Printf("\n## Fixes\n")
	printPullRequests(currentTag, previousTag, githubToken)
	fmt.Printf("\n**Full Changelog**: https://github.com/weaviate/weaviate/compare/%s...%s\n", previousTag, currentTag)
}

func printPullRequests(currentTag, previousTag, githubToken string) {
	cmd := exec.Command("bash", getScriptPath(), getProperTag(currentTag), getProperTag(previousTag))
	out, err := cmd.Output()
	fatal(err)

	for _, line := range strings.Split(string(out), "\n") {
		pr := getPullRequest(parsePullRequestNumber(line), githubToken)
		fmt.Printf("* %s by @%s in https://github.com/weaviate/weaviate/pull/%d\n", pr.Title, pr.User.Login, pr.Number)
	}
}

func getScriptPath() string {
	cwd, err := os.Getwd()
	fatal(err)

	if strings.Contains(cwd, "/tools") {
		cwd = cwd[:strings.Index(cwd, "/tools")]
	}
	return fmt.Sprintf("%s/tools/dev/generate_release_notes/generate_changes.sh", cwd)
}

func parsePullRequestNumber(commitTitle string) string {
	if strings.HasPrefix(commitTitle, "Merge pull request #") {
		parts := strings.Split(commitTitle, " ")
		pr := strings.ReplaceAll(parts[3], "#", "")
		return pr
	} else {
		parts := strings.Split(commitTitle, " (#")
		pr := strings.ReplaceAll(parts[len(parts)-1], ")", "")
		return pr
	}
}

func getPullRequest(pr, githubToken string) pullRequest {
	url := fmt.Sprintf("https://api.github.com/repos/weaviate/weaviate/pulls/%s", pr)
	req, err := http.NewRequest("GET", url, nil)
	fatal(err)

	req.Header.Add("Accept", "application/vnd.github.v3+json")
	req.Header.Add("Authorization", fmt.Sprintf("token %s", githubToken))
	client := &http.Client{}
	res, err := client.Do(req)
	fatal(err)
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	fatal(err)

	var resp pullRequest
	err = json.Unmarshal(bodyBytes, &resp)
	fatal(err)

	return resp
}

func getProperTag(version string) string {
	if !strings.HasPrefix(version, "v") {
		return fmt.Sprintf("v%s", version)
	}
	return version
}

func fatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func printUsage() {
	fmt.Printf("\nExample usage:\n")
	fmt.Printf("GITHUB_TOKEN=\"<token>\" CURRENT_VERSION=v1.26.12 PREVIOUS_VERSION=v1.26.11  go run .\n")
}
