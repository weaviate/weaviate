//go:build ignore
// +build ignore

package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/AlecAivazis/survey/v2"
)

func main() {
	files, err := filepath.Glob("./backup/my-bucket/*.db")
	if err != nil {
		log.Fatal(err)
	}

	if len(files) == 0 {
		fmt.Println("No .db files found in ./backup/my-bucket/")
		return
	}

	var selectedFiles []string
	prompt := &survey.MultiSelect{
		Message: "Select segments files:",
		Options: files,
		Default: files, // This will pre-select all files by default
	}

	err = survey.AskOne(prompt, &selectedFiles)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Wiping local bucket in anticipation of restore...")
	os.RemoveAll("./data/my-bucket")
	os.MkdirAll("./data/my-bucket", 0755)

	fmt.Printf("Restoring backup...")
	segmentsCopied := 0
	filesCopied := 0

	for _, segment := range selectedFiles {
		withoutExt := strings.TrimSuffix(segment, filepath.Ext(segment))

		for _, ext := range []string{".db", ".bloom", ".cna"} {
			source := withoutExt + ext
			target := strings.Replace(withoutExt, "backup/", "data/", 1)
			target = target + ext

			os.MkdirAll(filepath.Dir(target), 0o755)
			cpCmd := exec.Command("cp", "-rf", source, target)
			cpCmd.Stdout = os.Stdout
			cpCmd.Stderr = os.Stderr
			err := cpCmd.Run()
			if err != nil {
				panic(err)
			}
			filesCopied++
		}
		segmentsCopied++
	}

	fmt.Printf("Copied %d segments (%d files)\n", segmentsCopied, filesCopied)
}
