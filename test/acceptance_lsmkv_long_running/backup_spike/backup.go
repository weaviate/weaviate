package main

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func main() {
	candidates := []string{}
	err := filepath.WalkDir("./data", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		if filepath.Ext(path) == ".db" {
			candidates = append(candidates, path)
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	levelZeroSegments := []string{}
	for _, candidate := range candidates {
		f, err := os.Open(candidate)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		header, err := segmentindex.ParseHeader(f)
		if err != nil {
			panic(err)
		}

		if header.Level == 0 {
			levelZeroSegments = append(levelZeroSegments, candidate)
		}
	}

	segmentsCopied := 0
	filesCopied := 0
	for _, segment := range levelZeroSegments {
		withoutExt := strings.TrimSuffix(segment, filepath.Ext(segment))

		for _, ext := range []string{".db", ".bloom", ".cna"} {
			source := withoutExt + ext
			target := strings.Replace(withoutExt, "data/", "backup/", 1)
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
