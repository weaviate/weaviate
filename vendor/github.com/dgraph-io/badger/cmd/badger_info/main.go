/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* badger_info

Usage: badger_info --dir x [--value-dir y]

This command prints information about the badger key-value store.  It reads MANIFEST and prints its
info. It also prints info about missing/extra files, and general information about the value log
files (which are not referenced by the manifest).  Use this tool to report any issues about Badger
to the Dgraph team.
*/
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/table"
)

func main() {
	dirFlag := flag.String("dir", "", "The Badger database's index directory")
	valueDirFlag := flag.String("value-dir", "",
		"The Badger database's value log directory, if different from the index directory")

	flag.Parse()
	err := printInfo(*dirFlag, *valueDirFlag)
	if err != nil {
		fmt.Println("Error:", err.Error())
		os.Exit(1)
	}
}

func printInfo(dir, valueDir string) error {
	if dir == "" {
		return fmt.Errorf("--dir not supplied")
	}
	if valueDir == "" {
		valueDir = dir
	}
	fp, err := os.Open(filepath.Join(dir, badger.ManifestFilename))
	if err != nil {
		return err
	}
	defer func() {
		if fp != nil {
			fp.Close()
		}
	}()
	manifest, truncOffset, err := badger.ReplayManifestFile(fp)
	if err != nil {
		return err
	}
	fp.Close()
	fp = nil

	fileinfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}
	fileinfoByName := make(map[string]os.FileInfo)
	fileinfoMarked := make(map[string]bool)
	for _, info := range fileinfos {
		fileinfoByName[info.Name()] = info
		fileinfoMarked[info.Name()] = false
	}

	fmt.Print("[Manifest]\n")
	manifestTruncated := false
	manifestInfo, ok := fileinfoByName[badger.ManifestFilename]
	if ok {
		fileinfoMarked[badger.ManifestFilename] = true
		truncatedString := ""
		if truncOffset != manifestInfo.Size() {
			truncatedString = fmt.Sprintf(" [TRUNCATED to %d]", truncOffset)
			manifestTruncated = true
		}

		fmt.Printf("%-12s %10d  %s%s\n", manifestInfo.Name(), manifestInfo.Size(),
			manifestInfo.ModTime().Format(time.RFC3339), truncatedString)
	} else {
		fmt.Printf("%s [MISSING]\n", manifestInfo.Name())
	}

	numMissing := 0
	numEmpty := 0

	for level, lm := range manifest.Levels {
		fmt.Printf("[Level %d]\n", level)
		for tableID := range lm.Tables {
			tableFile := table.TableFilename(tableID)
			file, ok := fileinfoByName[tableFile]
			if ok {
				fileinfoMarked[tableFile] = true
				emptyString := ""
				if file.Size() == 0 {
					emptyString = " [EMPTY]"
					numEmpty++
				}
				// (Put level on every line to make easier to process with sed/perl.)
				fmt.Printf("%-12s %10d  %s %d%s\n", tableFile, file.Size(),
					file.ModTime().Format(time.RFC3339), level, emptyString)
			} else {
				fmt.Printf("%s [MISSING]\n", tableFile)
				numMissing++
			}
		}
	}

	valueDirFileinfos := fileinfos
	if valueDir != dir {
		valueDirFileinfos, err = ioutil.ReadDir(valueDir)
		if err != nil {
			return err
		}
	}

	// If valueDir is different from dir, holds extra files in the value dir.
	valueDirExtras := []os.FileInfo{}

	fmt.Print("[Value Log]\n")
	for _, file := range valueDirFileinfos {
		if !strings.HasSuffix(file.Name(), ".vlog") {
			if valueDir != dir {
				valueDirExtras = append(valueDirExtras, file)
			}
			continue
		}

		emptyString := ""
		if file.Size() == 0 {
			emptyString = " [EMPTY]"
			numEmpty++
		}
		fmt.Printf("%-12s %10d  %s%s\n", file.Name(), file.Size(),
			file.ModTime().Format(time.RFC3339), emptyString)

		fileinfoMarked[file.Name()] = true
	}

	numExtra := 0
	for _, file := range fileinfos {
		if fileinfoMarked[file.Name()] {
			continue
		}
		if numExtra == 0 {
			fmt.Print("[EXTRA]\n")
		}
		fmt.Printf("%-12s %10d  %s\n", file.Name(), file.Size(), file.ModTime().Format(time.RFC3339))
		numExtra++
	}

	numValueDirExtra := 0
	for _, file := range valueDirExtras {
		if numValueDirExtra == 0 {
			fmt.Print("[ValueDir EXTRA]\n")
		}
		fmt.Printf("%-12s %10d  %s\n", file.Name(), file.Size(), file.ModTime().Format(time.RFC3339))
		numValueDirExtra++
	}

	totalExtra := numExtra + numValueDirExtra
	if totalExtra == 0 && numMissing == 0 && numEmpty == 0 && !manifestTruncated {
		fmt.Println("Abnormalities: None.")
	} else {
		fmt.Println("Abnormalities:")
	}
	fmt.Printf("%d extra %s.\n", totalExtra, pluralFiles(totalExtra))
	fmt.Printf("%d missing %s.\n", numMissing, pluralFiles(numMissing))
	fmt.Printf("%d empty %s.\n", numEmpty, pluralFiles(numEmpty))
	fmt.Printf("%d truncated %s.\n", boolToNum(manifestTruncated), pluralManifest(manifestTruncated))

	return nil
}

func boolToNum(x bool) int {
	if x {
		return 1
	}
	return 0
}

func pluralManifest(manifestTruncated bool) string {
	if manifestTruncated {
		return "manifest"
	}
	return "manifests"
}

func pluralFiles(count int) string {
	if count == 1 {
		return "file"
	}
	return "files"
}
