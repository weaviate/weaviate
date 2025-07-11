//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type CommitLogCombiner struct {
	rootPath  string
	id        string
	threshold int64
	logger    logrus.FieldLogger
}

func NewCommitLogCombiner(rootPath, id string, threshold int64,
	logger logrus.FieldLogger,
) *CommitLogCombiner {
	return &CommitLogCombiner{
		rootPath:  rootPath,
		id:        id,
		threshold: threshold,
		logger:    logger,
	}
}

func (c *CommitLogCombiner) Do(partitions ...string) (bool, error) {
	// ensure partitions are sorted
	sort.Strings(partitions)

	executed := false
	for {
		// fileNames will already be in order
		fileNames, err := getCommitFileNames(c.rootPath, c.id, 0)
		if err != nil {
			return executed, errors.Wrap(err, "obtain files names")
		}

		anyOk := false
		for _, partFileNames := range c.partitonFileNames(fileNames, partitions) {
			ok, err := c.combineFirstMatch(partFileNames)
			if err != nil {
				return executed, err
			}
			anyOk = anyOk || ok
		}
		if !anyOk {
			break
		}
		executed = true
	}
	return executed, nil
}

func (c *CommitLogCombiner) partitonFileNames(fileNames, partitions []string) [][]string {
	if len(fileNames) == 0 {
		return [][]string{}
	}
	if len(partitions) == 0 {
		return [][]string{fileNames}
	}

	partitioned := make([][]string, 0, len(partitions)+1)

	i := 0
	partFileNames := []string{}
	for _, partition := range partitions {
		for ; i < len(fileNames); i++ {
			logname := strings.TrimSuffix(filepath.Base(fileNames[i]), ".condensed")
			if strings.Compare(logname, partition) > 0 {
				break
			}
			partFileNames = append(partFileNames, fileNames[i])
		}
		if len(partFileNames) > 0 {
			partitioned = append(partitioned, partFileNames)
			partFileNames = []string{}
		}
	}
	for ; i < len(fileNames); i++ {
		partFileNames = append(partFileNames, fileNames[i])
	}
	if len(fileNames) > 0 {
		partitioned = append(partitioned, partFileNames)
	}
	return partitioned
}

func (c *CommitLogCombiner) combineFirstMatch(fileNames []string) (bool, error) {
	for i, fileName := range fileNames {
		if !strings.HasSuffix(fileName, ".condensed") {
			// not an already condensed file, so no candidate for combining
			continue
		}

		if i == len(fileNames)-1 {
			// this is the last file, so there is nothing to combine it with
			return false, nil
		}

		if !strings.HasSuffix(fileNames[i+1], ".condensed") {
			// the next file is not a condensed file, so this file is not candidate
			// for merging with the next
			continue
		}

		currentStat, err := os.Stat(fileName)
		if err != nil {
			return false, errors.Wrapf(err, "stat file %q", fileName)
		}

		if currentStat.Size() > c.threshold {
			// already too big, can't combine further
			continue
		}

		nextStat, err := os.Stat(fileNames[i+1])
		if err != nil {
			return false, errors.Wrapf(err, "stat file %q", fileNames[i+1])
		}

		if currentStat.Size()+nextStat.Size() > c.threshold {
			// combining those two would exceed threshold
			continue
		}

		if err := c.combine(fileName, fileNames[i+1]); err != nil {
			return false, errors.Wrapf(err, "combine %q and %q", fileName, fileNames[i+1])
		}

		return true, nil
	}

	return false, nil
}

func (c *CommitLogCombiner) combine(left, right string) error {
	// all names are based on the first file, so that once file1 + file2 are
	// combined it is as if file2 had never existed and file 1 was just always
	// big enough to hold the contents of both

	// clearly indicate that the file is "in progress", in case we crash while
	// combining and the after restart there are multiple alternatives
	tmpName := strings.TrimSuffix(right, ".condensed") + (".combined.tmp")

	// finalName will look like an uncondensed original commit log, so the
	// condensor will pick it up without even knowing that it's a combined file
	finalName := strings.TrimSuffix(right, ".condensed")

	if err := c.mergeFiles(tmpName, left, right); err != nil {
		return errors.Wrap(err, "merge files")
	}

	if err := c.renameAndCleanUp(tmpName, finalName, left, right); err != nil {
		return errors.Wrap(err, "rename and clean up files")
	}

	c.logger.WithFields(logrus.Fields{
		"action":       "hnsw_commit_logger_combine_condensed_logs",
		"id":           c.id,
		"input_first":  left,
		"input_second": right,
		"output":       finalName,
	}).Info("successfully combined previously condensed commit log files")

	return nil
}

func (c *CommitLogCombiner) mergeFiles(outName, first, second string) error {
	out, err := os.Create(outName)
	if err != nil {
		return errors.Wrapf(err, "open target file %q", outName)
	}

	source1, err := os.Open(first)
	if err != nil {
		return errors.Wrapf(err, "open first source file %q", first)
	}
	defer source1.Close()

	source2, err := os.Open(second)
	if err != nil {
		return errors.Wrapf(err, "open second source file %q", second)
	}
	defer source2.Close()

	_, err = io.Copy(out, source1)
	if err != nil {
		return errors.Wrapf(err, "copy first source (%q) into target (%q)", first,
			outName)
	}

	_, err = io.Copy(out, source2)
	if err != nil {
		return errors.Wrapf(err, "copy second source (%q) into target (%q)", second,
			outName)
	}

	err = out.Close()
	if err != nil {
		return errors.Wrapf(err, "close target file %q", outName)
	}

	return nil
}

func (c *CommitLogCombiner) renameAndCleanUp(tmpName, finalName string,
	toDeletes ...string,
) error {
	// do the rename before the delete, because if we crash in between we end up
	// with duplicate files both with and without the ".condensed" suffix. The
	// new (and complete) merged file will not carry the suffix whereas the
	// sources will. This will look to the corrupted file fixer as if a
	// condensing had gone wrong and will delete the the source

	if err := os.Rename(tmpName, finalName); err != nil {
		return errors.Wrapf(err, "rename tmp (%q) to final (%q)", tmpName, finalName)
	}

	for _, toDelete := range toDeletes {
		if err := os.Remove(toDelete); err != nil {
			return errors.Wrapf(err, "clean up %q", toDelete)
		}
	}

	return nil
}
