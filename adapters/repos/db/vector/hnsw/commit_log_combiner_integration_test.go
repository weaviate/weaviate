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

package hnsw

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_CommitlogCombiner(t *testing.T) {
	// For the combiner the contents of a commit log file don't actually matter
	// so we can put arbitrary data in the files. It will only make decisions
	// about what should be appended, the actual condensing will be taken care of
	// by the condensor

	logger, _ := test.NewNullLogger()

	t.Run("without partitions", func(t *testing.T) {
		rootPath := t.TempDir()

		threshold := int64(1000)
		id := "combiner_test"
		// create commit logger directory
		require.Nil(t, os.MkdirAll(commitLogDirectory(rootPath, id), 0o777))

		name := func(fileName string) string {
			return commitLogFileName(rootPath, id, fileName)
		}

		t.Run("create several condensed files below the threshold", func(t *testing.T) {
			// 4 files of 300 bytes each, with 1000 byte threshold. This lets us verify
			// that one and two will be combined, so will three and four.
			require.Nil(t, createDummyFile(name("1000.condensed"), []byte("file1\n"), 300))
			require.Nil(t, createDummyFile(name("1001.condensed"), []byte("file2\n"), 300))
			require.Nil(t, createDummyFile(name("1002.condensed"), []byte("file3\n"), 300))
			require.Nil(t, createDummyFile(name("1003.condensed"), []byte("file4\n"), 300))
			require.Nil(t, createDummyFile(name("1004"), []byte("current\n"), 50))
		})

		t.Run("run combiner", func(t *testing.T) {
			_, err := NewCommitLogCombiner(rootPath, id, threshold, logger).Do()
			require.Nil(t, err)
		})

		t.Run("we are now left with combined files", func(t *testing.T) {
			dir, err := os.Open(commitLogDirectory(rootPath, id))
			require.Nil(t, err)

			fileNames, err := dir.Readdirnames(0)
			require.Nil(t, err)
			require.Len(t, fileNames, 3)
			require.ElementsMatch(t, []string{"1001", "1003", "1004"}, fileNames)

			t.Run("the first file is correctly combined", func(t *testing.T) {
				contents, err := os.ReadFile(commitLogFileName(rootPath, id, "1001"))
				require.Nil(t, err)
				require.Len(t, contents, 600)
				assert.Equal(t, contents[0:6], []byte("file1\n"))
				assert.Equal(t, contents[300:306], []byte("file2\n"))
			})

			t.Run("the second file is correctly combined", func(t *testing.T) {
				contents, err := os.ReadFile(commitLogFileName(rootPath, id, "1003"))
				require.Nil(t, err)
				require.Len(t, contents, 600)
				assert.Equal(t, contents[0:6], []byte("file3\n"))
				assert.Equal(t, contents[300:306], []byte("file4\n"))
			})

			t.Run("latest file is unchanged", func(t *testing.T) {
				contents, err := os.ReadFile(commitLogFileName(rootPath, id, "1004"))
				require.Nil(t, err)
				require.Len(t, contents, 50)
				assert.Equal(t, contents[0:8], []byte("current\n"))
				assert.Equal(t, contents[42:], []byte("rrent\ncu"))
			})
		})
	})

	t.Run("with partitions", func(t *testing.T) {
		rootPath := t.TempDir()
		createLogFiles := func(t *testing.T, id string, commitLogFile func(string) string) {
			require.NoError(t, os.MkdirAll(commitLogDirectory(rootPath, id), 0o777))
			require.NoError(t, createDummyFile(commitLogFile("1001.condensed"), []byte("file1\n"), 800))
			require.NoError(t, createDummyFile(commitLogFile("1002.condensed"), []byte("file2\n"), 700))
			require.NoError(t, createDummyFile(commitLogFile("1003.condensed"), []byte("file3\n"), 600))
			require.NoError(t, createDummyFile(commitLogFile("1004.condensed"), []byte("file4\n"), 500))
			require.NoError(t, createDummyFile(commitLogFile("1005.condensed"), []byte("file5\n"), 400))
			require.NoError(t, createDummyFile(commitLogFile("1006.condensed"), []byte("file6\n"), 300))
			require.NoError(t, createDummyFile(commitLogFile("1007.condensed"), []byte("file7\n"), 200))
			require.NoError(t, createDummyFile(commitLogFile("1008.condensed"), []byte("file8\n"), 100))
			require.NoError(t, createDummyFile(commitLogFile("1009"), []byte("current\n"), 50))
		}
		assertFilesExist := func(t *testing.T, id string, names ...string) {
			dir, err := os.Open(commitLogDirectory(rootPath, id))
			require.Nil(t, err)

			fileNames, err := dir.Readdirnames(0)
			require.NoError(t, err)
			require.Len(t, fileNames, len(names))
			require.ElementsMatch(t, names, fileNames)
		}
		assertFileContains := func(t *testing.T, commitLogFile string, expectedSize int, expectedContentByOffset map[int]string) {
			contents, err := os.ReadFile(commitLogFile)
			require.NoError(t, err)
			require.Len(t, contents, expectedSize)
			for off, cont := range expectedContentByOffset {
				bcont := []byte(cont)
				assert.Equal(t, contents[off:off+len(bcont)], bcont)
			}
		}

		t.Run("no partition", func(t *testing.T) {
			id := "combiner_test_no_partition"
			threshold := 10_000
			commitLogFile := func(name string) string { return commitLogFileName(rootPath, id, name) }

			t.Run("create log files", func(t *testing.T) {
				createLogFiles(t, id, commitLogFile)
			})

			t.Run("combine", func(t *testing.T) {
				_, err := NewCommitLogCombiner(rootPath, id, int64(threshold), logger).Do()
				require.NoError(t, err)
			})

			t.Run("verify combined files", func(t *testing.T) {
				assertFilesExist(t, id, "1002", "1004", "1006", "1008", "1009")

				assertFileContains(t, commitLogFile("1002"), 1500, map[int]string{0: "file1\n", 800: "file2\n"})
				assertFileContains(t, commitLogFile("1004"), 1100, map[int]string{0: "file3\n", 600: "file4\n"})
				assertFileContains(t, commitLogFile("1006"), 700, map[int]string{0: "file5\n", 400: "file6\n"})
				assertFileContains(t, commitLogFile("1008"), 300, map[int]string{0: "file7\n", 200: "file8\n"})
				assertFileContains(t, commitLogFile("1009"), 50, map[int]string{0: "current\n", 42: "rrent\ncu"})
			})
		})

		t.Run("partition 1004+1008", func(t *testing.T) {
			id := "combiner_test_partition_1004_1008"
			threshold := 10_000
			commitLogFile := func(name string) string { return commitLogFileName(rootPath, id, name) }

			t.Run("create log files", func(t *testing.T) {
				createLogFiles(t, id, commitLogFile)
			})

			t.Run("combine", func(t *testing.T) {
				_, err := NewCommitLogCombiner(rootPath, id, int64(threshold), logger).Do("1004", "1008")
				require.NoError(t, err)
			})

			t.Run("verify combined files", func(t *testing.T) {
				assertFilesExist(t, id, "1002", "1004", "1006", "1008", "1009")

				assertFileContains(t, commitLogFile("1002"), 1500, map[int]string{0: "file1\n", 800: "file2\n"})
				assertFileContains(t, commitLogFile("1004"), 1100, map[int]string{0: "file3\n", 600: "file4\n"})
				assertFileContains(t, commitLogFile("1006"), 700, map[int]string{0: "file5\n", 400: "file6\n"})
				assertFileContains(t, commitLogFile("1008"), 300, map[int]string{0: "file7\n", 200: "file8\n"})
				assertFileContains(t, commitLogFile("1009"), 50, map[int]string{0: "current\n", 42: "rrent\ncu"})
			})
		})

		t.Run("partition 1003+1006", func(t *testing.T) {
			id := "combiner_test_partition_1003_1006"
			threshold := 10_000
			commitLogFile := func(name string) string { return commitLogFileName(rootPath, id, name) }

			t.Run("create log files", func(t *testing.T) {
				createLogFiles(t, id, commitLogFile)
			})

			t.Run("combine", func(t *testing.T) {
				_, err := NewCommitLogCombiner(rootPath, id, int64(threshold), logger).Do("1003", "1006")
				require.NoError(t, err)
			})

			t.Run("verify combined files", func(t *testing.T) {
				assertFilesExist(t, id, "1002", "1003.condensed", "1005", "1006.condensed", "1008", "1009")

				assertFileContains(t, commitLogFile("1002"), 1500, map[int]string{0: "file1\n", 800: "file2\n"})
				assertFileContains(t, commitLogFile("1003.condensed"), 600, map[int]string{0: "file3\n"})
				assertFileContains(t, commitLogFile("1005"), 900, map[int]string{0: "file4\n", 500: "file5\n"})
				assertFileContains(t, commitLogFile("1006.condensed"), 300, map[int]string{0: "file6\n"})
				assertFileContains(t, commitLogFile("1008"), 300, map[int]string{0: "file7\n", 200: "file8\n"})
				assertFileContains(t, commitLogFile("1009"), 50, map[int]string{0: "current\n", 42: "rrent\ncu"})
			})
		})

		t.Run("partition 1003, low threshold", func(t *testing.T) {
			id := "combiner_test_partition_1003_1006_low_threshold"
			threshold := 1000
			commitLogFile := func(name string) string { return commitLogFileName(rootPath, id, name) }

			t.Run("create log files", func(t *testing.T) {
				createLogFiles(t, id, commitLogFile)
			})

			t.Run("combine", func(t *testing.T) {
				_, err := NewCommitLogCombiner(rootPath, id, int64(threshold), logger).Do("1003")
				require.NoError(t, err)
			})

			t.Run("verify combined files", func(t *testing.T) {
				assertFilesExist(t, id, "1001.condensed", "1002.condensed", "1003.condensed", "1005", "1007",
					"1008.condensed", "1009")

				assertFileContains(t, commitLogFile("1001.condensed"), 800, map[int]string{0: "file1\n"})
				assertFileContains(t, commitLogFile("1002.condensed"), 700, map[int]string{0: "file2\n"})
				assertFileContains(t, commitLogFile("1003.condensed"), 600, map[int]string{0: "file3\n"})
				assertFileContains(t, commitLogFile("1005"), 900, map[int]string{0: "file4\n", 500: "file5\n"})
				assertFileContains(t, commitLogFile("1007"), 500, map[int]string{0: "file6\n", 300: "file7\n"})
				assertFileContains(t, commitLogFile("1008.condensed"), 100, map[int]string{0: "file8\n"})
				assertFileContains(t, commitLogFile("1009"), 50, map[int]string{0: "current\n", 42: "rrent\ncu"})
			})
		})

		t.Run("partition 1005", func(t *testing.T) {
			id := "combiner_test_partition_1005"
			threshold := 10_000
			commitLogFile := func(name string) string { return commitLogFileName(rootPath, id, name) }

			t.Run("create log files", func(t *testing.T) {
				createLogFiles(t, id, commitLogFile)
			})

			t.Run("combine", func(t *testing.T) {
				_, err := NewCommitLogCombiner(rootPath, id, int64(threshold), logger).Do("1005")
				require.NoError(t, err)
			})

			t.Run("verify combined files", func(t *testing.T) {
				assertFilesExist(t, id, "1002", "1004", "1005.condensed", "1007", "1008.condensed", "1009")

				assertFileContains(t, commitLogFile("1002"), 1500, map[int]string{0: "file1\n", 800: "file2\n"})
				assertFileContains(t, commitLogFile("1004"), 1100, map[int]string{0: "file3\n", 600: "file4\n"})
				assertFileContains(t, commitLogFile("1005.condensed"), 400, map[int]string{0: "file5\n"})
				assertFileContains(t, commitLogFile("1007"), 500, map[int]string{0: "file6\n", 300: "file7\n"})
				assertFileContains(t, commitLogFile("1008.condensed"), 100, map[int]string{0: "file8\n"})
				assertFileContains(t, commitLogFile("1009"), 50, map[int]string{0: "current\n", 42: "rrent\ncu"})
			})
		})

		t.Run("partition 1005, low threshold", func(t *testing.T) {
			id := "combiner_test_partition_1005_low_threshold"
			threshold := 1200
			commitLogFile := func(name string) string { return commitLogFileName(rootPath, id, name) }

			t.Run("create log files", func(t *testing.T) {
				createLogFiles(t, id, commitLogFile)
			})

			t.Run("combine", func(t *testing.T) {
				_, err := NewCommitLogCombiner(rootPath, id, int64(threshold), logger).Do("1005")
				require.NoError(t, err)
			})

			t.Run("verify combined files", func(t *testing.T) {
				assertFilesExist(t, id, "1001.condensed", "1002.condensed", "1004", "1005.condensed",
					"1007", "1008.condensed", "1009")

				assertFileContains(t, commitLogFile("1001.condensed"), 800, map[int]string{0: "file1\n"})
				assertFileContains(t, commitLogFile("1002.condensed"), 700, map[int]string{0: "file2\n"})
				assertFileContains(t, commitLogFile("1004"), 1100, map[int]string{0: "file3\n", 600: "file4\n"})
				assertFileContains(t, commitLogFile("1005.condensed"), 400, map[int]string{0: "file5\n"})
				assertFileContains(t, commitLogFile("1007"), 500, map[int]string{0: "file6\n", 300: "file7\n"})
				assertFileContains(t, commitLogFile("1008.condensed"), 100, map[int]string{0: "file8\n"})
				assertFileContains(t, commitLogFile("1009"), 50, map[int]string{0: "current\n", 42: "rrent\ncu"})
			})
		})

		t.Run("multiple partitions", func(t *testing.T) {
			id := "combiner_test_multiple_partitions"
			threshold := 10_000
			commitLogFile := func(name string) string { return commitLogFileName(rootPath, id, name) }

			t.Run("create log files", func(t *testing.T) {
				createLogFiles(t, id, commitLogFile)
			})

			t.Run("combine", func(t *testing.T) {
				_, err := NewCommitLogCombiner(rootPath, id, int64(threshold), logger).Do("1001", "1002",
					"1003", "1004", "1005", "1006", "1007", "1008", "1009", "1010")
				require.NoError(t, err)
			})

			t.Run("verify combined files", func(t *testing.T) {
				assertFilesExist(t, id, "1001.condensed", "1002.condensed", "1003.condensed", "1004.condensed",
					"1005.condensed", "1006.condensed", "1007.condensed", "1008.condensed", "1009")

				assertFileContains(t, commitLogFile("1001.condensed"), 800, map[int]string{0: "file1\n"})
				assertFileContains(t, commitLogFile("1002.condensed"), 700, map[int]string{0: "file2\n"})
				assertFileContains(t, commitLogFile("1003.condensed"), 600, map[int]string{0: "file3\n"})
				assertFileContains(t, commitLogFile("1004.condensed"), 500, map[int]string{0: "file4\n"})
				assertFileContains(t, commitLogFile("1005.condensed"), 400, map[int]string{0: "file5\n"})
				assertFileContains(t, commitLogFile("1006.condensed"), 300, map[int]string{0: "file6\n"})
				assertFileContains(t, commitLogFile("1007.condensed"), 200, map[int]string{0: "file7\n"})
				assertFileContains(t, commitLogFile("1008.condensed"), 100, map[int]string{0: "file8\n"})
				assertFileContains(t, commitLogFile("1009"), 50, map[int]string{0: "current\n", 42: "rrent\ncu"})
			})
		})
	})
}

func createDummyFile(fileName string, content []byte, size int) error {
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}

	defer f.Close()

	written := 0
	for {
		if size == written {
			break
		}

		if size-written < len(content) {
			content = content[:(size - written)]
		}

		n, err := f.Write([]byte(content))
		written += n

		if err != nil {
			return err
		}
	}

	return nil
}
