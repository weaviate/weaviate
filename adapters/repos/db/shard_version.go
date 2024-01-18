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

package db

import (
	"encoding/binary"
	"os"

	"github.com/pkg/errors"
)

// ShardCodeBaseVersion must be increased whenever there are breaking changes -
// including those that we can handle in a non-breaking way
// the version checker can then decide on init if it should prevent startup
// completely. If it does not prevent startup, but there is still a version
// mismatch, the version can be used to make specific decisions
//
// CHANGELOG
//   - Version 1 - Everything up until Weaviate v1.10.1 inclusive
//   - Version 2 - Inverted Index is now stored in an always sorted fashion and
//     doc ids are stored as BigEndian. To make this backward-compatible with v1,
//     doc ids need to be read and written as Little Endian. In addition, an
//     additional sort step is required in three places: during a MapList call,
//     during a Map Cursor and during Map Compactions. BM25 is entirely disabled
//     prior to this version
const (
	ShardCodeBaseVersion                  = uint16(2)
	ShardCodeBaseMinimumVersionForStartup = uint16(1)
)

type shardVersioner struct {
	version uint16

	// we don't need the file after initialization, but still need to track its
	// path so we can delete it on .Drop()
	path string
}

func newShardVersioner(baseDir string, dataPresent bool) (*shardVersioner, error) {
	sv := &shardVersioner{}

	return sv, sv.init(baseDir, dataPresent)
}

func (sv *shardVersioner) init(fileName string, dataPresent bool) error {
	sv.path = fileName

	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return err
	}

	stat, err := f.Stat()
	if err != nil {
		return err
	}

	var version uint16 = 1
	if stat.Size() > 0 {
		// the file has existed before, we need to initialize with its content
		err := binary.Read(f, binary.LittleEndian, &version)
		if err != nil {
			return errors.Wrap(err, "read initial version from file")
		}
	} else {
		// if the version file does not yet exist, there are two scenarios:
		// 1) We are just creating this class, which means its version is
		//    ShardCodeBaseVersion.
		// 2) There is data present, so we must assume it was built with a version
		//    that did not yet have this versioner present, so we assume it's v1
		if !dataPresent {
			version = ShardCodeBaseVersion
		} else {
			version = 1
		}

		err := binary.Write(f, binary.LittleEndian, &version)
		if err != nil {
			return errors.Wrap(err, "write version back to file")
		}

		if err := f.Close(); err != nil {
			return errors.Wrap(err, "close version file")
		}
	}

	if version < ShardCodeBaseMinimumVersionForStartup {
		return errors.Errorf("cannot start up shard: it was built with shard "+
			"version v%d, but this version of Weaviate requires at least shard version v%d",
			version, ShardCodeBaseMinimumVersionForStartup)
	}

	sv.version = version

	return nil
}

func (sv *shardVersioner) Drop() error {
	err := os.Remove(sv.path)
	if err != nil {
		return errors.Wrap(err, "drop versioner file")
	}
	return nil
}

func (sv *shardVersioner) Version() uint16 {
	return sv.version
}
