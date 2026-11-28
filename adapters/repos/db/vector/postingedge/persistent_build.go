//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package postingedge

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// What a build of the base leaves in its directory before it writes anything
// of the base: the list of what it creates, and the doc ids of the vectors it
// builds the base from (integration-plan.md, section 9).
const (
	buildListFile    = "list.json"
	buildSourcesFile = "sources.u32"
	buildListVersion = 1
)

// buildList is what a build of the base creates, and what it builds it from.
type buildList struct {
	Version int `json:"version"`
	// Files are in the directory of the index, by name.
	Files []string `json:"files"`
	// Buckets are in the store of the shard, by name.
	Buckets []string `json:"buckets"`
	// Sources is how many doc ids the base is built from, which are in
	// buildSourcesFile, and SourcesCRC is the checksum of that file.
	Sources    int    `json:"sources"`
	SourcesCRC uint32 `json:"sourcesCrc32"`

	sources []uint64
}

// baseBucketNames are the names in the store of every bucket a base of the
// index may have: all of its buckets but the one of the vectors kept aside.
func (p *Persistent) baseBucketNames() []string {
	kinds := []string{
		growBucketPostings, growBucketMap, growBucketRouting, growBucketDists,
		growBucketEdges, growBucketDeleted, growBucketPending,
	}
	names := make([]string, len(kinds))
	for i, kind := range kinds {
		names[i] = p.bucketName(kind)
	}
	return names
}

// bucketThere tells whether a bucket of the index is on disk or open in the
// store.
func (p *Persistent) bucketThere(name string) (bool, error) {
	if p.store.Bucket(name) != nil {
		return true, nil
	}
	path := filepath.Join(p.store.GetDir(), name)
	switch _, err := os.Lstat(path); {
	case err == nil:
		return true, nil
	case os.IsNotExist(err):
		return false, nil
	default:
		return false, errors.Wrapf(err, "look for %s", path)
	}
}

// baseLeft lists what there is on disk of a base of the index: the files of
// its directory, by name, and its buckets in the store but for the one of the
// vectors kept aside, by name.
func (p *Persistent) baseLeft() (files, buckets []string, err error) {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return nil, nil, errors.Wrap(err, "read the directory of the index")
	}
	for _, entry := range entries {
		files = append(files, entry.Name())
	}
	for _, name := range p.baseBucketNames() {
		there, err := p.bucketThere(name)
		if err != nil {
			return nil, nil, err
		}
		if there {
			buckets = append(buckets, name)
		}
	}
	return files, buckets, nil
}

// paths gives the files of the index and its buckets by where they are.
func (p *Persistent) paths(files, buckets []string) []string {
	paths := make([]string, 0, len(files)+len(buckets))
	for _, name := range files {
		paths = append(paths, filepath.Join(p.dir, name))
	}
	for _, name := range buckets {
		paths = append(paths, filepath.Join(p.store.GetDir(), name))
	}
	return paths
}

// writeBuildList leaves in the directory of the build what the build creates
// and the doc ids of the vectors it builds the base from, before it creates
// any of it. The caller has checked that nothing of a base is there: what a
// cleanup removes is never older than the build. The doc ids go first and the
// list last, each whole before it has its name: a list on disk has every doc
// id it counts.
func (p *Persistent) writeBuildList(files, buckets []string, ids []uint64) (*buildList, error) {
	if err := os.MkdirAll(p.buildDir, 0o755); err != nil {
		return nil, errors.Wrap(err, "make the directory of the build of the base")
	}
	raw := make([]byte, 4*len(ids))
	for i, id := range ids {
		binary.BigEndian.PutUint32(raw[4*i:], uint32(id))
	}
	list := &buildList{
		Version: buildListVersion, Files: files, Buckets: buckets,
		Sources: len(ids), SourcesCRC: crc32.ChecksumIEEE(raw), sources: ids,
	}
	encoded, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return nil, errors.Wrap(err, "encode the list of the build of the base")
	}
	if err := writeSynced(filepath.Join(p.buildDir, buildSourcesFile), raw); err != nil {
		return nil, errors.Wrap(err, "write the doc ids the base is built from")
	}
	if err := writeSynced(filepath.Join(p.buildDir, buildListFile), encoded); err != nil {
		return nil, errors.Wrap(err, "write the list of the build of the base")
	}
	return list, errors.Wrap(syncDir(p.buildDir), "write the list of the build of the base")
}

// readBuildList reads the list a build of the base left, nil if it left
// none. A list that names a file outside the directory of the index, a bucket
// that is not one of a base, or doc ids that are not whole is an error.
func (p *Persistent) readBuildList() (*buildList, error) {
	path := filepath.Join(p.buildDir, buildListFile)
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "read the list of a build of the base")
	}
	var list buildList
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, errors.Wrapf(err, "read the list of a build of the base, %s", path)
	}
	if list.Version != buildListVersion {
		return nil, errors.Errorf("the list of a build of the base, %s, is of version %d: this index reads %d",
			path, list.Version, buildListVersion)
	}
	for _, name := range list.Files {
		if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
			return nil, errors.Errorf("the list of a build of the base, %s, names %q, which is not a file of the directory of the index",
				path, name)
		}
	}
	bases := p.baseBucketNames()
	for _, name := range list.Buckets {
		if !slices.Contains(bases, name) {
			return nil, errors.Errorf("the list of a build of the base, %s, names %q, which is not a bucket of a base of the index",
				path, name)
		}
	}
	sources, err := os.ReadFile(filepath.Join(p.buildDir, buildSourcesFile))
	if err != nil {
		return nil, errors.Wrapf(err, "read the doc ids of the list of a build of the base, %s", path)
	}
	if len(sources) != 4*list.Sources || crc32.ChecksumIEEE(sources) != list.SourcesCRC {
		return nil, errors.Errorf("the doc ids of the list of a build of the base, %s, are not whole: %d bytes for %d doc ids",
			path, len(sources), list.Sources)
	}
	list.sources = make([]uint64, list.Sources)
	for i := range list.sources {
		list.sources[i] = uint64(binary.BigEndian.Uint32(sources[4*i:]))
	}
	return &list, nil
}

// cleanBuild removes what a build of the base that did not end left: what its
// list names and is there, and nothing else. It removes nothing unless
// nothing of a base that the list does not name is there, and every vector
// the base was built from is still kept aside, with its vector or with the
// mark of a delete made while it was built, which kept tells
// (integration-plan.md, section 9). It leaves a line in the log with what it
// removed and why. The meta goes first: a cleanup that stops halfway never
// leaves a meta next to part of a base.
func (p *Persistent) cleanBuild(ctx context.Context, list *buildList, kept func(id uint64) bool, why string) error {
	files, buckets, err := p.baseLeft()
	if err != nil {
		return err
	}
	if err := p.outsideList(list, files, buckets); err != nil {
		return err
	}
	var missing []uint64
	for _, id := range list.sources {
		if !kept(id) {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		return errors.Errorf("a build of the base did not end, and %d of the %d vectors it was built from are not "+
			"kept aside (%s): nothing is removed", len(missing), len(list.sources), docsOf(missing))
	}

	var removed []string
	remove := func(name string) error {
		path := filepath.Join(p.dir, name)
		if err := os.RemoveAll(path); err != nil {
			return errors.Wrapf(err, "remove %s", path)
		}
		removed = append(removed, path)
		return nil
	}
	// The meta first, and the meta written next to its place with it.
	isMeta := func(name string) bool { return name == diskMetaFile || name == diskMetaFile+".tmp" }
	for _, name := range files {
		if isMeta(name) {
			if err := remove(name); err != nil {
				return err
			}
		}
	}
	if len(removed) > 0 {
		if err := syncDir(p.dir); err != nil {
			return errors.Wrap(err, "remove the meta of a base that was not built whole")
		}
	}
	for _, name := range files {
		if !isMeta(name) {
			if err := remove(name); err != nil {
				return err
			}
		}
	}
	for _, name := range buckets {
		if err := shutdownBucket(ctx, p.store, name); err != nil {
			return errors.Wrapf(err, "close the bucket %s", name)
		}
		path := filepath.Join(p.store.GetDir(), name)
		if err := os.RemoveAll(path); err != nil {
			return errors.Wrapf(err, "remove %s", path)
		}
		removed = append(removed, path)
	}
	what := "nothing, as nothing of it is there"
	if len(removed) > 0 {
		what = strings.Join(removed, ", ")
	}
	p.logger.WithFields(logrus.Fields{
		"action":  "postingedge_clean_build",
		"removed": len(removed),
		"vectors": len(list.sources),
	}).Warnf("removed what a build of the base wrote, because %s, and the list it left names it all and "+
		"the %d vectors it was built from are kept aside: %s", why, len(list.sources), what)
	return nil
}

// outsideList is an error if anything of a base that the list of a build
// does not name is there.
func (p *Persistent) outsideList(list *buildList, files, buckets []string) error {
	var outside []string
	for _, name := range files {
		if !slices.Contains(list.Files, name) {
			outside = append(outside, filepath.Join(p.dir, name))
		}
	}
	for _, name := range buckets {
		if !slices.Contains(list.Buckets, name) {
			outside = append(outside, filepath.Join(p.store.GetDir(), name))
		}
	}
	if len(outside) > 0 {
		return errors.Errorf("a build of the base did not end, and the index holds %s, which the list of the build "+
			"does not name: nothing is removed", strings.Join(outside, ", "))
	}
	return nil
}

// removeBuildDir removes the directory of a build of the base, its mark and
// its scratch, with a line in the log that says why.
func (p *Persistent) removeBuildDir(why string) error {
	switch _, err := os.Lstat(p.buildDir); {
	case os.IsNotExist(err):
		return nil
	case err != nil:
		return errors.Wrap(err, "look for the directory of a build of the base")
	}
	if err := os.RemoveAll(p.buildDir); err != nil {
		return errors.Wrapf(err, "remove the directory of a build of the base, %s", p.buildDir)
	}
	p.logger.WithField("action", "postingedge_clean_build").
		Infof("removed %s, the directory of a build of the base, because %s", p.buildDir, why)
	return nil
}

// openWithoutMeta opens an index that has no meta, and so no base: it keeps
// its vectors aside. What there is on disk of a base is of a build that did
// not end, and is removed if the list the build left names all of it and
// every vector the base was built from is still kept aside; otherwise the
// index does not open, and nothing is removed. An index with no meta and
// files of a base that no list names is what a copy or a restore that left
// the meta out leaves (integration-plan.md, section 9).
func (p *Persistent) openWithoutMeta(ctx context.Context) error {
	list, err := p.readBuildList()
	if err != nil {
		return errors.Wrap(err, "nothing is removed")
	}
	files, buckets, err := p.baseLeft()
	if err != nil {
		return err
	}
	left := len(files)+len(buckets) > 0
	if left && list == nil {
		return errors.Errorf("the index has no %s and holds %s: a base without its meta, which a copy or a restore "+
			"that left the meta out leaves, and no build of the base left a list of it; nothing is removed",
			diskMetaFile, strings.Join(p.paths(files, buckets), ", "))
	}
	if left {
		if err := p.outsideList(list, files, buckets); err != nil {
			return err
		}
		// The vectors the base was built from are kept aside in their bucket,
		// which is not made here when it is not there.
		there, err := p.bucketThere(p.stagingName())
		if err != nil {
			return err
		}
		if !there {
			return errors.Errorf("a build of the base did not end, and the %d vectors it was built from are not "+
				"kept aside: there is no bucket %s; nothing is removed", list.Sources, p.stagingName())
		}
	}
	if err := p.openStaging(ctx); err != nil {
		return err
	}
	if err := p.loadStaged(); err != nil {
		_ = p.closeStaging(ctx)
		return err
	}
	if left {
		kept := func(id uint64) bool {
			_, staged := p.staged[id]
			_, gone := p.gone[id]
			return staged || gone
		}
		if err := p.cleanBuild(ctx, list, kept, "the index was opened with no meta, after a build of the base that "+
			"did not end"); err != nil {
			_ = p.closeStaging(ctx)
			return err
		}
	}
	if err := p.removeBuildDir("a build of the base did not end, and nothing of the base it was writing is " +
		"left"); err != nil {
		_ = p.closeStaging(ctx)
		return err
	}
	// A vector that was deleted while a base was built: that base is not
	// there, and neither is the vector.
	if err := p.settleGone(); err != nil {
		_ = p.closeStaging(ctx)
		return err
	}
	return nil
}

// docsOf names the first doc ids of a list, and how many more there are.
func docsOf(ids []uint64) string {
	const shown = 5
	names := make([]string, 0, shown)
	for _, id := range ids[:min(len(ids), shown)] {
		names = append(names, "doc "+strconv.FormatUint(id, 10))
	}
	if more := len(ids) - shown; more > 0 {
		names = append(names, strconv.Itoa(more)+" more")
	}
	return strings.Join(names, ", ")
}

// writeSynced writes a file whole next to its place, syncs it and gives it
// its name.
func writeSynced(path string, raw []byte) error {
	file, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

// syncDir syncs a directory, so that what was named or removed in it stays
// so after a crash.
func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}
