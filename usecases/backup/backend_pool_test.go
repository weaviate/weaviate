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

package backup

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

// probeTimeout bounds the waits these tests make on the probe's state, so a pool
// that stops running the work one of them waits on fails an assertion instead of
// hanging.
const probeTimeout = 5 * time.Second

// backendFullMsg is the rejection text the probe returns for a class a test fails.
const backendFullMsg = "backend is full"

// uploadProbe is the sourcer and the backend the shared-pool tests run against.
// It feeds fixed class descriptors and records what the pool does, and onWrite
// lets a test hold one chunk open while it inspects what else is running.
type uploadProbe struct {
	sourcePath string
	descs      []backup.ClassDescriptor
	// onWrite runs while the chunk write is in flight, holding the context that
	// write was given. Returning an error fails it the way a backend rejection does.
	onWrite   func(ctx context.Context, class, key string) error
	onRelease func(class string)
	// onDescriptor runs before a class is snapshotted, holding the producer's own
	// context, so a test can keep the producer open while the pool fails around it.
	onDescriptor func(ctx context.Context, class string)
	// producerDone closes when the producer stops snapshotting classes, just
	// before it closes the descriptor channel.
	producerDone chan struct{}
	// producerStopsAfter, when positive, ends the producer after that many
	// descriptors with no error descriptor, as a recovered panic in it would.
	producerStopsAfter int
	// panicOnSourcePathCall, when positive, panics on that call to SourceDataPath.
	// submitClass calls it once per class, so this panics inside the descriptor
	// loop with the classes before it already running in the pool.
	panicOnSourcePathCall int
	sourcePathCalls       atomic.Int64

	mu sync.Mutex
	// writing counts the chunk writes of each class that have not returned.
	writing  map[string]int
	written  []string
	releases []string
	// snapshotted names the classes the producer snapshotted, in order.
	snapshotted []string
	// snapshottedAfterRelease names the classes the producer snapshotted after
	// their own index had already been released.
	snapshottedAfterRelease []string
	meta                    backup.BackupDescriptor
}

func newUploadProbe(sourcePath string, descs ...backup.ClassDescriptor) *uploadProbe {
	return &uploadProbe{
		sourcePath:   sourcePath,
		descs:        descs,
		writing:      map[string]int{},
		producerDone: make(chan struct{}),
	}
}

func (p *uploadProbe) ReleaseBackup(_ context.Context, _, class string) error {
	p.mu.Lock()
	p.releases = append(p.releases, class)
	p.mu.Unlock()
	if p.onRelease != nil {
		p.onRelease(class)
	}
	return nil
}

func (p *uploadProbe) Backupable(context.Context, []string) error { return nil }

// BackupDescriptors produces one descriptor at a time from its own goroutine and
// stops between classes once ctx is cancelled, as DB.BackupDescriptors does. A
// test can then observe the pool while a later class has not been snapshotted yet.
func (p *uploadProbe) BackupDescriptors(ctx context.Context, _ string, _ []string, _ []*backup.BackupDescriptor,
) <-chan backup.ClassDescriptor {
	ch := make(chan backup.ClassDescriptor, len(p.descs))
	go func() {
		// producerDone closes before ch, so producerDone is already closed by the
		// time a consumer draining ch sees it closed.
		defer func() {
			close(p.producerDone)
			close(ch)
		}()
		for i, d := range p.descs {
			if p.producerStopsAfter > 0 && i >= p.producerStopsAfter {
				return
			}
			if err := ctx.Err(); err != nil {
				ch <- backup.ClassDescriptor{Name: d.Name, Error: err}
				return
			}
			if p.onDescriptor != nil {
				p.onDescriptor(ctx, d.Name)
			}
			p.mu.Lock()
			p.snapshotted = append(p.snapshotted, d.Name)
			if slices.Contains(p.releases, d.Name) {
				p.snapshottedAfterRelease = append(p.snapshottedAfterRelease, d.Name)
			}
			p.mu.Unlock()

			ch <- d
			if d.Error != nil {
				return
			}
		}
	}()
	return ch
}

// classesSnapshotted reports the classes the producer snapshotted, in order.
func (p *uploadProbe) classesSnapshotted() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.snapshotted...)
}

// classesSnapshottedAfterRelease reports the classes the producer snapshotted
// after their own index had already been released.
func (p *uploadProbe) classesSnapshottedAfterRelease() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.snapshottedAfterRelease...)
}

func (p *uploadProbe) Write(ctx context.Context, _, key, _, _ string, r backup.ReadCloserWithError) (n int64, err error) {
	// CloseWithError mirrors the real backends, which signal their own failure
	// to the producer
	defer func() { r.CloseWithError(err) }()
	if n, err = io.Copy(io.Discard, r); err != nil {
		return n, err
	}

	class := strings.SplitN(key, "/", 2)[0]
	p.mu.Lock()
	p.writing[class]++
	p.written = append(p.written, key)
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.writing[class]--
		p.mu.Unlock()
	}()
	if p.onWrite != nil {
		err = p.onWrite(ctx, class, key)
	}
	return n, err
}

func (p *uploadProbe) PutObject(_ context.Context, _, key, _, _ string, b []byte) error {
	if key == BackupFile {
		p.mu.Lock()
		defer p.mu.Unlock()
		return json.Unmarshal(b, &p.meta)
	}
	return nil
}

func (p *uploadProbe) SourceDataPath() string {
	if n := p.sourcePathCalls.Add(1); p.panicOnSourcePathCall > 0 && n == int64(p.panicOnSourcePathCall) {
		panic("uploadProbe: injected panic in the descriptor loop")
	}
	return p.sourcePath
}
func (p *uploadProbe) IsExternal() bool { return true }
func (p *uploadProbe) Name() string     { return "uploadProbe" }

func (p *uploadProbe) HomeDir(_, _, _ string) string { return p.sourcePath }

func (p *uploadProbe) GetObject(context.Context, string, string, string, string) ([]byte, error) {
	return nil, backup.ErrNotFound{}
}

func (p *uploadProbe) AllBackups(context.Context) ([]*backup.DistributedBackupDescriptor, error) {
	return nil, nil
}

func (p *uploadProbe) Initialize(context.Context, string, string, string) error { return nil }

func (p *uploadProbe) Read(context.Context, string, string, string, string, io.WriteCloser) (int64, error) {
	return 0, nil
}

// writingTotal reports how many chunk writes are in flight across all classes.
func (p *uploadProbe) writingTotal() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, c := range p.writing {
		n += c
	}
	return n
}

func (p *uploadProbe) snapshot() (written, releases []string, meta backup.BackupDescriptor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.written...), append([]string(nil), p.releases...), p.meta
}

// runUpload drives uploader.all over the probe's classes with a pool poolSize wide.
func runUpload(t *testing.T, ctx context.Context, p *uploadProbe, poolSize int) (*backup.BackupDescriptor, error) {
	t.Helper()
	desc, _, err := runUploadWithStat(t, ctx, p, poolSize)
	return desc, err
}

// runUploadWithStat is runUpload plus the status slot the upload published to,
// which is what a poll for the node reads.
func runUploadWithStat(t *testing.T, ctx context.Context, p *uploadProbe, poolSize int) (*backup.BackupDescriptor, *backupStat, error) {
	t.Helper()
	u, desc, names, stat := newProbeUploader(t, p, poolSize)
	return desc, stat, u.all(ctx, names, desc, nil, "", "")
}

// newProbeUploader builds the uploader the pool tests drive, along with the
// arguments and the status slot of the upload. A test whose call to all does not
// return normally still needs to read the slot, which is what a poll for the
// node reads. The slot is therefore handed out before the call, not after it.
func newProbeUploader(t *testing.T, p *uploadProbe, poolSize int) (*uploader, *backup.BackupDescriptor, []string, *backupStat) {
	t.Helper()
	names := make([]string, len(p.descs))
	for i, d := range p.descs {
		names[i] = d.Name
	}
	logger, _ := test.NewNullLogger()
	store := nodeStore{objectStore{backend: p, backupId: "backup-1"}}
	stat := &backupStat{}
	u := newUploader(config.Backup{}, p, nil, nil, nil, nil, store, "backup-1", stat, logger).
		withCompression(zipConfig{Level: int(NoCompression), GoPoolSize: poolSize})

	desc := &backup.BackupDescriptor{ID: "backup-1", Classes: make([]backup.ClassDescriptor, 0, len(names))}
	return u, desc, names, stat
}

// classDescriptorWithShards returns desc carrying n copies of its shard, so one
// class can put several shard jobs into the pool at once.
func classDescriptorWithShards(desc backup.ClassDescriptor, n int) backup.ClassDescriptor {
	shard := *desc.Shards[0]
	desc.Shards = make([]*backup.ShardDescriptor, n)
	for i := range desc.Shards {
		s := shard
		s.Name = shard.Name + "-" + string(rune('a'+i))
		desc.Shards[i] = &s
	}
	return desc
}

// TestUploaderAllUploadsClassesConcurrently pins the two properties one pool for
// the whole backup buys. Every class can be in flight at once even though each
// holds a single shard. No class is released while it is still being read.
func TestUploaderAllUploadsClassesConcurrently(t *testing.T) {
	classes := []string{"Class-A", "Class-B", "Class-C", "Class-D"}
	sourcePath := t.TempDir()
	p := newUploadProbe(sourcePath, genClassDescriptions(t, sourcePath, classes...)...)

	gate := make(chan struct{})
	var openGate sync.Once
	open := func() { openGate.Do(func() { close(gate) }) }
	p.onWrite = func(context.Context, string, string) error {
		<-gate
		return nil
	}

	var (
		desc     *backup.BackupDescriptor
		err      error
		finished = make(chan struct{})
	)
	go func() {
		defer close(finished)
		desc, err = runUpload(t, context.Background(), p, len(classes))
	}()
	t.Cleanup(func() {
		open()
		<-finished
	})

	require.Eventually(t, func() bool { return p.writingTotal() == len(classes) },
		probeTimeout, time.Millisecond,
		"every class should have a chunk in flight at once")

	// Every class is mid-upload for the whole window, and a release would delete
	// the files those uploads are still reading.
	assert.Never(t, func() bool {
		_, releases, _ := p.snapshot()
		return len(releases) > 0
	}, 300*time.Millisecond, 5*time.Millisecond,
		"no class may be released while its chunks are still being written")

	open()
	select {
	case <-finished:
	case <-time.After(probeTimeout):
		t.Fatal("upload did not finish after the gate opened")
	}
	require.NoError(t, err)
	assert.Equal(t, backup.Success, desc.Status)
}

// TestUploaderAllKeepsRequestOrder pins that the descriptor lists classes in the
// order they were requested even when a later class finishes first.
func TestUploaderAllKeepsRequestOrder(t *testing.T) {
	classes := []string{"Class-A", "Class-B"}
	sourcePath := t.TempDir()
	p := newUploadProbe(sourcePath, genClassDescriptions(t, sourcePath, classes...)...)

	bReleased := make(chan struct{})
	var once sync.Once
	p.onRelease = func(class string) {
		if class == "Class-B" {
			once.Do(func() { close(bReleased) })
		}
	}
	p.onWrite = func(_ context.Context, class, _ string) error {
		if class != "Class-A" {
			return nil
		}
		select {
		case <-bReleased:
		case <-time.After(probeTimeout):
			return errors.New("Class-B never finished while Class-A waited")
		}
		return nil
	}

	desc, err := runUpload(t, context.Background(), p, len(classes))
	require.NoError(t, err)

	got := make([]string, len(desc.Classes))
	for i, c := range desc.Classes {
		got[i] = c.Name
	}
	assert.Equal(t, classes, got)
}

func TestUploaderAllClassDescriptors(t *testing.T) {
	tests := []struct {
		name     string
		shards   []int // shards per class, one entry per class
		poolSize int
	}{
		{name: "one shard per class", shards: []int{1, 1, 1}, poolSize: 4},
		{name: "several shards of one class share the pool", shards: []int{4}, poolSize: 4},
		{name: "pool narrower than the work", shards: []int{3, 3}, poolSize: 1},
		{name: "no classes at all", shards: nil, poolSize: 2},
		{name: "class without shards is still recorded", shards: []int{0, 1}, poolSize: 2},
		{name: "unset pool size still runs one worker", shards: []int{2}, poolSize: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			names := make([]string, len(tc.shards))
			for i := range tc.shards {
				names[i] = "Class-" + string(rune('A'+i))
			}
			sourcePath := t.TempDir()
			descs := genClassDescriptions(t, sourcePath, names...)
			for i, n := range tc.shards {
				descs[i] = classDescriptorWithShards(descs[i], n)
			}
			p := newUploadProbe(sourcePath, descs...)

			desc, err := runUpload(t, context.Background(), p, tc.poolSize)
			require.NoError(t, err)

			written, _, meta := p.snapshot()
			require.Len(t, desc.Classes, len(names))

			var total int64
			for i, c := range desc.Classes {
				assert.Equal(t, names[i], c.Name)
				// How many chunks a shard becomes belongs to the zip layer. What the
				// pool owns is that every chunk it recorded was written and that no
				// shard's chunks were lost against a concurrent sibling.
				covered := map[string]bool{}
				for chunk, shards := range c.Chunks {
					assert.Contains(t, written, chunkKey(c.Name, chunk))
					require.Len(t, shards, 1)
					covered[shards[0]] = true
				}
				for _, shard := range c.Shards {
					assert.True(t, covered[shard.Name], "no chunk recorded for shard %s of %s", shard.Name, c.Name)
				}
				total += c.PreCompressionSizeBytes
			}
			assert.Equal(t, total, desc.PreCompressionSizeBytes)
			assert.Len(t, written, countChunks(desc.Classes))
			assert.Equal(t, backup.Success, meta.Status)

			// Releasing is fire-and-forget, so the set is only complete eventually.
			assert.Eventually(t, func() bool {
				_, releases, _ := p.snapshot()
				return len(dedupe(releases)) == len(names)
			}, probeTimeout, time.Millisecond, "every class must be released")
		})
	}
}

func countChunks(classes []backup.ClassDescriptor) int {
	n := 0
	for _, c := range classes {
		n += len(c.Chunks)
	}
	return n
}

func dedupe(xs []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, x := range xs {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	return out
}

func TestUploaderAllFailures(t *testing.T) {
	const failing = "Class-B"
	tests := []struct {
		name      string
		descError error
		// failWrites rejects every write of the failing class. failOnPoolCancel
		// instead waits for the descriptor error to cancel the pool and only then
		// rejects, so the shards fail with the operation still uncancelled.
		failWrites       bool
		failOnPoolCancel bool
		cancel           bool
		wantErr          string
		wantStatus       backup.Status
	}{
		{
			name:       "backend rejects a chunk write",
			failWrites: true,
			wantErr:    backendFullMsg,
			wantStatus: backup.Failed,
		},
		{
			name:       "descriptor arrives with an error",
			descError:  errors.New("class vanished"),
			wantErr:    "class vanished",
			wantStatus: backup.Failed,
		},
		{
			name:             "shards cancelled by a descriptor error still report a failure",
			descError:        errors.New("class vanished"),
			failOnPoolCancel: true,
			wantErr:          "class vanished",
			wantStatus:       backup.Failed,
		},
		{
			name:       "operation is cancelled while uploading",
			cancel:     true,
			wantErr:    context.Canceled.Error(),
			wantStatus: backup.Cancelled,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			names := []string{"Class-A", failing}
			sourcePath := t.TempDir()
			descs := genClassDescriptions(t, sourcePath, names...)
			for i := range descs {
				descs[i] = classDescriptorWithShards(descs[i], 2)
			}
			if tc.descError != nil {
				descs[1] = backup.ClassDescriptor{Name: failing, Error: tc.descError}
			}
			p := newUploadProbe(sourcePath, descs...)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancel {
				var once sync.Once
				p.onWrite = func(context.Context, string, string) error {
					once.Do(cancel)
					return nil
				}
			}
			if tc.failWrites {
				p.onWrite = func(_ context.Context, class, _ string) error {
					if class == failing {
						return errors.New(backendFullMsg)
					}
					return nil
				}
			}
			if tc.failOnPoolCancel {
				p.onWrite = func(writeCtx context.Context, _, _ string) error {
					select {
					case <-writeCtx.Done():
						return errors.New(backendFullMsg)
					case <-time.After(probeTimeout):
						return errors.New("the pool was never cancelled")
					}
				}
			}

			desc, err := runUpload(t, ctx, p, 4)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.Equal(t, tc.wantStatus, desc.Status)
			assert.NotContains(t, desc.Error, "%!w(",
				"a step of the upload that succeeded must not be reported as an error")

			for _, c := range desc.Classes {
				assert.NotEqual(t, failing, c.Name, "a class that did not finish must not be in the descriptor")
			}

			// Releasing is fire-and-forget, so the set is only complete eventually.
			assert.Eventually(t, func() bool {
				_, releases, _ := p.snapshot()
				return len(dedupe(releases)) == len(names)
			}, probeTimeout, time.Millisecond, "every class must be released even when the backup fails")
		})
	}
}

// TestUploaderAllWaitsForDescriptorProducer pins that all does not release an
// index while the descriptor producer can still snapshot it. The producer runs
// on the operation's context, which cancelling the pool never reaches.
func TestUploaderAllWaitsForDescriptorProducer(t *testing.T) {
	const failing = "Class-A"
	names := []string{failing, "Class-B", "Class-C"}
	last := names[len(names)-1]
	sourcePath := t.TempDir()
	p := newUploadProbe(sourcePath, genClassDescriptions(t, sourcePath, names...)...)

	failed := make(chan struct{})
	var once sync.Once
	p.onWrite = func(_ context.Context, class, _ string) error {
		if class != failing {
			return nil
		}
		once.Do(func() { close(failed) })
		return errors.New(backendFullMsg)
	}
	releasedLast := make(chan struct{})
	var relOnce sync.Once
	p.onRelease = func(class string) {
		if class == last {
			relOnce.Do(func() { close(releasedLast) })
		}
	}
	// onDescriptor holds the last class's snapshot open until all stops the
	// producer. Whichever of the two arms below wins is the assertion. A release
	// reaching the held class first is the ordering this test exists to catch.
	p.onDescriptor = func(ctx context.Context, class string) {
		if class != last {
			return
		}
		select {
		case <-failed:
		case <-time.After(probeTimeout):
			t.Error("the failing class never failed the pool")
			return
		}
		select {
		case <-releasedLast:
			t.Error("the last class's index was released while the producer could still snapshot it")
		case <-ctx.Done():
		}
	}

	desc, err := runUpload(t, context.Background(), p, 4)
	require.Error(t, err)
	assert.Equal(t, backup.Failed, desc.Status)

	select {
	case <-p.producerDone:
	default:
		t.Error("all returned while the descriptor producer was still snapshotting classes")
	}

	select {
	case <-p.producerDone:
	case <-time.After(probeTimeout):
		t.Fatal("the descriptor producer never finished")
	}
	assert.Empty(t, p.classesSnapshottedAfterRelease(),
		"a class snapshotted after its index was released stays marked in progress")
}

// TestUploaderAllStopsDescriptorProducerOnFailure pins that the drain waits only
// for the class already being snapshotted. all owns the producer's context, so a
// failed backup does not wait out a snapshot of every class it never reached.
func TestUploaderAllStopsDescriptorProducerOnFailure(t *testing.T) {
	const failing, gated = "Class-A", "Class-C"
	names := []string{failing, "Class-B", gated, "Class-D"}
	sourcePath := t.TempDir()
	p := newUploadProbe(sourcePath, genClassDescriptions(t, sourcePath, names...)...)

	p.onWrite = func(_ context.Context, class, _ string) error {
		if class != failing {
			return nil
		}
		return errors.New(backendFullMsg)
	}
	// Holding the gated class until all stops the producer puts one class in flight
	// at exactly the moment the drain starts, which is the wait the drain bounds.
	p.onDescriptor = func(ctx context.Context, class string) {
		if class != gated {
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(probeTimeout):
			t.Error("all never stopped the descriptor producer")
		}
	}

	desc, err := runUpload(t, context.Background(), p, 4)
	require.Error(t, err)
	assert.Equal(t, backup.Failed, desc.Status)

	select {
	case <-p.producerDone:
	case <-time.After(probeTimeout):
		t.Fatal("the descriptor producer never finished")
	}
	assert.Equal(t, []string{failing, "Class-B", gated}, p.classesSnapshotted(),
		"the class after the one in flight must never be snapshotted")
	assert.Empty(t, p.classesSnapshottedAfterRelease(),
		"a class snapshotted after its index was released stays marked in progress")

	// The gated class was snapshotted and its descriptor then thrown away by the
	// drain, which is safe only because the release still covers it.
	assert.Eventually(t, func() bool {
		_, releases, _ := p.snapshot()
		return len(dedupe(releases)) == len(names)
	}, probeTimeout, time.Millisecond, "every class must be released, described or not")
}

// TestUploaderAllRejectsPartialDescriptorRun pins that a producer ending without
// saying why fails the backup. A recovered panic in it closes the channel with no
// error descriptor. That otherwise reads as "every class was described" and
// publishes a SUCCESS that omits the classes it never reached.
func TestUploaderAllRejectsPartialDescriptorRun(t *testing.T) {
	names := []string{"Class-A", "Class-B", "Class-C"}
	sourcePath := t.TempDir()
	p := newUploadProbe(sourcePath, genClassDescriptions(t, sourcePath, names...)...)
	p.producerStopsAfter = 1

	desc, err := runUpload(t, context.Background(), p, 4)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "describes 1 of 3 classes")
	assert.NotEqual(t, backup.Success, desc.Status)

	_, _, meta := p.snapshot()
	assert.NotEqual(t, backup.Success, meta.Status,
		"a backup missing classes must not be published as successful")
}

// TestUploaderAllCancelsThePoolBeforeDrainingIt pins the order the pool defers
// run in when the descriptor loop does not reach its own wait. Draining shard
// jobs that nothing has cancelled gives each of them the full storeTimeout, so
// the backup goroutine parks for a day on a backup that is already over.
func TestUploaderAllCancelsThePoolBeforeDrainingIt(t *testing.T) {
	classes := []string{"Class-A", "Class-B"}
	sourcePath := t.TempDir()
	p := newUploadProbe(sourcePath, genClassDescriptions(t, sourcePath, classes...)...)
	// Class-A is submitted and its shard reaches the backend. Class-B panics on
	// its way into the pool, leaving Class-A in flight.
	p.panicOnSourcePathCall = 2
	p.onWrite = func(ctx context.Context, _, _ string) error {
		<-ctx.Done()
		return ctx.Err()
	}

	// the package shadows the any type with a mock.Anything alias, so the
	// recovered value is reduced to whether there was one
	var (
		panicked bool
		finished = make(chan struct{})
	)
	u, desc, names, _ := newProbeUploader(t, p, len(classes))
	go func() {
		defer close(finished)
		defer func() { panicked = recover() != nil }()
		_ = u.all(context.Background(), names, desc, nil, "", "")
	}()

	select {
	case <-finished:
	case <-time.After(probeTimeout):
		t.Fatal("all never returned: it drained the pool without cancelling it first")
	}
	require.True(t, panicked, "the panic must still propagate out of all")
}

// TestUploaderAllDoesNotPublishSuccessOnPanic pins that a backup that stopped
// part way through is not published as finished. A node reporting success is a
// node the coordinator counts as done. A panic in the descriptor loop leaves
// the named error nil. One in a shard job arrives as the pool's error, which
// has to name the crash rather than the cancellation its siblings return.
func TestUploaderAllDoesNotPublishSuccessOnPanic(t *testing.T) {
	classes := []string{"Class-A", "Class-B"}
	tests := []struct {
		name string
		// panicIn arranges for the named goroutine to panic.
		panicIn func(p *uploadProbe)
		// poolSize 1 leaves the other shard jobs queued, so they start after the
		// panic and return the cancellation.
		poolSize int
		// wantErrContains also names what the injector rests on, so a guard added
		// against it makes the case fail rather than pass for another reason.
		wantErrContains []string
	}{
		{
			name:     "descriptor loop",
			panicIn:  func(p *uploadProbe) { p.panicOnSourcePathCall = 2 },
			poolSize: len(classes),
		},
		{
			name: "shard job",
			panicIn: func(p *uploadProbe) {
				// a nil descriptor panics in createFileList, which runs before compress
				// installs its own recovery around the consumer goroutine
				p.descs[0] = classDescriptorWithShards(p.descs[0], 2)
				p.descs[0].Shards[0] = nil
			},
			poolSize:        1,
			wantErrContains: []string{"panic occurred", "nil pointer"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourcePath := t.TempDir()
			p := newUploadProbe(sourcePath, genClassDescriptions(t, sourcePath, classes...)...)
			tt.panicIn(p)

			finished := make(chan struct{})
			u, desc, names, stat := newProbeUploader(t, p, tt.poolSize)
			go func() {
				defer close(finished)
				defer func() { _ = recover() }()
				_ = u.all(context.Background(), names, desc, nil, "", "")
			}()

			select {
			case <-finished:
			case <-time.After(probeTimeout):
				t.Fatal("all never returned")
			}

			got := stat.get()
			assert.Equal(t, backup.Failed, got.Status,
				"a backup that panicked part way through must not be published as successful")
			assert.NotEmpty(t, got.Err, "the failure has to carry a reason a poll can read")
			for _, want := range tt.wantErrContains {
				assert.Contains(t, got.Err, want)
			}

			_, _, meta := p.snapshot()
			assert.NotEqual(t, backup.Success, meta.Status)
		})
	}
}
