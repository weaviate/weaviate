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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/config"
)

// The invariant these tests establish is PER CLASS: no class may be admitted after
// a release for that class has been issued, and the terminal release happens after
// the producer has finished. It is deliberately NOT "every release happens after the
// producer finished" — the per-class release in uploader.class fires as soon as that
// class's upload ends, which is already ordered after that class's admission.

// recordingSourcer is a Sourcer whose producer is driven by explicit handshakes, so
// the test never has to sleep to know where the producer is.
type recordingSourcer struct {
	mu                   sync.Mutex
	released             map[string]bool
	admittedAfterRelease map[string]bool
	releaseSawClose      map[string]bool
	releaseSawOpen       map[string]bool

	producerClosed atomic.Bool

	// produce emits descriptors; the channel is closed for it.
	produce func(ctx context.Context, emit func(backup.ClassDescriptor))
	// onRelease runs after each release is recorded.
	onRelease func(class string)
}

func newRecordingSourcer(produce func(ctx context.Context, emit func(backup.ClassDescriptor))) *recordingSourcer {
	return &recordingSourcer{
		released:             map[string]bool{},
		admittedAfterRelease: map[string]bool{},
		releaseSawClose:      map[string]bool{},
		releaseSawOpen:       map[string]bool{},
		produce:              produce,
	}
}

func (s *recordingSourcer) Backupable(context.Context, []string) error { return nil }

func (s *recordingSourcer) BackupDescriptors(ctx context.Context, _ backup.Op, classes []string,
	_ []*backup.BackupDescriptor,
) <-chan backup.ClassDescriptor {
	ch := make(chan backup.ClassDescriptor, len(classes))
	enterrors.GoWrapper(func() {
		defer func() {
			// Set before the close so a release that observes the closed channel also
			// observes the flag.
			s.producerClosed.Store(true)
			close(ch)
		}()
		s.produce(ctx, func(desc backup.ClassDescriptor) { ch <- desc })
	}, logrus.New())
	return ch
}

func (s *recordingSourcer) ReleaseBackup(_ context.Context, _ backup.Op, class string) error {
	s.mu.Lock()
	s.released[class] = true
	if s.producerClosed.Load() {
		s.releaseSawClose[class] = true
	} else {
		s.releaseSawOpen[class] = true
	}
	s.mu.Unlock()
	if s.onRelease != nil {
		s.onRelease(class)
	}
	return nil
}

// admit records whether class was already released at the moment it is admitted.
func (s *recordingSourcer) admit(class string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released[class] {
		s.admittedAfterRelease[class] = true
	}
}

func (s *recordingSourcer) snapshot() (admittedAfterRelease, releaseSawClose, releaseSawOpen map[string]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := func(src map[string]bool) map[string]bool {
		dst := make(map[string]bool, len(src))
		for k, v := range src {
			dst[k] = v
		}
		return dst
	}
	return cp(s.admittedAfterRelease), cp(s.releaseSawClose), cp(s.releaseSawOpen)
}

func newJoinTestUploader(t *testing.T, s Sourcer) *uploader {
	t.Helper()
	fb := newFakeBackend()
	fb.On("SourceDataPath").Return(t.TempDir()).Maybe()
	fb.On("PutObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	return newUploader(config.Backup{}, s, nil, nil, nil,
		nodeStore{objectStore{fb, "join-test", "", "", "node1"}},
		"join-test", func(backup.Status) {}, logrus.New())
}

// interimReleaseGrace bounds how long the A-release handshake waits for a
// pre-fix interim release of B before letting the producer proceed. It is a
// backstop, not the synchronisation: without the fix B's release arrives on this
// channel immediately, and with the fix no release of B exists to wait for.
const interimReleaseGrace = 500 * time.Millisecond

// runCancelledTwoClassBackup drives uploader.all over classes A and B against a
// producer that emits A, waits until the test has observed A's release, then admits
// and emits B before returning (and thus closing the channel). The consume loop is
// ended by cancelling the operation context while the producer is still parked, so
// the pre-fix interim releases run against a live producer.
func runCancelledTwoClassBackup(t *testing.T) *recordingSourcer {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aReleased := make(chan struct{})
	bReleaseStarted := make(chan struct{})
	var aOnce, bOnce sync.Once

	var s *recordingSourcer
	s = newRecordingSourcer(func(_ context.Context, emit func(backup.ClassDescriptor)) {
		s.admit("A")
		emit(backup.ClassDescriptor{Name: "A"})

		select {
		case <-aReleased:
		case <-time.After(10 * time.Second):
			return
		}

		s.admit("B")
		emit(backup.ClassDescriptor{Name: "B"})
	})
	s.onRelease = func(class string) {
		if class == "B" {
			bOnce.Do(func() { close(bReleaseStarted) })
			return
		}
		if class != "A" {
			return
		}
		aOnce.Do(func() {
			// Ends the consume loop while the producer is still parked.
			cancel()
			select {
			case <-bReleaseStarted:
			case <-time.After(interimReleaseGrace):
			}
			close(aReleased)
		})
	}

	u := newJoinTestUploader(t, s)
	_ = u.all(ctx, []string{"A", "B"}, &backup.BackupDescriptor{}, nil, "", "")

	return s
}

// Unlike the terminal-release assertion below, a single read is sound here:
// admittedAfterRelease is written only by the producer, via admit(), and the join
// guarantees the producer has finished before uploader.all returns. No detached
// goroutine can add to it afterwards.
func TestNoClassIsAdmittedAfterItsRelease(t *testing.T) {
	s := runCancelledTwoClassBackup(t)

	admittedAfterRelease, _, _ := s.snapshot()
	require.Empty(t, admittedAfterRelease,
		"a class admitted after its release has no releaser left")
}

func TestTerminalReleaseHappensAfterProducerClose(t *testing.T) {
	s := runCancelledTwoClassBackup(t)

	// releaseIndexes fires one detached goroutine per class, so uploader.all
	// returning does not mean every release has been recorded yet — only that each
	// was issued after the join. The ordering under test is the product's; the lag
	// is purely in this fixture's observation of it, so poll instead of reading
	// once. Each map entry is only ever set, never cleared, so the condition is
	// monotone and cannot flap.
	require.Eventually(t, func() bool {
		_, releaseSawClose, _ := s.snapshot()
		return releaseSawClose["A"] && releaseSawClose["B"]
	}, 10*time.Second, 5*time.Millisecond,
		"every class must get at least one release after the producer finished")

	// Re-read on this goroutine rather than reusing a value captured inside the
	// poll: require.Eventually runs its condition in a separate goroutine.
	_, _, releaseSawOpen := s.snapshot()
	// The consume loop never reaches B, so B has no per-class release of its own:
	// every release it gets is terminal and must therefore follow the close. Safe to
	// read once here — releaseSawOpen is only ever set while producerClosed is false,
	// and the poll above has already observed it true.
	require.False(t, releaseSawOpen["B"],
		"a class the loop never consumed must not be released while the producer is still running")
}

// Hoisting the close into a defer makes a panicked producer look like a completed
// one, so a stream that ends before every requested class has a descriptor has to be
// an error rather than a silently short backup.
func TestUploaderFailsOnTruncatedDescriptorStream(t *testing.T) {
	s := newRecordingSourcer(func(_ context.Context, emit func(backup.ClassDescriptor)) {
		emit(backup.ClassDescriptor{Name: "A"})
	})

	u := newJoinTestUploader(t, s)
	err := u.all(context.Background(), []string{"A", "B"}, &backup.BackupDescriptor{}, nil, "", "")

	require.Error(t, err)
	require.Contains(t, err.Error(), "B", "the error must name the missing class")
}
