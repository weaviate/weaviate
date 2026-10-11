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

package errors

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// recoveryChildEnv names the callback the child process should panic in. The
// assertion runs in the parent, because a report would die with the child.
const recoveryChildEnv = "WEAVIATE_TEST_PANIC_IN"

// TestRecoveryDisabled is the only thing pinning DISABLE_RECOVERY_ON_PANIC:
// delete the branch that reads it and every other test stays green. Recovery
// being on is asserted in process; only its absence needs a child.
func TestRecoveryDisabled(t *testing.T) {
	if in := os.Getenv(recoveryChildEnv); in != "" {
		panicIn(in)
		return
	}

	// entcfg.Enabled's parsing is covered by entities/config.TestEnabled
	const enabled = "true"
	// one entry per way a recovery is reached, so a path that stops reading the
	// setting fails on that path. GoWrapperWithBlock reaches GoWrapperWithErrorCh's
	// guard; the package-level RunRecovered is covered in process by TestRunRecovered.
	callbacks := []string{"Go", "TryGo", "RunRecovered", "GoWrapper", "GoWrapperWithBlock"}

	for _, callback := range callbacks {
		t.Run(callback, func(t *testing.T) {
			out, err := runChild(t, callback, enabled)
			require.Error(t, err, "the panic must take the process down, got:\n%s", out)
			require.Contains(t, out, "panic:", "the process must die on an unrecovered panic")
		})
	}
}

func runChild(t *testing.T, callback, value string) (string, error) {
	t.Helper()
	// an empty name would leave the child reading itself as the parent
	require.NotEmpty(t, callback)

	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestRecoveryDisabled$", "-test.v")
	cmd.Env = append(os.Environ(),
		recoveryChildEnv+"="+callback,
		"DISABLE_RECOVERY_ON_PANIC="+value,
	)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// panicIn raises a panic through the named callback.
func panicIn(callback string) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	raise := func() error { panic("recovery probe") }

	switch callback {
	case "Go":
		eg := NewErrorGroupWrapper(logger)
		eg.Go(raise)
		_ = eg.Wait()
	case "TryGo":
		eg := NewErrorGroupWrapper(logger)
		eg.TryGo(raise)
		_ = eg.Wait()
	case "RunRecovered":
		_ = NewErrorGroupWrapper(logger).RunRecovered(raise)
	case "GoWrapper":
		// GoWrapper has no return signal. Without the wait, panicIn returns and
		// the child exits zero, which reads as a recovery that never happened.
		entered := make(chan struct{})
		GoWrapper(func() { close(entered); _ = raise() }, logger)
		<-entered
		time.Sleep(500 * time.Millisecond)
	case "GoWrapperWithBlock":
		// it blocks on the error channel, so it needs no sleep
		_ = GoWrapperWithBlock(func() { _ = raise() }, logger)
	default:
		fmt.Fprintln(os.Stderr, "unknown callback "+callback)
		os.Exit(2)
	}
}
