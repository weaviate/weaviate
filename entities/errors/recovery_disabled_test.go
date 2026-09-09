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

// recoveryChildEnv names the callback the child process should panic in. A test
// that asserts a panic reaches the runtime cannot recover it and report, because
// the report dies with the process, so the assertion runs in the parent.
const recoveryChildEnv = "WEAVIATE_TEST_PANIC_IN"

// survived is what the child prints once the callback returned, which it only
// does where the recovery swallowed the panic. Exiting zero proves nothing on
// its own: a child that matched no test does that too.
const survived = "the recovery swallowed it"

// TestRecoveryDisabled guards DISABLE_RECOVERY_ON_PANIC, the setting a developer
// exports to debug a panic. Nothing else pins it: removing the whole branch that
// reads it leaves this package green in every other test.
func TestRecoveryDisabled(t *testing.T) {
	if in := os.Getenv(recoveryChildEnv); in != "" {
		panicIn(in)
		return
	}

	// entcfg.Enabled's string parsing is tested elsewhere; this only pins that
	// each recovery site reads the setting at all.
	const enabled = "true"
	// every callback that recovers, so a site that stops consulting the setting
	// fails on that site rather than on a value
	callbacks := []string{"Go", "TryGo", "RunInline", "GoWrapper", "GoWrapperWithBlock"}

	for _, callback := range callbacks {
		t.Run(callback+"/recovery off", func(t *testing.T) {
			out, err := runChild(t, callback, enabled)
			require.Error(t, err, "the panic must take the process down, got:\n%s", out)
			require.Contains(t, out, "panic:", "the process must die on an unrecovered panic")
		})
		t.Run(callback+"/recovery on", func(t *testing.T) {
			out, err := runChild(t, callback, "false")
			require.NoError(t, err, "the recovery must swallow it, got:\n%s", out)
			require.Contains(t, out, survived, "and the child must have run the callback")
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
	case "RunInline":
		_ = NewErrorGroupWrapper(logger).RunInline(raise)
	case "GoWrapper":
		// GoWrapper has no return signal, so sleep long enough for an unrecovered
		// panic to end the process first; a swallowed panic just pays the wait.
		GoWrapper(func() { _ = raise() }, logger)
		time.Sleep(10 * time.Second)
	case "GoWrapperWithBlock":
		// it blocks on the error channel, so it needs no sleep
		_ = GoWrapperWithBlock(func() { _ = raise() }, logger)
	default:
		fmt.Fprintln(os.Stderr, "unknown callback "+callback)
		os.Exit(2)
	}
	fmt.Fprintln(os.Stdout, survived)
}
