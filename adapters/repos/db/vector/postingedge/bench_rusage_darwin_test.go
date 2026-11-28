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

//go:build postingedgebench && darwin

package postingedge

import (
	"encoding/binary"
	"os"
	"runtime"
	"syscall"
	"time"
	"unsafe"

	"github.com/pkg/errors"
)

// processCPU returns the accounting of the CPU of this process so far
// (proc_pid_rusage, with RUSAGE_INFO_V6), with no privilege: the times on the
// CPU, all of them and on the performance cores, the time runnable (running
// or waiting for a core), the time by QoS class, the instructions and the cycles,
// all of them and on the performance cores, and the page-ins.
func processCPU() (processCPUTimes, error) {
	const (
		procInfoCallPidRusage = 9
		rusageInfoV6          = 6
	)
	var buf [512]byte
	_, _, errno := syscall.Syscall6(syscall.SYS_PROC_INFO, procInfoCallPidRusage, uintptr(os.Getpid()), rusageInfoV6, 0,
		uintptr(unsafe.Pointer(&buf[0])), 0)
	if errno != 0 {
		return processCPUTimes{}, errors.Wrap(errno, "read the resource usage of the process")
	}
	// word is the uint64 of index i of rusage_info_v6, after its uuid of 16
	// bytes; ns one of its times, in units of mach_absolute_time.
	word := func(i int) uint64 { return binary.LittleEndian.Uint64(buf[16+i*8:]) }
	numer, denom := machTimebase()
	ns := func(i int) uint64 { return word(i) * numer / denom }
	c := processCPUTimes{
		UserNs: ns(0), SystemNs: ns(1), Pageins: word(4), DiskReadBytes: word(16),
		Instructions: word(29), Cycles: word(30), RunnableNs: ns(34),
		UserPNs: ns(36), SystemPNs: ns(37), PInstructions: word(38), PCycles: word(39),
	}
	// ri_cpu_time_qos_default to ri_cpu_time_qos_user_interactive.
	for q := range c.QoSNs {
		c.QoSNs[q] = ns(18 + q)
	}
	return c, nil
}

// machTimebase is mach_timebase_info, which Go does not export: numer/denom
// nanoseconds a unit of mach_absolute_time, 125/3 on Apple silicon (a 24 MHz
// counter) and 1/1 on Intel. TestProcessCPU checks it against getrusage.
func machTimebase() (numer, denom uint64) {
	if runtime.GOARCH == "arm64" {
		return 125, 3
	}
	return 1, 1
}

// processCPUTimeOfGetrusage is the time on the CPU of this process as
// getrusage tells it, in microseconds: the check of the units of processCPU.
func processCPUTimeOfGetrusage() (time.Duration, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, errors.Wrap(err, "getrusage")
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano()), nil
}

// processDiskReads returns what the system has read from disk for this
// process so far: the page-ins, which are the faults of its mappings that
// went to disk, and the bytes read, with whatever the system read ahead. It
// is the accounting the system keeps of every process (proc_pid_rusage, with
// RUSAGE_INFO_V2), and needs no privilege.
func processDiskReads() ([growCounters]uint64, error) {
	const (
		procInfoCallPidRusage = 9
		rusageInfoV2          = 2
		// Where ri_pageins and ri_diskio_bytesread are in rusage_info_v2,
		// after its uuid of 16 bytes: its fifth and its seventeenth uint64.
		pageinsAt, bytesReadAt = 16 + 4*8, 16 + 16*8
	)
	var buf [256]byte
	_, _, errno := syscall.Syscall6(syscall.SYS_PROC_INFO, procInfoCallPidRusage, uintptr(os.Getpid()), rusageInfoV2, 0,
		uintptr(unsafe.Pointer(&buf[0])), 0)
	if errno != 0 {
		return [growCounters]uint64{}, errors.Wrap(errno, "read the resource usage of the process")
	}
	return [growCounters]uint64{binary.LittleEndian.Uint64(buf[pageinsAt:]), binary.LittleEndian.Uint64(buf[bytesReadAt:])}, nil
}
