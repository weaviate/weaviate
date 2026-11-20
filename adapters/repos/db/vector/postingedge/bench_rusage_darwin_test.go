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
	"syscall"
	"unsafe"

	"github.com/pkg/errors"
)

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
