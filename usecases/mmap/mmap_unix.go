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

//go:build darwin || linux

package mmap

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

type MMap []byte

func MapRegion(f *os.File, length int, prot, flags int, offset int64) (MMap, error) {
	if offset%int64(os.Getpagesize()) != 0 {
		return nil, fmt.Errorf("offset parameter must be a multiple of the system's page size")
	}

	var fd uintptr
	if flags&ANON == 0 {
		fd = uintptr(f.Fd())
		if length < 0 {
			fi, err := f.Stat()
			if err != nil {
				return nil, err
			}
			length = int(fi.Size())
		}
	} else {
		if length <= 0 {
			return nil, fmt.Errorf("anonymous mapping requires non-zero length")
		}
		fd = ^uintptr(0)
	}

	return mmap(length, uintptr(prot), uintptr(flags), fd, offset)
}

func mmap(len int, inprot, inflags, fd uintptr, off int64) ([]byte, error) {
	flags := unix.MAP_SHARED
	prot := unix.PROT_READ
	switch {
	case inprot&COPY != 0:
		prot |= unix.PROT_WRITE
		flags = unix.MAP_PRIVATE
	case inprot&RDWR != 0:
		prot |= unix.PROT_WRITE
	}
	if inprot&EXEC != 0 {
		prot |= unix.PROT_EXEC
	}
	if inflags&ANON != 0 {
		flags |= unix.MAP_ANON
	}

	b, err := unixMmap(int(fd), off, len, prot, flags)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func unixMmap(fd int, offset int64, length int, prot int, flags int) ([]byte, error) {
	addr, _, errno := syscall.Syscall6(syscall.SYS_MMAP, 0, uintptr(length), uintptr(prot), uintptr(flags), uintptr(fd), uintptr(offset))
	if errno != 0 {
		return nil, errno
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(addr)), length), nil
}

func (m *MMap) Unmap() error {
	data := *m
	if len(data) == 0 {
		return nil
	}
	addr := uintptr(unsafe.Pointer(&data[0]))
	_, _, errno := syscall.Syscall(syscall.SYS_MUNMAP, addr, uintptr(len(data)), 0)
	if errno != 0 {
		return errno
	}
	*m = nil
	return nil
}
