//go:build !noasm && arm64
// AUTO-GENERATED BY GOAT -- DO NOT EDIT

package asm

import "unsafe"

//go:noescape
func hamming_neon_byte_256(a, b, res, len unsafe.Pointer)
