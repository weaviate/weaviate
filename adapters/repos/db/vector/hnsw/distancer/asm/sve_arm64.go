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

package asm

// SVEVectorLengthBytes returns the SVE vector length of the host in bytes
// (16 on Neoverse V2, 32 on Neoverse V1), read with RDVL. It executes an SVE
// instruction, so it must only be called when cpu.ARM64.HasSVE is true.
func SVEVectorLengthBytes() int
