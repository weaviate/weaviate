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
#include <arm_neon.h>

// hamming only works with length >= 16

void hamming_bitwise(unsigned long long *a, unsigned long long *b,
                     unsigned long long *res, long *len) {
  int size = *len;

  // use the vectorized version for the first n - (n % 4) elements
  int l = size - (size % 4);

  // create 4*4 registers to store the result
  uint32x4_t res_vec0 = vdupq_n_u32(0);
  uint32x4_t res_vec1 = vdupq_n_u32(0);
  uint32x4_t res_vec2 = vdupq_n_u32(0);
  uint32x4_t res_vec3 = vdupq_n_u32(0);

  int i = 0;

  // load 2*4 uint64s at a time
  while (i + 8 <= l) {
    uint64x2x4_t a4 = vld1q_u64_x4(a + i);
    uint64x2x4_t b4 = vld1q_u64_x4(b + i);

    res_vec0 += vpaddlq_u16(vpaddlq_u8(
        vcntq_u8(vreinterpretq_u8_u64(veorq_u64(a4.val[0], b4.val[0])))));

    res_vec1 += vpaddlq_u16(vpaddlq_u8(
        vcntq_u8(vreinterpretq_u8_u64(veorq_u64(a4.val[1], b4.val[1])))));

    res_vec2 += vpaddlq_u16(vpaddlq_u8(
        vcntq_u8(vreinterpretq_u8_u64(veorq_u64(a4.val[2], b4.val[2])))));

    res_vec3 += vpaddlq_u16(vpaddlq_u8(
        vcntq_u8(vreinterpretq_u8_u64(veorq_u64(a4.val[3], b4.val[3])))));

    i += 8;
  }

  while (i < l) {
    uint64x2_t a4 = vld1q_u64(a + i);
    uint64x2_t b4 = vld1q_u64(b + i);

    res_vec0 += vpaddlq_u16(
        vpaddlq_u8(vcntq_u8(vreinterpretq_u8_u64(veorq_u64(a4, b4)))));

    i += 2;
  }

  // convert to f32 implicitly
  int32_t sum = 0;
  sum += vaddvq_u32(res_vec0);
  sum += vaddvq_u32(res_vec1);
  sum += vaddvq_u32(res_vec2);
  sum += vaddvq_u32(res_vec3);

  // add the remaining vectors
  for (int i = l; i < size; i++) {
    uint64_t xor_result = a[i] ^ b[i];
    sum += __builtin_popcountll(xor_result);
  }

  res[0] = sum;
}