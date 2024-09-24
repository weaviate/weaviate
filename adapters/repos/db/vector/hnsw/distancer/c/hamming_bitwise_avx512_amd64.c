//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

#include <immintrin.h>
#include <stdint.h>

void hamming_bitwise_512(uint64_t *a, uint64_t *b, uint64_t *res, long *len) {
  int n = *len;
  uint64_t sum = 0;

  // fast path for small dimensions
  if (n < 8) {
    do {
      sum += __builtin_popcountll(a[0] ^ b[0]);
      //   sum += a[0] != b[0] ? 1 : 0;
      n--;
      a++;
      b++;
    } while (n);

    *res = sum;
    return;
  }

  // Create 4 registers to store the results
  long long acc = 0;
  long long pop = 0;

  __m256i ones_256 = _mm256_set1_epi32(1);
  __m256i zeros_256 = _mm256_setzero_si256();

  while (n >= 16) {
    // Unroll loop for 16 uint64s
    __m256i a_vec0 = _mm256_loadu_si256((__m256i const *)a);
    __m256i a_vec1 = _mm256_loadu_si256((__m256i const *)a + 4);
    __m256i a_vec2 = _mm256_loadu_si256((__m256i const *)a + 8);
    __m256i a_vec3 = _mm256_loadu_si256((__m256i const *)a + 12);

    __m256i b_vec0 = _mm256_loadu_si256((__m256i const *)b);
    __m256i b_vec1 = _mm256_loadu_si256((__m256i const *)b + 4);
    __m256i b_vec2 = _mm256_loadu_si256((__m256i const *)b + 8);
    __m256i b_vec3 = _mm256_loadu_si256((__m256i const *)b + 12);

    __m256i cmp_result_1 = _mm256_xor_si256(a_vec0, b_vec0);
    __m256i cmp_result_2 = _mm256_xor_si256(a_vec1, b_vec1);
    __m256i cmp_result_3 = _mm256_xor_si256(a_vec2, b_vec2);
    __m256i cmp_result_4 = _mm256_xor_si256(a_vec3, b_vec3);

    _mm256_popcnt_epi64(cmp_result_1);

    n -= 16;
    a += 16;
    b += 16;
  }

  // Process 8 floats at a time
  while (n >= 4) {
    // Unroll loop for 16 uint64s
    __m256 a_vec0 = _mm256_loadu_si256((__m256i const *)a);

    __m256 b_vec0 = _mm256_loadu_si256((__m256i const *)b);

    __m256i cmp_result_1 = _mm256_xor_si256(a_vec0, b_vec0);

    uint64_t *p1 = (uint64_t *)&cmp_result_1;

    acc += __builtin_popcountll(p1[0]) + __builtin_popcountll(p1[1]) +
           __builtin_popcountll(p1[2]);

    n -= 4;
    a += 4;
    b += 4;
  }

  // Tail
  while (n) {
    acc += __builtin_popcountll(a[0] ^ b[0]);
    n--;
    a++;
    b++;
  }

  sum += (uint64_t)acc + (uint64_t)pop;

  *res = sum;
  // *res = 0;
}