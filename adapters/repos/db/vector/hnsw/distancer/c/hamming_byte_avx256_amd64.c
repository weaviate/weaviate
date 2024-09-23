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

void hamming_byte_256(unsigned char *a, unsigned char *b, unsigned int *res,
                      long *len) {
  int n = *len;

  // fast path for small dimensions
  if (n < 32) {
    int acc = 0;
    for (int i = 0; i < n; i++) {
      acc += (a[i] != b[i]);
    }

    *res = (unsigned int)acc;
    return;
  }

  //   __m256i acc = _mm256_setzero_si256();

  int i;
  // Create 4 registers to store the results
  // __m256i acc = _mm256_setzero_si256();
  int acc = 0;

  __m256i zeros_256 = _mm256_setzero_si256();

  __m256i blend1_256 = _mm256_setzero_si256();
  __m256i blend2_256 = _mm256_setzero_si256();
  __m256i blend3_256 = _mm256_setzero_si256();
  __m256i blend4_256 = _mm256_setzero_si256();

  __mmask32 cmp_result_1;
  __mmask32 cmp_result_2;
  __mmask32 cmp_result_3;
  __mmask32 cmp_result_4;

  __m256i cmp_result_i_1 = _mm256_setzero_si256();
  __m256i cmp_result_i_2 = _mm256_setzero_si256();
  __m256i cmp_result_i_3 = _mm256_setzero_si256();
  __m256i cmp_result_i_4 = _mm256_setzero_si256();

  __m256i ones_256 = _mm256_set1_epi32(1);

  while (n >= 128) {
    // // Unroll loop for 128 bytes
    // __m256 a_vec0 = _mm256_loadu_epi8(a);
    // __m256 a_vec1 = _mm256_loadu_epi8(a + 32);
    // __m256 a_vec2 = _mm256_loadu_epi8(a + 64);
    // __m256 a_vec3 = _mm256_loadu_epi8(a + 96);

    // __m256 b_vec0 = _mm256_loadu_epi8(b);
    // __m256 b_vec1 = _mm256_loadu_epi8(b + 32);
    // __m256 b_vec2 = _mm256_loadu_epi8(b + 64);
    // __m256 b_vec3 = _mm256_loadu_epi8(b + 96);

    // cmp_result_1 = _mm256_cmp_epi8_mask(a_vec0, b_vec0, _MM_CMPINT_NE);
    // cmp_result_2 = _mm256_cmp_epi8_mask(a_vec1, b_vec1, _MM_CMPINT_NE);
    // cmp_result_3 = _mm256_cmp_epi8_mask(a_vec2, b_vec2, _MM_CMPINT_NE);
    // cmp_result_4 = _mm256_cmp_epi8_mask(a_vec3, b_vec3, _MM_CMPINT_NE);

    // acc = _mm256_add_epi32(
    //     acc, _mm256_popcnt_epi32(_mm256_set1_epi32(cmp_result_1)));
    // acc = _mm256_add_epi32(
    //     acc, _mm256_popcnt_epi32(_mm256_set1_epi32(cmp_result_2)));
    // acc = _mm256_add_epi32(
    //     acc, _mm256_popcnt_epi32(_mm256_set1_epi32(cmp_result_3)));
    // acc = _mm256_add_epi32(
    //     acc, _mm256_popcnt_epi32(_mm256_set1_epi32(cmp_result_4)));

    __m256i a_vec0 = _mm256_loadu_si256((__m256i *)(a));
    __m256i a_vec1 = _mm256_loadu_si256((__m256i *)(a + 32));
    __m256i a_vec2 = _mm256_loadu_si256((__m256i *)(a + 64));
    __m256i a_vec3 = _mm256_loadu_si256((__m256i *)(a + 96));

    __m256i b_vec0 = _mm256_loadu_si256((__m256i *)(b));
    __m256i b_vec1 = _mm256_loadu_si256((__m256i *)(b + 32));
    __m256i b_vec2 = _mm256_loadu_si256((__m256i *)(b + 64));
    __m256i b_vec3 = _mm256_loadu_si256((__m256i *)(b + 96));

    __m256i cmp0 = _mm256_cmpeq_epi8(a_vec0, b_vec0);
    __m256i cmp1 = _mm256_cmpeq_epi8(a_vec1, b_vec1);
    __m256i cmp2 = _mm256_cmpeq_epi8(a_vec2, b_vec2);
    __m256i cmp3 = _mm256_cmpeq_epi8(a_vec3, b_vec3);

    int mask0 = _mm256_movemask_epi8(cmp0);
    int mask1 = _mm256_movemask_epi8(cmp1);
    int mask2 = _mm256_movemask_epi8(cmp2);
    int mask3 = _mm256_movemask_epi8(cmp3);

    int equal_count = __builtin_popcount(mask0) + __builtin_popcount(mask1) +
                      __builtin_popcount(mask2) + __builtin_popcount(mask3);

    // 128 is the total number of bytes we compared
    int diff_count = 128 - equal_count;

    acc += diff_count;

    n -= 128;
    a += 128;
    b += 128;
  }

  // Process 32 bytes at a time
  while (n >= 32) {

    __m256i a_vec0 = _mm256_loadu_si256((__m256i *)(a));
    __m256i b_vec0 = _mm256_loadu_si256((__m256i *)(b));

    __m256i cmp = _mm256_cmpeq_epi8(a_vec0, b_vec0);
    int mask = _mm256_movemask_epi8(cmp);
    int equal_count = __builtin_popcount(mask);
    acc += 32 - equal_count;

    n -= 32;
    a += 32;
    b += 32;
  }

  // // Reduce
  // __m128i acc_low = _mm256_extracti128_si256(acc, 0);
  // __m128i acc_high = _mm256_extracti128_si256(acc, 1);
  // __m128i acc128 = _mm_add_epi32(acc_low, acc_high);
  // acc128 =
  //     _mm_add_epi32(acc128, _mm_shuffle_epi32(acc128, _MM_SHUFFLE(0, 1, 2,
  //     3)));
  // acc128 =
  //     _mm_add_epi32(acc128, _mm_shuffle_epi32(acc128, _MM_SHUFFLE(0, 0, 0,
  //     1)));

  // unsigned int result = _mm_extract_epi32(acc128, 0);

  unsigned int tail_res = 0;

  // Tail
  while (n) {
    if (*a != *b) {
      acc++;
      // tail_res++;
    }
    n--;
    a++;
    b++;
  }

  *res = (unsigned int)acc;
  // *res = tail_res;
}