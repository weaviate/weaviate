#include <immintrin.h>
#include <nmmintrin.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>

inline uint64_t popcnt_AVX2_lookup_512(__m256i *vec, __m256i *low_mask_vec,
                                       __m256i *lookup_vec) {

  size_t i = 0;

  __m256i acc = _mm256_setzero_si256();

  const __m256i lo = _mm256_and_si256(*vec, *low_mask_vec);
  const __m256i hi =
      _mm256_and_si256(_mm256_srli_epi16(*vec, 4), *low_mask_vec);
  const __m256i popcnt1 = _mm256_shuffle_epi8(*lookup_vec, lo);
  const __m256i popcnt2 = _mm256_shuffle_epi8(*lookup_vec, hi);
  __m256i local = _mm256_setzero_si256();
  local = _mm256_add_epi8(local, popcnt1);
  local = _mm256_add_epi8(local, popcnt2);

  acc = _mm256_add_epi64(acc, _mm256_sad_epu8(local, _mm256_setzero_si256()));

  uint64_t result = 0;

  result += (uint64_t)(_mm256_extract_epi64(acc, 0));
  result += (uint64_t)(_mm256_extract_epi64(acc, 1));
  result += (uint64_t)(_mm256_extract_epi64(acc, 2));
  result += (uint64_t)(_mm256_extract_epi64(acc, 3));

  return result;
}

void popcount(__m512i *v_ptr, __m512i *result, uint64_t *constants_avx512) {
  __m512i v = *v_ptr;
  const __m512i m1 = _mm512_set1_epi64(constants_avx512[0]);
  const __m512i m2 = _mm512_set1_epi64((constants_avx512[1]));
  const __m512i m4 = _mm512_set1_epi64(constants_avx512[2]);

  const __m512i t1 = _mm512_sub_epi8(v, (_mm512_srli_epi16(v, 1) & m1));
  const __m512i t2 = _mm512_add_epi8(t1 & m2, (_mm512_srli_epi16(t1, 2) & m2));
  const __m512i t3 = _mm512_add_epi8(t2, _mm512_srli_epi16(t2, 4)) & m4;
  *result = _mm512_sad_epu8(t3, _mm512_setzero_si512());
}

void CSA(__m512i *h, __m512i *l, __m512i *a_ptr, __m512i *b_ptr,
         __m512i *c_ptr) {
  __m512i a = *a_ptr;
  __m512i b = *b_ptr;
  __m512i c = *c_ptr;
  *l = _mm512_ternarylogic_epi32(c, b, a, 0x96);
  *h = _mm512_ternarylogic_epi32(c, b, a, 0xe8);
}

uint64_t simd_sum_epu64_256(__m256i *v_ptr) {

  __m256i v = *v_ptr;

  return (uint64_t)(_mm256_extract_epi64(v, 0)) +
         (uint64_t)(_mm256_extract_epi64(v, 1)) +
         (uint64_t)(_mm256_extract_epi64(v, 2)) +
         (uint64_t)(_mm256_extract_epi64(v, 3));
}

uint64_t simd_sum_epu64_512(__m512i *v_ptr) {

  __m512i v = *v_ptr;

  __m256i lo = _mm512_extracti64x4_epi64(v, 0);
  __m256i hi = _mm512_extracti64x4_epi64(v, 1);

  return simd_sum_epu64_256(&lo) + simd_sum_epu64_256(&hi);
}

uint64_t popcnt_AVX512_harleyseal(__m512i *data, uint64_t *size_ptr,
                                  uint64_t *constants_avx512) {

  uint64_t size = *size_ptr;

  __m512i total = _mm512_setzero_si512();
  __m512i ones = _mm512_setzero_si512();
  __m512i twos = _mm512_setzero_si512();
  __m512i fours = _mm512_setzero_si512();
  __m512i eights = _mm512_setzero_si512();
  __m512i sixteens = _mm512_setzero_si512();
  __m512i twosA, twosB, foursA, foursB, eightsA, eightsB;
  const uint64_t limit = size - size % 16;
  uint64_t i = 0;

  for (; i < limit; i += 16) {

    CSA(&twosA, &ones, &ones, &data[i + 0], &data[i + 1]);
    CSA(&twosB, &ones, &ones, &data[i + 2], &data[i + 3]);
    CSA(&foursA, &twos, &twos, &twosA, &twosB);
    CSA(&twosA, &ones, &ones, &data[i + 4], &data[i + 5]);
    CSA(&twosB, &ones, &ones, &data[i + 6], &data[i + 7]);
    CSA(&foursB, &twos, &twos, &twosA, &twosB);
    CSA(&eightsA, &fours, &fours, &foursA, &foursB);
    CSA(&twosA, &ones, &ones, &data[i + 8], &data[i + 9]);
    CSA(&twosB, &ones, &ones, &data[i + 10], &data[i + 11]);
    CSA(&foursA, &twos, &twos, &twosA, &twosB);
    CSA(&twosA, &ones, &ones, &data[i + 12], &data[i + 13]);
    CSA(&twosB, &ones, &ones, &data[i + 14], &data[i + 15]);
    CSA(&foursB, &twos, &twos, &twosA, &twosB);
    CSA(&eightsB, &fours, &fours, &foursA, &foursB);
    CSA(&sixteens, &eights, &eights, &eightsA, &eightsB);
    __m512i popcount_sixteens = _mm512_setzero_si512();
    popcount(&sixteens, &popcount_sixteens, constants_avx512);
    total = _mm512_add_epi64(total, popcount_sixteens);
  }

  __m512i popcount_eights = _mm512_setzero_si512();
  popcount(&eights, &popcount_eights, constants_avx512);
  __m512i popcount_fours = _mm512_setzero_si512();
  popcount(&fours, &popcount_fours, constants_avx512);
  __m512i popcount_twos = _mm512_setzero_si512();
  popcount(&twos, &popcount_twos, constants_avx512);
  __m512i popcount_ones = _mm512_setzero_si512();
  popcount(&ones, &popcount_ones, constants_avx512);

  total = _mm512_slli_epi64(total, 4); // * 16
  total = _mm512_add_epi64(total,
                           _mm512_slli_epi64(popcount_eights, 3)); // += 8 * ...
  total = _mm512_add_epi64(total,
                           _mm512_slli_epi64(popcount_fours, 2)); // += 4 * ...
  total = _mm512_add_epi64(total,
                           _mm512_slli_epi64(popcount_twos, 1)); // += 2 * ...
  total = _mm512_add_epi64(total, popcount_ones);

  for (; i < size; i++) {
    __m512i result = _mm512_setzero_si512();
    popcount(&data[i], &result, constants_avx512);
    total = _mm512_add_epi64(total, result);
  }

  return simd_sum_epu64_512(&total);
}

static inline uint64_t popcnt_64bit_512(uint64_t *src,
                                        uint64_t *popcnt_constants) {
  uint64_t x = *src;
  x = (x & popcnt_constants[0]) + ((x >> 1) & popcnt_constants[0]);
  x = (x & popcnt_constants[1]) + ((x >> 2) & popcnt_constants[1]);
  x = (x & popcnt_constants[2]) + ((x >> 4) & popcnt_constants[2]);
  return (x * popcnt_constants[3]) >> 56;
}

void hamming_bitwise_512(uint64_t *a, uint64_t *b, uint64_t *res, long *len,
                         uint64_t *popcnt_constants) {

  int n = *len;

  uint64_t sum = 0;

  // fast path for small dimensions
  if (n < 8) {
    do {
      uint64_t xor = a[0] ^ b[0];
      sum += popcnt_64bit_512(&xor, popcnt_constants);

      n--;
      a++;
      b++;
    } while (n);

    *res = sum;
    return;
  }

  __m256i zeros_256 = _mm256_setzero_si256();

  // process 128 uint64s at a time
  while (n >= 128) {

    size_t size = 16;

    __m512i a_vec0 = _mm512_loadu_si512((__m512i const *)a);
    __m512i a_vec1 = _mm512_loadu_si512((__m512i const *)(a + 8));
    __m512i a_vec2 = _mm512_loadu_si512((__m512i const *)(a + 16));
    __m512i a_vec3 = _mm512_loadu_si512((__m512i const *)(a + 24));
    __m512i a_vec4 = _mm512_loadu_si512((__m512i const *)(a + 32));
    __m512i a_vec5 = _mm512_loadu_si512((__m512i const *)(a + 40));
    __m512i a_vec6 = _mm512_loadu_si512((__m512i const *)(a + 48));
    __m512i a_vec7 = _mm512_loadu_si512((__m512i const *)(a + 56));
    __m512i a_vec8 = _mm512_loadu_si512((__m512i const *)(a + 64));
    __m512i a_vec9 = _mm512_loadu_si512((__m512i const *)(a + 72));
    __m512i a_vec10 = _mm512_loadu_si512((__m512i const *)(a + 80));
    __m512i a_vec11 = _mm512_loadu_si512((__m512i const *)(a + 88));
    __m512i a_vec12 = _mm512_loadu_si512((__m512i const *)(a + 96));
    __m512i a_vec13 = _mm512_loadu_si512((__m512i const *)(a + 104));
    __m512i a_vec14 = _mm512_loadu_si512((__m512i const *)(a + 112));
    __m512i a_vec15 = _mm512_loadu_si512((__m512i const *)(a + 120));

    __m512i b_vec0 = _mm512_loadu_si512((__m512i const *)b);
    __m512i b_vec1 = _mm512_loadu_si512((__m512i const *)(b + 8));
    __m512i b_vec2 = _mm512_loadu_si512((__m512i const *)(b + 16));
    __m512i b_vec3 = _mm512_loadu_si512((__m512i const *)(b + 24));
    __m512i b_vec4 = _mm512_loadu_si512((__m512i const *)(b + 32));
    __m512i b_vec5 = _mm512_loadu_si512((__m512i const *)(b + 40));
    __m512i b_vec6 = _mm512_loadu_si512((__m512i const *)(b + 48));
    __m512i b_vec7 = _mm512_loadu_si512((__m512i const *)(b + 56));
    __m512i b_vec8 = _mm512_loadu_si512((__m512i const *)(b + 64));
    __m512i b_vec9 = _mm512_loadu_si512((__m512i const *)(b + 72));
    __m512i b_vec10 = _mm512_loadu_si512((__m512i const *)(b + 80));
    __m512i b_vec11 = _mm512_loadu_si512((__m512i const *)(b + 88));
    __m512i b_vec12 = _mm512_loadu_si512((__m512i const *)(b + 96));
    __m512i b_vec13 = _mm512_loadu_si512((__m512i const *)(b + 104));
    __m512i b_vec14 = _mm512_loadu_si512((__m512i const *)(b + 112));
    __m512i b_vec15 = _mm512_loadu_si512((__m512i const *)(b + 120));

    __m512i cmp_results[16];

    cmp_results[0] = _mm512_xor_si512(a_vec0, b_vec0);
    cmp_results[1] = _mm512_xor_si512(a_vec1, b_vec1);
    cmp_results[2] = _mm512_xor_si512(a_vec2, b_vec2);
    cmp_results[3] = _mm512_xor_si512(a_vec3, b_vec3);
    cmp_results[4] = _mm512_xor_si512(a_vec4, b_vec4);
    cmp_results[5] = _mm512_xor_si512(a_vec5, b_vec5);
    cmp_results[6] = _mm512_xor_si512(a_vec6, b_vec6);
    cmp_results[7] = _mm512_xor_si512(a_vec7, b_vec7);
    cmp_results[8] = _mm512_xor_si512(a_vec8, b_vec8);
    cmp_results[9] = _mm512_xor_si512(a_vec9, b_vec9);
    cmp_results[10] = _mm512_xor_si512(a_vec10, b_vec10);
    cmp_results[11] = _mm512_xor_si512(a_vec11, b_vec11);
    cmp_results[12] = _mm512_xor_si512(a_vec12, b_vec12);
    cmp_results[13] = _mm512_xor_si512(a_vec13, b_vec13);
    cmp_results[14] = _mm512_xor_si512(a_vec14, b_vec14);
    cmp_results[15] = _mm512_xor_si512(a_vec15, b_vec15);

    sum += popcnt_AVX512_harleyseal(cmp_results, &size, popcnt_constants);

    n -= 128;
    a += 128;
    b += 128;
  }

  while (n) {
    uint64_t xor = a[0] ^ b[0];
    sum += popcnt_64bit_512(&xor, popcnt_constants);
    n--;
    a++;
    b++;
  }

  *res = sum;
  return;
}