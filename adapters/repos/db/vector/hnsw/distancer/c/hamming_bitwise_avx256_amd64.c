#include <immintrin.h>
#include <nmmintrin.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

static void init_lookup64bit(uint64_t *lut) {
  for (int i = 0; i < 256; i++) {
    uint64_t count = 0;
    int n = i;
    while (n) {
      count += n & 1;
      n >>= 1;
    }
    lut[i] = count;
  }
}

static uint64_t popcnt_lookup_64bit(uint8_t *data, uint64_t *n,
                                    uint64_t *lookup64bit) {
  if (data == NULL || n == NULL || lookup64bit == NULL) {
    return 0; // Error handling: invalid input
  }

  size_t result = 0;
  size_t i = 0;

  while (i + 4 <= *n) {
    result += lookup64bit[data[i]];
    i++;
    result += lookup64bit[data[i]];
    i++;
    result += lookup64bit[data[i]];
    i++;
    result += lookup64bit[data[i]];
    i++;
  }

  while (i < *n) {
    result += lookup64bit[data[i]];
    i++;
  }

  return result;
}

void hamming_bitwise_256(uint64_t *a, uint64_t *b, uint64_t *res, long *len) {
  // Initialize the lookup table

  uint64_t *lookup64bit = (uint64_t *)malloc(256 * sizeof(uint64_t));

  if (lookup64bit == NULL) {
    return;
  }

  // if (!is_initialized) {
  init_lookup64bit(lookup64bit);
  // is_initialized = 1;
  // }

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

  __m256i ones_256 = _mm256_set1_epi32(1);
  __m256i zeros_256 = _mm256_setzero_si256();

  size_t size = 256 / 8;

  while (n >= 16) {
    __m256i a_vec0 = _mm256_loadu_si256((__m256i const *)a);
    __m256i a_vec1 = _mm256_loadu_si256((__m256i const *)a + 4);
    __m256i a_vec2 = _mm256_loadu_si256((__m256i const *)a + 8);
    __m256i a_vec3 = _mm256_loadu_si256((__m256i const *)a + 12);

    __m256i b_vec0 = _mm256_loadu_si256((__m256i const *)b);
    __m256i b_vec1 = _mm256_loadu_si256((__m256i const *)b + 1);
    __m256i b_vec2 = _mm256_loadu_si256((__m256i const *)b + 2);
    __m256i b_vec3 = _mm256_loadu_si256((__m256i const *)b + 3);

    __m256i cmp_result_1 = _mm256_xor_si256(a_vec0, b_vec0);
    __m256i cmp_result_2 = _mm256_xor_si256(a_vec1, b_vec1);
    __m256i cmp_result_3 = _mm256_xor_si256(a_vec2, b_vec2);
    __m256i cmp_result_4 = _mm256_xor_si256(a_vec3, b_vec3);

    uint8_t *p1 = (uint8_t *)&cmp_result_1;
    uint8_t *p2 = (uint8_t *)&cmp_result_2;
    uint8_t *p3 = (uint8_t *)&cmp_result_3;
    uint8_t *p4 = (uint8_t *)&cmp_result_4;

    sum += popcnt_lookup_64bit(p1, &size, lookup64bit);
    sum += popcnt_lookup_64bit(p2, &size, lookup64bit);
    sum += popcnt_lookup_64bit(p3, &size, lookup64bit);
    sum += popcnt_lookup_64bit(p4, &size, lookup64bit);

    n -= 16;
    a += 16;
    b += 16;
  }

  while (n >= 4) {
    __m256i a_vec0 = _mm256_loadu_si256((__m256i const *)a);
    __m256i b_vec0 = _mm256_loadu_si256((__m256i const *)b);
    __m256i cmp_result_1 = _mm256_xor_si256(a_vec0, b_vec0);
    uint8_t *p1 = (uint8_t *)&cmp_result_1;
    sum += popcnt_lookup_64bit(p1, &size, lookup64bit);

    n -= 4;
    a += 4;
    b += 4;
  }

  while (n) {
    sum += __builtin_popcountll(a[0] ^ b[0]);
    n--;
    a++;
    b++;
  }

  *res = sum;
}