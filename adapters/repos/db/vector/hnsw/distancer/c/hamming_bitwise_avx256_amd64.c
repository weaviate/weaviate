#include <immintrin.h>
#include <nmmintrin.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

uint64_t popcnt_lookup_64bit(uint8_t *data, uint64_t *n,
                             uint64_t *lookup64bit) {
  // return data[3];

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

uint64_t popcount(uint64_t *x, uint64_t *lookup64bit) {
  uint8_t *data = (uint8_t *)x;
  uint64_t result = 0;
  result += lookup64bit[data[0]];

  result += lookup64bit[data[1]];

  result += lookup64bit[data[2]];

  result += lookup64bit[data[3]];

  result += lookup64bit[data[4]];

  result += lookup64bit[data[5]];

  result += lookup64bit[data[6]];

  result += lookup64bit[data[7]];

  return result;
}

void hamming_bitwise_256(uint64_t *a, uint64_t *b, uint64_t *res, long *len,
                         uint64_t *lookup64bit) {

  int n = *len;
  uint64_t sum = 0;

  // fast path for small dimensions
  // if (n < 8) {
  //   do {
  //     sum += __builtin_popcountll(a[0] ^ b[0]);

  //     n--;
  //     a++;
  //     b++;
  //   } while (n);

  //   *res = sum;
  //   return;
  // }

  __m256i zeros_256 = _mm256_setzero_si256();

  size_t size = 256 / 8;

  while (n >= 16) {
    __m256i a_vec0 = _mm256_loadu_si256((__m256i const *)a);
    __m256i a_vec1 = _mm256_loadu_si256((__m256i const *)(a + 4));
    __m256i a_vec2 = _mm256_loadu_si256((__m256i const *)(a + 8));
    __m256i a_vec3 = _mm256_loadu_si256((__m256i const *)(a + 12));

    __m256i b_vec0 = _mm256_loadu_si256((__m256i const *)b);
    __m256i b_vec1 = _mm256_loadu_si256((__m256i const *)(b + 4));
    __m256i b_vec2 = _mm256_loadu_si256((__m256i const *)(b + 8));
    __m256i b_vec3 = _mm256_loadu_si256((__m256i const *)(b + 12));

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
    uint64_t xor = a[0] ^ b[0];
    sum += popcount(&xor, lookup64bit);
    n--;
    a++;
    b++;
  }

  *res = sum;
}