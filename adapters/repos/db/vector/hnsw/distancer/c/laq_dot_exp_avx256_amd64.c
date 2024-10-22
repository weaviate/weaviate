#include <immintrin.h>
#include <stdint.h>

// Dot product between float and byte arrays
// Due to limitations with goat, it can only handle arrays of multiple of 16
// elements. i.e len % 16 == 0 and len >= 16
void laq_dot_exp_avx256(float *x, unsigned char *y1, unsigned char *y2,
                        float *a1, float *res, long *len) {
  int n = *len;

  float *a2 = res;

  float sum = 0;

  //  fast path for small dimensions
  if (n < 8) {
    do {
      sum += x[0] * (*a1 * (float)(y1[0]) + *a2 * (float)y2[0]);
      n--;
      x++;
      y1++;
      y2++;
    } while (n);

    *res = sum;
    return;
  }

  // Create 4 registers to store the results
  __m256 acc[4];
  acc[0] = _mm256_setzero_ps();
  acc[1] = _mm256_setzero_ps();
  acc[2] = _mm256_setzero_ps();
  acc[3] = _mm256_setzero_ps();

  __m256 a1_vec = _mm256_set1_ps(*a1);
  __m256 a2_vec = _mm256_set1_ps(*a2);

  while (n >= 32) {
    // Unroll loop for 32 floats
    __m256 x_vec0 = _mm256_loadu_ps(x);
    __m256 x_vec1 = _mm256_loadu_ps(x + 8);
    __m256 x_vec2 = _mm256_loadu_ps(x + 16);
    __m256 x_vec3 = _mm256_loadu_ps(x + 24);

    // Unroll loop for 32 bytes
    __m128i y1_byte_vec0 = _mm_loadu_si128((__m128i *)y1);
    __m128i y1_byte_vec1 = _mm_loadu_si128((__m128i *)(y1 + 8));
    __m128i y1_byte_vec2 = _mm_loadu_si128((__m128i *)(y1 + 16));
    __m128i y1_byte_vec3 = _mm_loadu_si128((__m128i *)(y1 + 24));

    // Convert to floats
    __m256 y1_vec0 = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(y1_byte_vec0));
    __m256 y1_vec1 = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(y1_byte_vec1));
    __m256 y1_vec2 = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(y1_byte_vec2));
    __m256 y1_vec3 = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(y1_byte_vec3));

    y1_vec0 = _mm256_mul_ps(y1_vec0, a1_vec);
    y1_vec1 = _mm256_mul_ps(y1_vec1, a1_vec);
    y1_vec2 = _mm256_mul_ps(y1_vec2, a1_vec);
    y1_vec3 = _mm256_mul_ps(y1_vec3, a1_vec);

    // Unroll loop for 32 bytes
    __m128i y2_byte_vec0 = _mm_loadu_si128((__m128i *)y2);
    __m128i y2_byte_vec1 = _mm_loadu_si128((__m128i *)(y2 + 8));
    __m128i y2_byte_vec2 = _mm_loadu_si128((__m128i *)(y2 + 16));
    __m128i y2_byte_vec3 = _mm_loadu_si128((__m128i *)(y2 + 24));

    // Convert to floats
    __m256 y2_vec0 = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(y2_byte_vec0));
    __m256 y2_vec1 = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(y2_byte_vec1));
    __m256 y2_vec2 = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(y2_byte_vec2));
    __m256 y2_vec3 = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(y2_byte_vec3));

    __m256 y1plusy2_vec0 = _mm256_fmadd_ps(y2_vec0, a2_vec, y1_vec0);
    __m256 y1plusy2_vec1 = _mm256_fmadd_ps(y2_vec1, a2_vec, y1_vec1);
    __m256 y1plusy2_vec2 = _mm256_fmadd_ps(y2_vec2, a2_vec, y1_vec2);
    __m256 y1plusy2_vec3 = _mm256_fmadd_ps(y2_vec3, a2_vec, y1_vec3);

    acc[0] = _mm256_fmadd_ps(x_vec0, y1plusy2_vec0, acc[0]);
    acc[1] = _mm256_fmadd_ps(x_vec1, y1plusy2_vec1, acc[1]);
    acc[2] = _mm256_fmadd_ps(x_vec2, y1plusy2_vec2, acc[2]);
    acc[3] = _mm256_fmadd_ps(x_vec3, y1plusy2_vec3, acc[3]);

    n -= 32;
    x += 32;
    y1 += 32;
    y2 += 32;
  }

  //   // Process 8 floats at a time
  while (n >= 8) {
    __m256 x_vec0 = _mm256_loadu_ps(x);

    __m128i y1_byte_vec0 = _mm_loadu_si128((__m128i *)y1);

    __m256 y1_vec0 = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(y1_byte_vec0));

    y1_vec0 = _mm256_mul_ps(y1_vec0, a1_vec);

    __m128i y2_byte_vec0 = _mm_loadu_si128((__m128i *)y2);

    __m256 y2_vec0 = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(y2_byte_vec0));

    __m256 y1plusy2_vec0 = _mm256_fmadd_ps(y2_vec0, a2_vec, y1_vec0);

    acc[0] = _mm256_fmadd_ps(x_vec0, y1plusy2_vec0, acc[0]);

    n -= 8;
    x += 8;
    y1 += 8;
    y2 += 8;
  }

  // Tail
  while (n) {
    sum += x[0] * (*a1 * (float)(y1[0]) + *a2 * (float)y2[0]);
    n--;
    x++;
    y1++;
    y2++;
  }

  // Reduce and store the result
  acc[0] = _mm256_add_ps(acc[1], acc[0]);
  acc[2] = _mm256_add_ps(acc[3], acc[2]);
  acc[0] = _mm256_add_ps(acc[2], acc[0]);
  __m256 t1 = _mm256_hadd_ps(acc[0], acc[0]);
  __m256 t2 = _mm256_hadd_ps(t1, t1);
  __m128 t3 = _mm256_extractf128_ps(t2, 1);
  __m128 t4 = _mm_add_ps(_mm256_castps256_ps128(t2), t3);
  sum += _mm_cvtss_f32(t4);

  *res = sum;
}
