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

void l2_256(float *a, float *b, float *res, long *len)
{
    int n = *len;
    float sum = 0;

    // fast path for small dimensions
    if (n < 8)
    {
        do
        {
            float diff = a[0] - b[0];
            float sq = diff * diff;
            sum += sq;
            n--;
            a++;
            b++;
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

    while (n >= 32)
    {
        // Unroll loop for 32 floats
        __m256 a_vec0 = _mm256_loadu_ps(a);
        __m256 a_vec1 = _mm256_loadu_ps(a + 8);
        __m256 a_vec2 = _mm256_loadu_ps(a + 16);
        __m256 a_vec3 = _mm256_loadu_ps(a + 24);

        __m256 b_vec0 = _mm256_loadu_ps(b);
        __m256 b_vec1 = _mm256_loadu_ps(b + 8);
        __m256 b_vec2 = _mm256_loadu_ps(b + 16);
        __m256 b_vec3 = _mm256_loadu_ps(b + 24);

        __m256 diff0 = _mm256_sub_ps(a_vec0, b_vec0);
        __m256 diff1 = _mm256_sub_ps(a_vec1, b_vec1);
        __m256 diff2 = _mm256_sub_ps(a_vec2, b_vec2);
        __m256 diff3 = _mm256_sub_ps(a_vec3, b_vec3);

        acc[0] = _mm256_fmadd_ps(diff0, diff0, acc[0]);
        acc[1] = _mm256_fmadd_ps(diff1, diff1, acc[1]);
        acc[2] = _mm256_fmadd_ps(diff2, diff2, acc[2]);
        acc[3] = _mm256_fmadd_ps(diff3, diff3, acc[3]);

        n -= 32;
        a += 32;
        b += 32;
    }

    // Process 8 floats at a time
    while (n >= 8)
    {
        __m256 a_vec0 = _mm256_loadu_ps(a);
        __m256 b_vec0 = _mm256_loadu_ps(b);
        __m256 diff0 = _mm256_sub_ps(a_vec0, b_vec0);

        acc[0] = _mm256_fmadd_ps(diff0, diff0, acc[0]);

        n -= 8;
        a += 8;
        b += 8;
    }

    // Tail
    while (n)
    {
        float diff = a[0] - b[0];
        float sq = diff * diff;
        sum += sq;
        n--;
        a++;
        b++;
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