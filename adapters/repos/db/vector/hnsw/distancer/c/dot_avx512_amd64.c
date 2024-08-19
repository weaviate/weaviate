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

void dot_512(float *a, float *b, float *res, long *len)
{
    int n = *len;
    float sum = 0;

    // fast path for small dimensions
    if (n < 8)
    {
        do
        {
            sum += a[0] * b[0];
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

    if (n >= 128)
    {
        // create 8 registers
        __m512 acc5[8];
        acc5[0] = _mm512_setzero_ps();
        acc5[1] = _mm512_setzero_ps();
        acc5[2] = _mm512_setzero_ps();
        acc5[3] = _mm512_setzero_ps();
        acc5[4] = _mm512_setzero_ps();
        acc5[5] = _mm512_setzero_ps();
        acc5[6] = _mm512_setzero_ps();
        acc5[7] = _mm512_setzero_ps();

        // Process 128 floats at a time
        do
        {
            __m512 a_vec0 = _mm512_loadu_ps(a);
            __m512 a_vec1 = _mm512_loadu_ps(a + 16);
            __m512 a_vec2 = _mm512_loadu_ps(a + 32);
            __m512 a_vec3 = _mm512_loadu_ps(a + 48);
            __m512 a_vec4 = _mm512_loadu_ps(a + 64);
            __m512 a_vec5 = _mm512_loadu_ps(a + 80);
            __m512 a_vec6 = _mm512_loadu_ps(a + 96);
            __m512 a_vec7 = _mm512_loadu_ps(a + 112);

            __m512 b_vec0 = _mm512_loadu_ps(b);
            __m512 b_vec1 = _mm512_loadu_ps(b + 16);
            __m512 b_vec2 = _mm512_loadu_ps(b + 32);
            __m512 b_vec3 = _mm512_loadu_ps(b + 48);
            __m512 b_vec4 = _mm512_loadu_ps(b + 64);
            __m512 b_vec5 = _mm512_loadu_ps(b + 80);
            __m512 b_vec6 = _mm512_loadu_ps(b + 96);
            __m512 b_vec7 = _mm512_loadu_ps(b + 112);

            acc5[0] = _mm512_fmadd_ps(a_vec0, b_vec0, acc5[0]);
            acc5[1] = _mm512_fmadd_ps(a_vec1, b_vec1, acc5[1]);
            acc5[2] = _mm512_fmadd_ps(a_vec2, b_vec2, acc5[2]);
            acc5[3] = _mm512_fmadd_ps(a_vec3, b_vec3, acc5[3]);
            acc5[4] = _mm512_fmadd_ps(a_vec4, b_vec4, acc5[4]);
            acc5[5] = _mm512_fmadd_ps(a_vec5, b_vec5, acc5[5]);
            acc5[6] = _mm512_fmadd_ps(a_vec6, b_vec6, acc5[6]);
            acc5[7] = _mm512_fmadd_ps(a_vec7, b_vec7, acc5[7]);

            n -= 128;
            a += 128;
            b += 128;
        } while (n >= 128);

        acc5[0] = _mm512_add_ps(acc5[1], acc5[0]);
        acc5[2] = _mm512_add_ps(acc5[3], acc5[2]);
        acc5[4] = _mm512_add_ps(acc5[5], acc5[4]);
        acc5[6] = _mm512_add_ps(acc5[7], acc5[6]);
        acc5[0] = _mm512_add_ps(acc5[2], acc5[0]);
        acc5[4] = _mm512_add_ps(acc5[6], acc5[4]);
        acc5[0] = _mm512_add_ps(acc5[4], acc5[0]);

        __m256 low = _mm512_castps512_ps256(acc5[0]);
        __m256 high = _mm256_castpd_ps(_mm512_extractf64x4_pd(_mm512_castps_pd(acc5[0]), 1));

        acc[0] = _mm256_add_ps(low, acc[0]);
        acc[0] = _mm256_add_ps(high, acc[0]);

        if (!n)
        {
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
            return;
        }
    }

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

        acc[0] = _mm256_fmadd_ps(a_vec0, b_vec0, acc[0]);
        acc[1] = _mm256_fmadd_ps(a_vec1, b_vec1, acc[1]);
        acc[2] = _mm256_fmadd_ps(a_vec2, b_vec2, acc[2]);
        acc[3] = _mm256_fmadd_ps(a_vec3, b_vec3, acc[3]);

        n -= 32;
        a += 32;
        b += 32;
    }

    // Process 8 floats at a time
    while (n >= 8)
    {
        __m256 a_vec0 = _mm256_loadu_ps(a);
        __m256 b_vec0 = _mm256_loadu_ps(b);

        acc[0] = _mm256_fmadd_ps(a_vec0, b_vec0, acc[0]);

        n -= 8;
        a += 8;
        b += 8;
    }

    // Tail
    while (n)
    {
        sum += a[0] * b[0];
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