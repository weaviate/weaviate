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

void hamming_256(float *a, float *b, float *res, long *len)
{
    int n = *len;
    int sum = 0;

    // fast path for small dimensions
    if (n < 8)
    {
        do
        {
            sum += a[0] != b[0] ? 1 : 0;
            n--;
            a++;
            b++;
        } while (n);

        *res = sum;
        return;
    }

    // Create 4 registers to store the results
    __m256i acc[4];
    acc[0] = _mm256_setzero_si256();
    acc[1] = _mm256_setzero_si256();
    acc[2] = _mm256_setzero_si256();
    acc[3] = _mm256_setzero_si256();

    __m256i ones_256 = _mm256_set1_epi32(1);
    __m256i zeros_256 = _mm256_setzero_si256();

    __m256i blend1_256 = _mm256_setzero_si256();
    __m256i blend2_256 = _mm256_setzero_si256();
    __m256i blend3_256 = _mm256_setzero_si256();
    __m256i blend4_256 = _mm256_setzero_si256();

    __m256 cmp_result_1 = _mm256_setzero_ps();
    __m256 cmp_result_2 = _mm256_setzero_ps();
    __m256 cmp_result_3 = _mm256_setzero_ps();
    __m256 cmp_result_4 = _mm256_setzero_ps();

    __m256i cmp_result_i_1 = _mm256_setzero_si256();
    __m256i cmp_result_i_2 = _mm256_setzero_si256();
    __m256i cmp_result_i_3 = _mm256_setzero_si256();
    __m256i cmp_result_i_4 = _mm256_setzero_si256();

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

        cmp_result_1 = _mm256_cmp_ps(a_vec0, b_vec0, _CMP_NEQ_OQ);
        cmp_result_2 = _mm256_cmp_ps(a_vec1, b_vec1, _CMP_NEQ_OQ);
        cmp_result_3 = _mm256_cmp_ps(a_vec2, b_vec2, _CMP_NEQ_OQ);
        cmp_result_4 = _mm256_cmp_ps(a_vec3, b_vec3, _CMP_NEQ_OQ);

        cmp_result_i_1 = _mm256_castps_si256(cmp_result_1);
        cmp_result_i_2 = _mm256_castps_si256(cmp_result_2);
        cmp_result_i_3 = _mm256_castps_si256(cmp_result_3);
        cmp_result_i_4 = _mm256_castps_si256(cmp_result_4);

        blend1_256 = _mm256_blendv_epi8(zeros_256, ones_256, cmp_result_i_1);
        blend2_256 = _mm256_blendv_epi8(zeros_256, ones_256, cmp_result_i_2);
        blend3_256 = _mm256_blendv_epi8(zeros_256, ones_256, cmp_result_i_3);
        blend4_256 = _mm256_blendv_epi8(zeros_256, ones_256, cmp_result_i_4);

        acc[0] = _mm256_add_epi32(acc[0], blend1_256);
        acc[1] = _mm256_add_epi32(acc[1], blend2_256);
        acc[2] = _mm256_add_epi32(acc[2], blend3_256);
        acc[3] = _mm256_add_epi32(acc[3], blend4_256);

        n -= 32;
        a += 32;
        b += 32;
    }

    // Process 8 floats at a time
    while (n >= 8)
    {
        __m256 a_vec0 = _mm256_loadu_ps(a);
        __m256 b_vec0 = _mm256_loadu_ps(b);

        // Perform comparison. _CMP_NEQ_OQ checks for not-equal (ordered, non-signaling)
        cmp_result_1 = _mm256_cmp_ps(a_vec0, b_vec0, _CMP_NEQ_OQ);

        // Cast the comparison result to integer type to use with blendv
        cmp_result_i_1 = _mm256_castps_si256(cmp_result_1);

        // Blend based on the comparison result. Note that blendv uses the MSB of each byte.
        blend1_256 = _mm256_blendv_epi8(zeros_256, ones_256, cmp_result_i_1);

        // Accumulate the result

        acc[0] = _mm256_add_epi32(acc[0], blend1_256);

        n -= 8;
        a += 8;
        b += 8;
    }

    // Tail
    while (n)
    {
        if (a[0] != b[0])
        {
            sum++;
        }
        n--;
        a++;
        b++;
    }

    // Reduce and store the result
    acc[0] = _mm256_add_epi32(acc[1], acc[0]);
    acc[2] = _mm256_add_epi32(acc[3], acc[2]);
    acc[0] = _mm256_add_epi32(acc[2], acc[0]);
    __m256 t1 = _mm256_hadd_epi32(acc[0], acc[0]);
    __m256 t2 = _mm256_hadd_epi32(t1, t1);
    __m128i t3 = _mm256_extracti128_si256(t2, 1);               // Extract the high 128 bits as integer vector
    __m128i t4 = _mm_add_epi32(_mm256_castsi256_si128(t2), t3); // Add two __m128i vectors
    sum += _mm_extract_epi32(t4, 0);

    *res = sum;
}