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

void hamming_512(float *a, float *b, float *res, long *len)
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

    __mmask16 mask0 = 0;
    __mmask16 mask1 = 0;
    __mmask16 mask2 = 0;
    __mmask16 mask3 = 0;
    __mmask16 mask4 = 0;
    __mmask16 mask5 = 0;
    __mmask16 mask6 = 0;
    __mmask16 mask7 = 0;

    __m512i ones = _mm512_set1_epi32(1);
    __m512i zeros = _mm512_setzero_si512();

    __m512 blend0 = _mm512_setzero_ps();
    __m512 blend1 = _mm512_setzero_ps();
    __m512 blend2 = _mm512_setzero_ps();
    __m512 blend3 = _mm512_setzero_ps();
    __m512 blend4 = _mm512_setzero_ps();
    __m512 blend5 = _mm512_setzero_ps();
    __m512 blend6 = _mm512_setzero_ps();
    __m512 blend7 = _mm512_setzero_ps();

    // Create 4 registers to store the results
    __m256i acc[4];
    acc[0] = _mm256_setzero_si256();
    acc[1] = _mm256_setzero_si256();
    acc[2] = _mm256_setzero_si256();
    acc[3] = _mm256_setzero_si256();

    if (n >= 128)
    {
        __m512i acc5[8];
        acc5[0] = _mm512_setzero_si512();
        acc5[1] = _mm512_setzero_si512();
        acc5[2] = _mm512_setzero_si512();
        acc5[3] = _mm512_setzero_si512();
        acc5[4] = _mm512_setzero_si512();
        acc5[5] = _mm512_setzero_si512();
        acc5[6] = _mm512_setzero_si512();
        acc5[7] = _mm512_setzero_si512();

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

            mask0 = _mm512_cmp_ps_mask(a_vec0, b_vec0, _CMP_NEQ_OQ);
            mask1 = _mm512_cmp_ps_mask(a_vec1, b_vec1, _CMP_NEQ_OQ);
            mask2 = _mm512_cmp_ps_mask(a_vec2, b_vec2, _CMP_NEQ_OQ);
            mask3 = _mm512_cmp_ps_mask(a_vec3, b_vec3, _CMP_NEQ_OQ);
            mask4 = _mm512_cmp_ps_mask(a_vec4, b_vec4, _CMP_NEQ_OQ);
            mask5 = _mm512_cmp_ps_mask(a_vec5, b_vec5, _CMP_NEQ_OQ);
            mask6 = _mm512_cmp_ps_mask(a_vec6, b_vec6, _CMP_NEQ_OQ);
            mask7 = _mm512_cmp_ps_mask(a_vec7, b_vec7, _CMP_NEQ_OQ);

            blend0 = _mm512_mask_blend_epi32(mask7, zeros, ones);
            blend1 = _mm512_mask_blend_epi32(mask0, zeros, ones);
            blend2 = _mm512_mask_blend_epi32(mask1, zeros, ones);
            blend3 = _mm512_mask_blend_epi32(mask2, zeros, ones);
            blend4 = _mm512_mask_blend_epi32(mask3, zeros, ones);
            blend5 = _mm512_mask_blend_epi32(mask4, zeros, ones);
            blend6 = _mm512_mask_blend_epi32(mask5, zeros, ones);
            blend7 = _mm512_mask_blend_epi32(mask6, zeros, ones);

            acc5[0] = _mm512_add_epi32(acc5[0], blend0);
            acc5[0] = _mm512_add_epi32(acc5[0], blend1);
            acc5[1] = _mm512_add_epi32(acc5[1], blend2);
            acc5[2] = _mm512_add_epi32(acc5[2], blend3);
            acc5[3] = _mm512_add_epi32(acc5[3], blend4);
            acc5[4] = _mm512_add_epi32(acc5[4], blend5);
            acc5[5] = _mm512_add_epi32(acc5[5], blend6);
            acc5[6] = _mm512_add_epi32(acc5[6], blend7);

            n -= 128;
            a += 128;
            b += 128;
        } while (n >= 128);

        acc5[0] = _mm512_add_epi32(acc5[1], acc5[0]);
        acc5[2] = _mm512_add_epi32(acc5[3], acc5[2]);
        acc5[4] = _mm512_add_epi32(acc5[5], acc5[4]);
        acc5[6] = _mm512_add_epi32(acc5[7], acc5[6]);
        acc5[0] = _mm512_add_epi32(acc5[2], acc5[0]);
        acc5[4] = _mm512_add_epi32(acc5[6], acc5[4]);
        acc5[0] = _mm512_add_epi32(acc5[4], acc5[0]);

        __m256i low = _mm512_castsi512_si256(acc5[0]);
        __m256i high = _mm512_extracti32x8_epi32(acc5[0], 1);

        acc[0] = _mm256_add_epi32(low, acc[0]);
        acc[0] = _mm256_add_epi32(high, acc[0]);

        if (!n)
        {
            // Reduce and store the result
            acc[0] = _mm256_add_epi32(acc[1], acc[0]);
            acc[2] = _mm256_add_epi32(acc[3], acc[2]);
            acc[0] = _mm256_add_epi32(acc[2], acc[0]);

            __m256i t1 = _mm256_hadd_epi32(acc[0], acc[0]);
            __m256i t2 = _mm256_hadd_epi32(t1, t1);
            __m128i t3 = _mm256_extracti128_si256(t2, 1);               // Extract the high 128 bits as integer vector
            __m128i t4 = _mm_add_epi32(_mm256_castsi256_si128(t2), t3); // Add two __m128i vectors
            sum += _mm_extract_epi32(t4, 0);

            *res = sum;
            return;
        }
    }

    __m256i ones_256 = _mm256_set1_epi32(1);
    __m256i zeros_256 = _mm256_setzero_si256(); // Vector of zeros
    // Use mask to blend 1s and 0s: 1s where comparison is true, 0s elsewhere
    __m256i blend0_256 = _mm256_setzero_si256();
    __m256i blend1_256 = _mm256_setzero_si256();
    __m256i blend2_256 = _mm256_setzero_si256();
    __m256i blend3_256 = _mm256_setzero_si256();

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

        mask0 = _mm256_cmp_ps_mask(a_vec0, b_vec0, _CMP_NEQ_OQ);
        mask1 = _mm256_cmp_ps_mask(a_vec1, b_vec1, _CMP_NEQ_OQ);
        mask2 = _mm256_cmp_ps_mask(a_vec2, b_vec2, _CMP_NEQ_OQ);
        mask3 = _mm256_cmp_ps_mask(a_vec3, b_vec3, _CMP_NEQ_OQ);

        blend0_256 = _mm256_mask_blend_epi32(mask0, zeros_256, ones_256);
        blend1_256 = _mm256_mask_blend_epi32(mask1, zeros_256, ones_256);
        blend2_256 = _mm256_mask_blend_epi32(mask2, zeros_256, ones_256);
        blend3_256 = _mm256_mask_blend_epi32(mask3, zeros_256, ones_256);

        acc[0] = _mm256_add_epi32(acc[0], blend0_256);
        acc[1] = _mm256_add_epi32(acc[1], blend1_256);
        acc[2] = _mm256_add_epi32(acc[2], blend2_256);
        acc[3] = _mm256_add_epi32(acc[3], blend3_256);

        n -= 32;
        a += 32;
        b += 32;
    }

    // Process 8 floats at a time
    while (n >= 8)
    {
        __m256 a_vec0 = _mm256_loadu_ps(a);
        __m256 b_vec0 = _mm256_loadu_ps(b);

        mask0 = _mm256_cmp_ps_mask(a_vec0, b_vec0, _CMP_NEQ_OQ);

        blend1_256 = _mm256_mask_blend_epi32(mask0, zeros_256, ones_256);
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