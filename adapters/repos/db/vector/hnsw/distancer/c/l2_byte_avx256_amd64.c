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

void l2_byte_256(unsigned char *a, unsigned char *b, unsigned int *res, long *len)
{
    int n = *len;

    // Fast path for small dimensions
    if (n < 32)
    {
        long acc = 0;
        for (int i = 0; i < n; i++)
        {
            long diff = a[i] - b[i]; 
            acc += diff * diff;      
        }

        *res = acc;
        return;
    }

    __m256i acc = _mm256_setzero_si256();

    int i;
    // Process 32 bytes at a time
    for (i = 0; i + 31 < n; i += 32)
    {
        // Load 32 bytes
        __m256i vec_a = _mm256_loadu_si256((__m256i_u*)(a + i));
        __m256i vec_b = _mm256_loadu_si256((__m256i_u*)(b + i));

        // Unpack 8 to 16 bits
        __m256i va_lo = _mm256_unpacklo_epi8(vec_a, _mm256_setzero_si256());
        __m256i vb_lo = _mm256_unpacklo_epi8(vec_b, _mm256_setzero_si256());
        __m256i va_hi = _mm256_unpackhi_epi8(vec_a, _mm256_setzero_si256());
        __m256i vb_hi = _mm256_unpackhi_epi8(vec_b, _mm256_setzero_si256());

        // Diff on high and low bits
        __m256i diff_lo = _mm256_sub_epi16(va_lo, vb_lo);
        __m256i diff_hi = _mm256_sub_epi16(va_hi, vb_hi);

        // Square the diffs
        __m256i sq_diff_lo = _mm256_madd_epi16(diff_lo, diff_lo);
        __m256i sq_diff_hi = _mm256_madd_epi16(diff_hi, diff_hi);

        // Accumulate the results
        acc = _mm256_add_epi32(acc, sq_diff_lo);
        acc = _mm256_add_epi32(acc, sq_diff_hi);
    }

    // Reduce
    __m128i acc_low = _mm256_extracti128_si256(acc, 0);
    __m128i acc_high = _mm256_extracti128_si256(acc, 1);
    __m128i acc128 = _mm_add_epi32(acc_low, acc_high);
    acc128 = _mm_add_epi32(acc128, _mm_shuffle_epi32(acc128, _MM_SHUFFLE(0, 1, 2, 3)));
    acc128 = _mm_add_epi32(acc128, _mm_shuffle_epi32(acc128, _MM_SHUFFLE(0, 0, 0, 1)));

    int result = _mm_extract_epi32(acc128, 0);

    // Tail
    for (; i < n; i++)
    {
        long diff = a[i] - b[i];
        result += diff * diff;
    }

    *res = result;
}
