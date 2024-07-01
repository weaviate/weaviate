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

void dot_byte_256(unsigned char *a, unsigned char *b, unsigned int *res, long *len)
{
    int n = *len;

    // fast path for small dimensions
    if (n < 32)
    {
        long acc = 0;
        for (int i = 0; i < n; i++)
        {
            acc += (unsigned int)(a[i]) * (unsigned int)(b[i]);
        }

        *res = acc;
        return;
    }

    __m256i acc = _mm256_setzero_si256();

    int i;
    // Process 32 bytes at a time
    for (i = 0; i + 31 < n; i += 32)
    {
        __m256i vec_a, vec_b;

        // Load 32 bytes
        vec_a = _mm256_loadu_si256((const __m256i *)(a + i));
        vec_b = _mm256_loadu_si256((const __m256i *)(b + i));

        // Create two registries for vector a
        __m256i a_high = _mm256_srli_epi16(vec_a, 8);  // arithmetic right shift
        __m256i a_low = _mm256_bslli_epi128(vec_a, 1); // left 1 byte = low to high in each 16-bit element
        a_low = _mm256_srli_epi16(a_low, 8);           // arithmetic right shift

        // Create two registries for vector b
        __m256i b_high = _mm256_srli_epi16(vec_b, 8);
        __m256i b_low = _mm256_bslli_epi128(vec_b, 1);
        b_low = _mm256_srli_epi16(b_low, 8);

        __m256i prod_hi = _mm256_madd_epi16(a_high, b_high);
        __m256i prod_lo = _mm256_madd_epi16(a_low, b_low);

        __m256i quadsum = _mm256_add_epi32(prod_lo, prod_hi);

        acc = _mm256_add_epi32(acc, quadsum);
    }

    // Reduce
    __m128i acc_low = _mm256_extracti128_si256(acc, 0);
    __m128i acc_high = _mm256_extracti128_si256(acc, 1);
    __m128i acc128 = _mm_add_epi32(acc_low, acc_high);
    acc128 = _mm_add_epi32(acc128, _mm_shuffle_epi32(acc128, _MM_SHUFFLE(0, 1, 2, 3)));
    acc128 = _mm_add_epi32(acc128, _mm_shuffle_epi32(acc128, _MM_SHUFFLE(0, 0, 0, 1)));

    unsigned int result = _mm_extract_epi32(acc128, 0);

    // Tail
    for (; i < n; i++)
    {
        result += (unsigned int)(a[i]) * (unsigned int)(b[i]);
    }

    *res = result;
}