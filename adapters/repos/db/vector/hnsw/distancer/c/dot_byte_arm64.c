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

#include <arm_neon.h>

void dot_byte_256(unsigned char *a, unsigned char *b, unsigned int *res, long *len)
{
    int size = *len;

    // use the vectorized version for the first n - (n % 16) elements
    int l = size - (size % 16);

    int32x4_t res_vec0 = vdupq_n_s32(0);
    int32x4_t res_vec1 = vdupq_n_s32(0);
    int32x4_t res_vec2 = vdupq_n_s32(0);
    int32x4_t res_vec3 = vdupq_n_s32(0);

    int i = 0;

    // load 4*16 bytes at a time
    while (i + 64 <= l)
    {
        int8x16x4_t a4 = vld1q_s8_x4(a + i);
        int8x16x4_t b4 = vld1q_s8_x4(b + i);

        int8x16_t sum0_8 = vmulq_s8(a4.val[0], b4.val[0]);
        int8x16_t sum1_8 = vmulq_s8(a4.val[1], b4.val[1]);
        int8x16_t sum2_8 = vmulq_s8(a4.val[2], b4.val[2]);
        int8x16_t sum3_8 = vmulq_s8(a4.val[3], b4.val[3]);

        // Pairwise add and promote from int8 to int16
        int16x8_t sum0_16 = vpaddlq_s8(sum0_8);
        int16x8_t sum1_16 = vpaddlq_s8(sum1_8);
        int16x8_t sum2_16 = vpaddlq_s8(sum2_8);
        int16x8_t sum3_16 = vpaddlq_s8(sum3_8);

        // Further pairwise add and promote from int16 to int32
        int32x4_t sum0_32 = vpaddlq_s16(sum0_16);
        int32x4_t sum1_32 = vpaddlq_s16(sum1_16);
        int32x4_t sum2_32 = vpaddlq_s16(sum2_16);
        int32x4_t sum3_32 = vpaddlq_s16(sum3_16);

        res_vec0 += sum0_32;
        res_vec1 += sum1_32;
        res_vec2 += sum2_32;
        res_vec3 += sum3_32;

        i += 64;
    }

    while (i < l)
    {
        int8x16_t a_vec = vld1q_s8(a + i);
        int8x16_t b_vec = vld1q_s8(b + i);

        int8x16_t sum0_8 = vmulq_s8(a_vec, b_vec);
        int16x8_t sum0_16 = vpaddlq_s8(sum0_8);
        int32x4_t sum0_32 = vpaddlq_s16(sum0_16);

        res_vec0 += sum0_32;

        i += 16;
    }

    int sum = 0;

    sum += vaddvq_s32(res_vec0);
    sum += vaddvq_s32(res_vec1);
    sum += vaddvq_s32(res_vec2);
    sum += vaddvq_s32(res_vec3);
    int8x16_t vec = vld1q_s8(a + l);

    // process the last elements
    int j = l;
    while (j < size)
    {
        sum += a[j] * b[j];
        // these two work for some reason:
        // sum += a[l] * b[l];
        // sum += a[size - 1] * b[size - 1];
        j++;
    }

    *res = sum;
}