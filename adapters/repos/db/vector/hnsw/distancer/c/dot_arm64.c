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

void dot(float *a, float *b, float *res, long *len)
{
    // if the length is smaller than 4, we can't use the vectorized version
    if (*len < 4)
    {
        for (int i = 0; i < *len; i++)
        {
            res[0] += a[i] * b[i];
        }
        return;
    }

    // use the vectorized version for the first n - (n % 4) elements
    int l = *len - (*len % 4);

    int i = 0;

    // create 4*4 registers to store the result
    float32x4_t res_vec0 = vdupq_n_f32(0);
    float32x4_t res_vec1 = vdupq_n_f32(0);
    float32x4_t res_vec2 = vdupq_n_f32(0);
    float32x4_t res_vec3 = vdupq_n_f32(0);

    // load 4*4 floats at a time
    while (i + 16 < l)
    {
        float32x4x4_t a4 = vld1q_f32_x4(a + i);
        float32x4x4_t b4 = vld1q_f32_x4(b + i);

        res_vec0 += vmulq_f32(a4.val[0], b4.val[0]);
        res_vec1 += vmulq_f32(a4.val[1], b4.val[1]);
        res_vec2 += vmulq_f32(a4.val[2], b4.val[2]);
        res_vec3 += vmulq_f32(a4.val[3], b4.val[3]);

        i += 16;
    }

    while (i < l)
    {
        float32x4_t a_vec = vld1q_f32(a + i);
        float32x4_t b_vec = vld1q_f32(b + i);
        res_vec0 += vmulq_f32(a_vec, b_vec);

        i += 4;
    }

    // convert to scalar
    float sum = vaddvq_f32(res_vec0);
    sum += vaddvq_f32(res_vec1);
    sum += vaddvq_f32(res_vec2);
    sum += vaddvq_f32(res_vec3);

    // add the remaining vectors
    for (int i = l; i < *len; i++)
    {
        sum += a[i] * b[i];
    }

    res[0] = sum;
}
