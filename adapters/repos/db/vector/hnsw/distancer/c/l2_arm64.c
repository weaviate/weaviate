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

void l2(float *a, float *b, float *res, long *len)
{
    float32x4_t res_vec = vdupq_n_f32(0);

    // if the length is smaller than 4, we can't use the vectorized version
    if (*len < 4)
    {
        for (int i = 0; i < *len; i++)
        {
            float diff = a[i] - b[i];
            float sq = diff * diff;
            res[0] += sq;
        }
        return;
    }

    // use the vectorized version for the first n - (n % 4) elements
    int l = *len - (*len % 4);

    for (int i = 0; i < l; i += 4)
    {
        float32x4_t a_vec = vld1q_f32(a + i);
        float32x4_t b_vec = vld1q_f32(b + i);
        float32x4_t diff = vsubq_f32(a_vec, b_vec);
        float32x4_t sq = vmulq_f32(diff, diff);

        res_vec = vaddq_f32(res_vec, sq);
    }

    // convert to scalar
    float sum = vaddvq_f32(res_vec);

    // add the remaining vectors
    for (int i = l; i < *len; i++)
    {
        float diff = a[i] - b[i];
        float sq = diff * diff;
        sum += sq;
    }

    res[0] = sum;
}
