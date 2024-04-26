//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

#include <arm_sve.h>

// l2_sve only works with length >= 16
void l2_sve(float *a, float *b, float *res, long *len)
{
    uint64_t size = *len;

    uint64_t vsize = svcntw();
    uint64_t vsizex4 = vsize * 4;

    // use the vectorized version for the first n - (n % 4) elements
    uint64_t l = size - (size % vsize);

    // create 4*4 registers to store the result
    svfloat32_t res_vec0 = svdup_n_f32(0.0f);
    svfloat32_t res_vec1 = svdup_n_f32(0.0f);
    svfloat32_t res_vec2 = svdup_n_f32(0.0f);
    svfloat32_t res_vec3 = svdup_n_f32(0.0f);

    svbool_t pred = svptrue_b32();

    uint64_t i = 0;

    // load 4*vsize floats at a time
    while (i + vsizex4 <= l)
    {
        svfloat32_t a0 = svld1_f32(pred, a + i);
        svfloat32_t a1 = svld1_f32(pred, a + i + vsize);
        svfloat32_t a2 = svld1_f32(pred, a + i + vsize*2);
        svfloat32_t a3 = svld1_f32(pred, a + i + vsize*3);
        svfloat32_t b0 = svld1_f32(pred, b + i);
        svfloat32_t b1 = svld1_f32(pred, b + i + vsize);
        svfloat32_t b2 = svld1_f32(pred, b + i + vsize*2);
        svfloat32_t b3 = svld1_f32(pred, b + i + vsize*3);

        svfloat32_t diff0 = svsub_f32_x(pred, a0, b0);
        svfloat32_t diff1 = svsub_f32_x(pred, a1, b1);
        svfloat32_t diff2 = svsub_f32_x(pred, a2, b2);
        svfloat32_t diff3 = svsub_f32_x(pred, a3, b3);

        res_vec0 = svmla_f32_x(pred, res_vec0, diff0, diff0);
        res_vec1 = svmla_f32_x(pred, res_vec1, diff1, diff1);
        res_vec2 = svmla_f32_x(pred, res_vec2, diff2, diff2);
        res_vec3 = svmla_f32_x(pred, res_vec3, diff3, diff3);

        i += vsizex4;
    }

    while (i < l)
    {
        svfloat32_t a_vec = svld1_f32(pred, a + i);
        svfloat32_t b_vec = svld1_f32(pred, b + i);
        svfloat32_t diff = svsub_f32_x(pred, a_vec, b_vec);
        res_vec0 = svmla_f32_x(pred, res_vec0, diff, diff);

        i += vsize;
    }

    // reduce
    float32_t sum = svaddv_f32(pred, res_vec0);
    sum += svaddv_f32(pred, res_vec1);
    sum += svaddv_f32(pred, res_vec2);
    sum += svaddv_f32(pred, res_vec3);

    // add the remaining vectors
    for (i = l; i < size; i++)
    {
        float32_t diff = a[i] - b[i];
        float32_t sq = diff * diff;
        sum += sq;
    }

    res[0] = sum;
}

