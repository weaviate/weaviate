#include <arm_neon.h>

// Dot product between float and byte arrays
// Due to limitations with goat, it can only handle arrays of multiple of 16 elements.
// i.e len % 16 == 0 and len >= 16
void dot_float_byte_neon(float *a, unsigned char *b, float *res, long *len)
{
    int n = *len;
    float sum = 0;

    // Create 4*4 registers to store the result
    float32x4_t res_vec = vdupq_n_f32(0);

    // Vectorized loop
    while (n >= 16)
    {
        float32x4_t a_vec0 = vld1q_f32(a);
        float32x4_t a_vec1 = vld1q_f32(a + 4);
        float32x4_t a_vec2 = vld1q_f32(a + 8);
        float32x4_t a_vec3 = vld1q_f32(a + 12);

        uint8x16_t byte_vector = vld1q_u8(b);
        uint16x8_t byte_vector_low = vmovl_u8(vget_low_u8(byte_vector));
        uint16x8_t byte_vector_high = vmovl_u8(vget_high_u8(byte_vector));

        float32x4_t b_vec0 = vcvtq_f32_u32(vmovl_u16(vget_low_u16(byte_vector_low)));
        float32x4_t b_vec1 = vcvtq_f32_u32(vmovl_u16(vget_high_u16(byte_vector_low)));
        float32x4_t b_vec2 = vcvtq_f32_u32(vmovl_u16(vget_low_u16(byte_vector_high)));
        float32x4_t b_vec3 = vcvtq_f32_u32(vmovl_u16(vget_high_u16(byte_vector_high)));

        res_vec = vmlaq_f32(res_vec, a_vec0, b_vec0);
        res_vec = vmlaq_f32(res_vec, a_vec1, b_vec1);
        res_vec = vmlaq_f32(res_vec, a_vec2, b_vec2);
        res_vec = vmlaq_f32(res_vec, a_vec3, b_vec3);

        n -= 16;
        a += 16;
        b += 16;
    }

    // Horizontal add
    float temp[4];
    vst1q_f32(temp, res_vec);
    sum += temp[0] + temp[1] + temp[2] + temp[3];

    *res = sum;
}
