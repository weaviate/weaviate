#include <arm_neon.h>

void dot_byte_256(unsigned char *a, unsigned char *b, unsigned int *res, long *len)
{
    int size = *len;

    // Use the vectorized version for the first n - (n % 16) elements
    int l = size - (size % 16);

    uint32x4_t res_vec0 = vdupq_n_u32(0);
    uint32x4_t res_vec1 = vdupq_n_u32(0);
    uint32x4_t res_vec2 = vdupq_n_u32(0);
    uint32x4_t res_vec3 = vdupq_n_u32(0);

    int i = 0;

    // Load 4*16 bytes at a time
    while (i + 64 <= l)
    {
        uint8x16x4_t a4 = vld1q_u8_x4(a + i);
        uint8x16x4_t b4 = vld1q_u8_x4(b + i);

        // Convert 8-bit vectors to 16-bit vectors to prevent overflow
        uint16x8_t a0_low = vmovl_u8(vget_low_u8(a4.val[0]));
        uint16x8_t a0_high = vmovl_u8(vget_high_u8(a4.val[0]));
        uint16x8_t b0_low = vmovl_u8(vget_low_u8(b4.val[0]));
        uint16x8_t b0_high = vmovl_u8(vget_high_u8(b4.val[0]));

        uint16x8_t a1_low = vmovl_u8(vget_low_u8(a4.val[1]));
        uint16x8_t a1_high = vmovl_u8(vget_high_u8(a4.val[1]));
        uint16x8_t b1_low = vmovl_u8(vget_low_u8(b4.val[1]));
        uint16x8_t b1_high = vmovl_u8(vget_high_u8(b4.val[1]));

        uint16x8_t a2_low = vmovl_u8(vget_low_u8(a4.val[2]));
        uint16x8_t a2_high = vmovl_u8(vget_high_u8(a4.val[2]));
        uint16x8_t b2_low = vmovl_u8(vget_low_u8(b4.val[2]));
        uint16x8_t b2_high = vmovl_u8(vget_high_u8(b4.val[2]));

        uint16x8_t a3_low = vmovl_u8(vget_low_u8(a4.val[3]));
        uint16x8_t a3_high = vmovl_u8(vget_high_u8(a4.val[3]));
        uint16x8_t b3_low = vmovl_u8(vget_low_u8(b4.val[3]));
        uint16x8_t b3_high = vmovl_u8(vget_high_u8(b4.val[3]));

        // Multiply 16-bit vectors
        uint16x8_t product0_low = vmulq_u16(a0_low, b0_low);
        uint16x8_t product0_high = vmulq_u16(a0_high, b0_high);

        uint16x8_t product1_low = vmulq_u16(a1_low, b1_low);
        uint16x8_t product1_high = vmulq_u16(a1_high, b1_high);

        uint16x8_t product2_low = vmulq_u16(a2_low, b2_low);
        uint16x8_t product2_high = vmulq_u16(a2_high, b2_high);

        uint16x8_t product3_low = vmulq_u16(a3_low, b3_low);
        uint16x8_t product3_high = vmulq_u16(a3_high, b3_high);

        // Sum the products to 32-bit integers
        uint32x4_t sum0_low_32 = vpaddlq_u16(product0_low);
        uint32x4_t sum0_high_32 = vpaddlq_u16(product0_high);

        uint32x4_t sum1_low_32 = vpaddlq_u16(product1_low);
        uint32x4_t sum1_high_32 = vpaddlq_u16(product1_high);

        uint32x4_t sum2_low_32 = vpaddlq_u16(product2_low);
        uint32x4_t sum2_high_32 = vpaddlq_u16(product2_high);

        uint32x4_t sum3_low_32 = vpaddlq_u16(product3_low);
        uint32x4_t sum3_high_32 = vpaddlq_u16(product3_high);

        // Add the results to the final vectors
        res_vec0 = vaddq_u32(res_vec0, sum0_low_32);
        res_vec0 = vaddq_u32(res_vec0, sum0_high_32);

        res_vec1 = vaddq_u32(res_vec1, sum1_low_32);
        res_vec1 = vaddq_u32(res_vec1, sum1_high_32);

        res_vec2 = vaddq_u32(res_vec2, sum2_low_32);
        res_vec2 = vaddq_u32(res_vec2, sum2_high_32);

        res_vec3 = vaddq_u32(res_vec3, sum3_low_32);
        res_vec3 = vaddq_u32(res_vec3, sum3_high_32);

        i += 64;
    }

    // Process the remaining elements
    while (i < l)
    {
        uint8x16_t a_vec = vld1q_u8(a + i);
        uint8x16_t b_vec = vld1q_u8(b + i);

        // Convert 8-bit vectors to 16-bit vectors to prevent overflow
        uint16x8_t a_vec_low = vmovl_u8(vget_low_u8(a_vec));
        uint16x8_t a_vec_high = vmovl_u8(vget_high_u8(a_vec));
        uint16x8_t b_vec_low = vmovl_u8(vget_low_u8(b_vec));
        uint16x8_t b_vec_high = vmovl_u8(vget_high_u8(b_vec));

        // Multiply 16-bit vectors
        uint16x8_t product_low = vmulq_u16(a_vec_low, b_vec_low);
        uint16x8_t product_high = vmulq_u16(a_vec_high, b_vec_high);

        // Sum the products to 32-bit integers
        uint32x4_t sum_low_32 = vpaddlq_u16(product_low);
        uint32x4_t sum_high_32 = vpaddlq_u16(product_high);

        // Add the results to the final vector
        res_vec0 = vaddq_u32(res_vec0, sum_low_32);
        res_vec0 = vaddq_u32(res_vec0, sum_high_32);

        i += 16;
    }

    uint32_t sum = 0;

    sum += vaddvq_u32(res_vec0);
    sum += vaddvq_u32(res_vec1);
    sum += vaddvq_u32(res_vec2);
    sum += vaddvq_u32(res_vec3);

    // Process the last few elements manually
    int j = l;
    while (j < size)
    {
        sum += (uint32_t)(a[j]) * (uint32_t)(b[j]);
        j++;
    }

    *res = sum;
}
