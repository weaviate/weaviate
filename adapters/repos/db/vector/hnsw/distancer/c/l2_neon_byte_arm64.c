#include <arm_neon.h>

void l2_neon_byte_256(unsigned char *a, unsigned char *b, unsigned int *res, long *len)
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

        int16x8_t diff_0_low, diff_0_high, diff_1_low, diff_1_high, diff_2_low, diff_2_high, diff_3_low, diff_3_high;

        // Compute differences and extend to signed 16-bit integers
        diff_0_low = vreinterpretq_s16_u16(vsubl_u8(vget_low_u8(a4.val[0]), vget_low_u8(b4.val[0])));
        diff_0_high = vreinterpretq_s16_u16(vsubl_u8(vget_high_u8(a4.val[0]), vget_high_u8(b4.val[0])));

        diff_1_low = vreinterpretq_s16_u16(vsubl_u8(vget_low_u8(a4.val[1]), vget_low_u8(b4.val[1])));
        diff_1_high = vreinterpretq_s16_u16(vsubl_u8(vget_high_u8(a4.val[1]), vget_high_u8(b4.val[1])));

        diff_2_low = vreinterpretq_s16_u16(vsubl_u8(vget_low_u8(a4.val[2]), vget_low_u8(b4.val[2])));
        diff_2_high = vreinterpretq_s16_u16(vsubl_u8(vget_high_u8(a4.val[2]), vget_high_u8(b4.val[2])));

        diff_3_low = vreinterpretq_s16_u16(vsubl_u8(vget_low_u8(a4.val[3]), vget_low_u8(b4.val[3])));
        diff_3_high = vreinterpretq_s16_u16(vsubl_u8(vget_high_u8(a4.val[3]), vget_high_u8(b4.val[3])));

        int32x4_t sq_0_low = vmull_s16(vget_low_s16(diff_0_low), vget_low_s16(diff_0_low));
        sq_0_low += vmull_s16(vget_high_s16(diff_0_low), vget_high_s16(diff_0_low));
        int32x4_t sq_0_high = vmull_s16(vget_low_s16(diff_0_high), vget_low_s16(diff_0_high));
        sq_0_high += vmull_s16(vget_high_s16(diff_0_high), vget_high_s16(diff_0_high));

        int32x4_t sq_1_low = vmull_s16(vget_low_s16(diff_1_low), vget_low_s16(diff_1_low));
        sq_1_low += vmull_s16(vget_high_s16(diff_1_low), vget_high_s16(diff_1_low));
        int32x4_t sq_1_high = vmull_s16(vget_low_s16(diff_1_high), vget_low_s16(diff_1_high));
        sq_1_high += vmull_s16(vget_high_s16(diff_1_high), vget_high_s16(diff_1_high));

        int32x4_t sq_2_low = vmull_s16(vget_low_s16(diff_2_low), vget_low_s16(diff_2_low));
        sq_2_low += vmull_s16(vget_high_s16(diff_2_low), vget_high_s16(diff_2_low));
        int32x4_t sq_2_high = vmull_s16(vget_low_s16(diff_2_high), vget_low_s16(diff_2_high));
        sq_2_high += vmull_s16(vget_high_s16(diff_2_high), vget_high_s16(diff_2_high));

        int32x4_t sq_3_low = vmull_s16(vget_low_s16(diff_3_low), vget_low_s16(diff_3_low));
        sq_3_low += vmull_s16(vget_high_s16(diff_3_low), vget_high_s16(diff_3_low));
        int32x4_t sq_3_high = vmull_s16(vget_low_s16(diff_3_high), vget_low_s16(diff_3_high));
        sq_3_high += vmull_s16(vget_high_s16(diff_3_high), vget_high_s16(diff_3_high));

        // convert to unsigned 32-bit ints (square is garantueed to be positive)
        res_vec0 += vreinterpretq_u32_s32(sq_0_low);
        res_vec0 += vreinterpretq_u32_s32(sq_0_high);

        res_vec1 += vreinterpretq_u32_s32(sq_1_low);
        res_vec1 += vreinterpretq_u32_s32(sq_1_high);

        res_vec2 += vreinterpretq_u32_s32(sq_2_low);
        res_vec2 += vreinterpretq_u32_s32(sq_2_high);

        res_vec3 += vreinterpretq_u32_s32(sq_3_low);
        res_vec3 += vreinterpretq_u32_s32(sq_3_high);

        i += 64;
    }

    // Process the remaining elements
    while (i < l)
    {
        uint8x16_t a_vec = vld1q_u8(a + i);
        uint8x16_t b_vec = vld1q_u8(b + i);

        int16x8_t diff_low = vreinterpretq_s16_u16(vsubl_u8(vget_low_u8(a_vec), vget_low_u8(b_vec)));
        int16x8_t diff_high = vreinterpretq_s16_u16(vsubl_u8(vget_high_u8(a_vec), vget_high_u8(b_vec)));

        int32x4_t sq_low = vmull_s16(vget_low_s16(diff_low), vget_low_s16(diff_low));
        sq_low += vmull_s16(vget_high_s16(diff_low), vget_high_s16(diff_low));

        int32x4_t sq_high = vmull_s16(vget_low_s16(diff_high), vget_low_s16(diff_high));
        sq_high += vmull_s16(vget_high_s16(diff_high), vget_high_s16(diff_high));

        res_vec0 += vreinterpretq_u32_s32(sq_low);
        res_vec0 += vreinterpretq_u32_s32(sq_high);

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
        int32_t diff = (int32_t)(a[j]) - (int32_t)(b[j]);
        sum += (uint32_t)(diff * diff);
        j++;
    }

    *res = sum;
}
