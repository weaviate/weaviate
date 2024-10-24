#include <arm_neon.h>

// Dot product between float and byte arrays
// Due to limitations with goat, it can only handle arrays of multiple of 16
// elements. i.e len % 16 == 0 and len >= 16

void laq_dot_exp_neon(float *x, unsigned char *y1, unsigned char *y2, float *a1,
                      float *a2, float *res, long *len) {
  int n = *len;
  float sum = 0;

  // Create 4*4 registers to store the result
  float32x4_t res_vec = vdupq_n_f32(0);

  float32x4_t a1_vec = vdupq_n_f32(*a1);
  float32x4_t a2_vec = vdupq_n_f32(*a2);

  // Vectorized loop
  while (n >= 16) {
    float32x4_t x_vec0 = vld1q_f32(x);
    float32x4_t x_vec1 = vld1q_f32(x + 4);
    float32x4_t x_vec2 = vld1q_f32(x + 8);
    float32x4_t x_vec3 = vld1q_f32(x + 12);

    uint8x16_t y1_vec0_u8 = vld1q_u8(y1);
    uint16x8_t y1_vec0_low_u16 = vmovl_u8(vget_low_u8(y1_vec0_u8));
    uint16x8_t y1_vec0_high_u16 = vmovl_u8(vget_high_u8(y1_vec0_u8));

    float32x4_t y1_vec0 =
        vcvtq_f32_u32(vmovl_u16(vget_low_u16(y1_vec0_low_u16)));
    float32x4_t y1_vec1 =
        vcvtq_f32_u32(vmovl_u16(vget_high_u16(y1_vec0_low_u16)));
    float32x4_t y1_vec2 =
        vcvtq_f32_u32(vmovl_u16(vget_low_u16(y1_vec0_high_u16)));
    float32x4_t y1_vec3 =
        vcvtq_f32_u32(vmovl_u16(vget_high_u16(y1_vec0_high_u16)));

    y1_vec0 = vmulq_f32(y1_vec0, a1_vec);
    y1_vec1 = vmulq_f32(y1_vec1, a1_vec);
    y1_vec2 = vmulq_f32(y1_vec2, a1_vec);
    y1_vec3 = vmulq_f32(y1_vec3, a1_vec);

    uint8x16_t y2_vec0_u8 = vld1q_u8(y2);
    uint16x8_t y2_vec0_low_u16 = vmovl_u8(vget_low_u8(y2_vec0_u8));
    uint16x8_t y2_vec0_high_u16 = vmovl_u8(vget_high_u8(y2_vec0_u8));

    float32x4_t y2_vec0 =
        vcvtq_f32_u32(vmovl_u16(vget_low_u16(y2_vec0_low_u16)));
    float32x4_t y2_vec1 =
        vcvtq_f32_u32(vmovl_u16(vget_high_u16(y2_vec0_low_u16)));
    float32x4_t y2_vec2 =
        vcvtq_f32_u32(vmovl_u16(vget_low_u16(y2_vec0_high_u16)));
    float32x4_t y2_vec3 =
        vcvtq_f32_u32(vmovl_u16(vget_high_u16(y2_vec0_high_u16)));

    y1_vec0 = vfmaq_f32(y1_vec0, y2_vec0, a2_vec);
    y1_vec1 = vfmaq_f32(y1_vec1, y2_vec1, a2_vec);
    y1_vec2 = vfmaq_f32(y1_vec2, y2_vec2, a2_vec);
    y1_vec3 = vfmaq_f32(y1_vec3, y2_vec3, a2_vec);

    res_vec = vfmaq_f32(res_vec, x_vec0, y1_vec0);
    // res_vec = vfmaq_f32(res_vec, x_vec0, y2_vec0);
    res_vec = vfmaq_f32(res_vec, x_vec1, y1_vec1);
    // res_vec = vfmaq_f32(res_vec, x_vec1, y2_vec1);
    res_vec = vfmaq_f32(res_vec, x_vec2, y1_vec2);
    // res_vec = vfmaq_f32(res_vec, x_vec2, y2_vec2);
    res_vec = vfmaq_f32(res_vec, x_vec3, y1_vec3);
    // res_vec = vfmaq_f32(res_vec, x_vec3, y2_vec3);
    // res_vec = vmlaq_f32(res_vec, x_vec1, y1plusy2_vec1);
    // res_vec = vmlaq_f32(res_vec, x_vec2, y1plusy2_vec2);
    // res_vec = vmlaq_f32(res_vec, x_vec3, y1plusy2_vec3);
    // multiple res_vec

    n -= 16;
    x += 16;
    y1 += 16;
    y2 += 16;
  }

  // Horizontal add
  float temp[4];
  vst1q_f32(temp, res_vec);
  sum += temp[0] + temp[1] + temp[2] + temp[3];

  *res = sum;
}
