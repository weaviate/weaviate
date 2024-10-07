#include <arm_neon.h>

void hamming_neon_byte_256(unsigned char *a, unsigned char *b,
                           unsigned int *res, long *len) {
  int size = *len;

  // Use the vectorized version for the first n - (n % 16) elements
  int l = size - (size % 16);

  uint32x4_t res_vec0 = vdupq_n_u32(0);
  uint32x4_t res_vec1 = vdupq_n_u32(0);
  uint32x4_t res_vec2 = vdupq_n_u32(0);
  uint32x4_t res_vec3 = vdupq_n_u32(0);

  int i = 0;

  // Load 4*16 bytes at a time
  while (i + 64 <= l) {
    uint8x16x4_t a4 = vld1q_u8_x4(a + i);
    uint8x16x4_t b4 = vld1q_u8_x4(b + i);

    res_vec0 -= vpaddlq_s16(
        vpaddlq_s8(vreinterpretq_s8_u8(vceqq_u8(a4.val[0], b4.val[0]))));
    res_vec1 -= vpaddlq_s16(
        vpaddlq_s8(vreinterpretq_s8_u8(vceqq_u8(a4.val[1], b4.val[1]))));
    res_vec2 -= vpaddlq_s16(
        vpaddlq_s8(vreinterpretq_s8_u8(vceqq_u8(a4.val[2], b4.val[2]))));
    res_vec3 -= vpaddlq_s16(
        vpaddlq_s8(vreinterpretq_s8_u8(vceqq_u8(a4.val[3], b4.val[3]))));

    i += 64;
  }

  // Process the remaining elements
  while (i < l) {

    uint8x16_t a_vec = vld1q_u8(a + i);
    uint8x16_t b_vec = vld1q_u8(b + i);
    res_vec0 -=
        vpaddlq_s16(vpaddlq_s8(vreinterpretq_s8_u8(vceqq_s8(a_vec, b_vec))));

    i += 16;
  }

  uint32_t sum = size;

  sum -= vaddvq_u32(res_vec0);
  sum -= vaddvq_u32(res_vec1);
  sum -= vaddvq_u32(res_vec2);
  sum -= vaddvq_u32(res_vec3);

  // Process the last few elements manually
  int j = l;
  while (j < size) {
    if (a[j] == b[j]) {
      sum--;
    }
    j++;
  }

  *res = sum;
}
