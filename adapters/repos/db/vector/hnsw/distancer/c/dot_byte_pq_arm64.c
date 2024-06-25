#include <arm_neon.h>

/*void dot_byte_pq_arm64(unsigned char *a, unsigned char *b, unsigned int *res, long *len)*/
void dot_byte_pq_arm64(float *uncompressed, unsigned char *compressed, float *codebook, float *res, long *len_uncompressed, long *len_compressed)
{
    int size_uncompressed = *len_uncompressed;
    int size_compressed = *len_compressed;

    int segmentLen = size_uncompressed / size_compressed;

    *res = 0;
}
