
from peachpy import *
from peachpy.x86_64 import *

a_base = Argument(ptr(const_float_))
a_len = Argument(size_t)
x_cap = Argument(size_t)
b_base = Argument(ptr(const_float_))
b_len = Argument(size_t)
y_cap = Argument(size_t)

# YMM - 256bit - 8 float32
# XMM - 128bit - 4 float32

with Function("DotProductAVX", (a_base, a_len, x_cap, b_base, b_len, y_cap), float_, target=uarch.default + isa.fma3) as function:
    reg_x = GeneralPurposeRegister64()
    reg_y = GeneralPurposeRegister64()
    reg_length = GeneralPurposeRegister64()
    reg_length_y = GeneralPurposeRegister64()

    LOAD.ARGUMENT(reg_x, a_base)
    LOAD.ARGUMENT(reg_y, b_base)
    LOAD.ARGUMENT(reg_length, a_len)
    LOAD.ARGUMENT(reg_length_y, b_len)

    ymm_acc = YMMRegister()
    ymm_acc2 = YMMRegister()
    ymm_acc3 = YMMRegister()
    ymm_acc4 = YMMRegister()
    VXORPS(ymm_acc, ymm_acc, ymm_acc)
    VXORPS(ymm_acc2, ymm_acc2, ymm_acc2)
    VXORPS(ymm_acc3, ymm_acc3, ymm_acc3)
    VXORPS(ymm_acc4, ymm_acc4, ymm_acc4)

    ymm_x = YMMRegister()
    ymm_y = YMMRegister()

    index = GeneralPurposeRegister64()
    XOR(index, index)

    vector_loop = Loop()
    JB(vector_loop.end)
    with vector_loop:
        VMOVUPS(ymm_x, [reg_x])
        VMOVUPS(ymm_y, [reg_y])
        VFMADD231PS(ymm_acc, ymm_x, ymm_y)
        ADD(reg_x, 32)
        ADD(reg_y, 32)

        SUB(reg_length, 8)
        JAE(vector_loop.begin)

    # VADDPS(ymm_acc, ymm_acc, ymm_acc2)
    # VADDPS(ymm_acc3, ymm_acc3, ymm_acc4)
    # VADDPS(ymm_acc, ymm_acc, ymm_acc3)

    # VMULPS(ymm_acc, ymm_x, [reg_y])
    VHADDPS(ymm_acc, ymm_acc, ymm_acc)
    VHADDPS(ymm_acc, ymm_acc, ymm_acc)

    xmm_temp = XMMRegister()
    VEXTRACTF128(xmm_temp, ymm_acc, 1)

    xmm_acc = ymm_acc.as_xmm
    VADDPS(xmm_acc, xmm_acc, xmm_temp)

    RETURN(xmm_acc)


# a = b * c + a

# a1 = b1 & c1 +a1
# a2 = b2 & c2 +a2
