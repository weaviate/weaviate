
from peachpy import *
from peachpy.x86_64 import *

x = Argument(ptr(const_float_))
y = Argument(ptr(const_float_))
length = Argument(ptr(size_t))

# YMM - 256bit - 8 float32
# XMM - 128bit - 4 float32

with Function("DotProductAVX", (x, y, length), float_, target=uarch.default + isa.fma3) as function:
    reg_x = GeneralPurposeRegister64()
    reg_y = GeneralPurposeRegister64()
    reg_length = GeneralPurposeRegister64()

    LOAD.ARGUMENT(reg_x, x)
    LOAD.ARGUMENT(reg_y, y)
    LOAD.ARGUMENT(reg_length, length)

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

    vector_loop = Loop()
    JB(vector_loop.end)
    with vector_loop:
        VMOVUPS(ymm_x, [reg_x])
        VMOVUPS(ymm_y, [reg_y])
        VFMADD231PS(ymm_acc, ymm_x, ymm_y)
        ADD(reg_x, 32)
        ADD(reg_y, 32)

        VMOVUPS(ymm_x, [reg_x])
        VMOVUPS(ymm_y, [reg_y])
        VFMADD231PS(ymm_acc2, ymm_x, ymm_y)
        ADD(reg_x, 32)
        ADD(reg_y, 32)

        VMOVUPS(ymm_x, [reg_x])
        VMOVUPS(ymm_y, [reg_y])
        VFMADD231PS(ymm_acc3, ymm_x, ymm_y)
        ADD(reg_x, 32)
        ADD(reg_y, 32)

        VMOVUPS(ymm_x, [reg_x])
        VMOVUPS(ymm_y, [reg_y])
        VFMADD231PS(ymm_acc4, ymm_x, ymm_y)
        ADD(reg_x, 32)
        ADD(reg_y, 32)

        SUB([reg_length], 32)
        JAE(vector_loop.begin)

    VADDPS(ymm_acc, ymm_acc, ymm_acc2)
    VADDPS(ymm_acc3, ymm_acc3, ymm_acc4)
    VADDPS(ymm_acc, ymm_acc, ymm_acc3)

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
