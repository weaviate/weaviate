//go:build !noasm && amd64
// AUTO-GENERATED BY GOAT -- DO NOT EDIT

TEXT ·popcnt_AVX2_lookup(SB), $0-32
	MOVQ vec+0(FP), DI
	MOVQ low_mask_vec+8(FP), SI
	MOVQ lookup_vec+16(FP), DX

TEXT ·popcnt_64bit(SB), $0-32
	MOVQ src+0(FP), DI
	MOVQ popcnt_constants+8(FP), SI

TEXT ·popcnt_lookup_64bit(SB), $0-32
	MOVQ data+0(FP), DI
	MOVQ n+8(FP), SI
	MOVQ lookup64bit+16(FP), DX
	MOVQ popcnt_constants+24(FP), CX
	BYTE $0x55                       // pushq	%rbp
	WORD $0x8948; BYTE $0xe5         // movq	%rsp, %rbp
	WORD $0x5741                     // pushq	%r15
	WORD $0x5641                     // pushq	%r14
	WORD $0x5541                     // pushq	%r13
	WORD $0x5441                     // pushq	%r12
	BYTE $0x53                       // pushq	%rbx
	LONG $0xf0e48348                 // andq	$-16, %rsp
	LONG $0x60ec8348                 // subq	$96, %rsp
	WORD $0x8b4c; BYTE $0x06         // movq	(%rsi), %r8
	WORD $0x894d; BYTE $0xc1         // movq	%r8, %r9
	LONG $0xf9c18349                 // addq	$-7, %r9
	JE   LBB0_1
	WORD $0x8b4c; BYTE $0x29         // movq	(%rcx), %r13
	LONG $0x08518b4c                 // movq	8(%rcx), %r10
	LONG $0x10598b4c                 // movq	16(%rcx), %r11
	LONG $0x18618b4c                 // movq	24(%rcx), %r12
	LONG $0xf8708d4d                 // leaq	-8(%r8), %r14
	LONG $0x78fe8349                 // cmpq	$120, %r14
	JAE  LBB0_5
	WORD $0xc031                     // xorl	%eax, %eax
	WORD $0xc931                     // xorl	%ecx, %ecx
	JMP  LBB0_4

LBB0_1:
	WORD $0xc931 // xorl	%ecx, %ecx
	WORD $0xc031 // xorl	%eax, %eax
	JMP  LBB0_8

LBB0_5:
	LONG $0x03eec149                           // shrq	$3, %r14
	LONG $0x01c68349                           // addq	$1, %r14
	WORD $0x894d; BYTE $0xf7                   // movq	%r14, %r15
	LONG $0xf0e78349                           // andq	$-16, %r15
	QUAD $0x00000000fd0c8d4a                   // leaq	(,%r15,8), %rcx
	LONG $0x6ef9c1c4; BYTE $0xc5               // vmovq	%r13, %xmm0
	LONG $0x597d62c4; BYTE $0xe0               // vpbroadcastq	%xmm0, %ymm12
	LONG $0x6ef9c1c4; BYTE $0xca               // vmovq	%r10, %xmm1
	LONG $0x597d62c4; BYTE $0xe9               // vpbroadcastq	%xmm1, %ymm13
	LONG $0x6ef9c1c4; BYTE $0xd3               // vmovq	%r11, %xmm2
	LONG $0x597de2c4; BYTE $0xd2               // vpbroadcastq	%xmm2, %ymm2
	LONG $0x6ef9c1c4; BYTE $0xdc               // vmovq	%r12, %xmm3
	LONG $0x597de2c4; BYTE $0xdb               // vpbroadcastq	%xmm3, %ymm3
	LONG $0xc0878d48; WORD $0x0003; BYTE $0x00 // leaq	960(%rdi), %rax
	LONG $0xef3941c4; BYTE $0xc0               // vpxor	%xmm8, %xmm8, %xmm8
	WORD $0x894c; BYTE $0xfb                   // movq	%r15, %rbx
	LONG $0xef3141c4; BYTE $0xc9               // vpxor	%xmm9, %xmm9, %xmm9
	LONG $0xef2941c4; BYTE $0xd2               // vpxor	%xmm10, %xmm10, %xmm10
	LONG $0xef2141c4; BYTE $0xdb               // vpxor	%xmm11, %xmm11, %xmm11

LBB0_6:
	QUAD $0xfffffd00a07efac5       // vmovq	-768(%rax), %xmm4               # xmm4 = mem[0],zero
	QUAD $0xfffffcc0a87efac5       // vmovq	-832(%rax), %xmm5               # xmm5 = mem[0],zero
	LONG $0xe46cd1c5               // vpunpcklqdq	%xmm4, %xmm5, %xmm4     # xmm4 = xmm5[0],xmm4[0]
	QUAD $0xfffffc80a87efac5       // vmovq	-896(%rax), %xmm5               # xmm5 = mem[0],zero
	QUAD $0xfffffc40b07efac5       // vmovq	-960(%rax), %xmm6               # xmm6 = mem[0],zero
	LONG $0xed6cc9c5               // vpunpcklqdq	%xmm5, %xmm6, %xmm5     # xmm5 = xmm6[0],xmm5[0]
	LONG $0x3855e3c4; WORD $0x01e4 // vinserti128	$1, %xmm4, %ymm5, %ymm4
	QUAD $0xfffffe00a87efac5       // vmovq	-512(%rax), %xmm5               # xmm5 = mem[0],zero
	QUAD $0xfffffdc0b07efac5       // vmovq	-576(%rax), %xmm6               # xmm6 = mem[0],zero
	LONG $0xed6cc9c5               // vpunpcklqdq	%xmm5, %xmm6, %xmm5     # xmm5 = xmm6[0],xmm5[0]
	QUAD $0xfffffd80b07efac5       // vmovq	-640(%rax), %xmm6               # xmm6 = mem[0],zero
	QUAD $0xfffffd40b87efac5       // vmovq	-704(%rax), %xmm7               # xmm7 = mem[0],zero
	LONG $0xf66cc1c5               // vpunpcklqdq	%xmm6, %xmm7, %xmm6     # xmm6 = xmm7[0],xmm6[0]
	LONG $0x384de3c4; WORD $0x01ed // vinserti128	$1, %xmm5, %ymm6, %ymm5
	QUAD $0xffffff00b07efac5       // vmovq	-256(%rax), %xmm6               # xmm6 = mem[0],zero
	QUAD $0xfffffec0b87efac5       // vmovq	-320(%rax), %xmm7               # xmm7 = mem[0],zero
	LONG $0xf66cc1c5               // vpunpcklqdq	%xmm6, %xmm7, %xmm6     # xmm6 = xmm7[0],xmm6[0]
	QUAD $0xfffffe80b87efac5       // vmovq	-384(%rax), %xmm7               # xmm7 = mem[0],zero
	QUAD $0xfffffe40807efac5       // vmovq	-448(%rax), %xmm0               # xmm0 = mem[0],zero
	LONG $0xc76cf9c5               // vpunpcklqdq	%xmm7, %xmm0, %xmm0     # xmm0 = xmm0[0],xmm7[0]
	LONG $0x387de3c4; WORD $0x01c6 // vinserti128	$1, %xmm6, %ymm0, %ymm0
	LONG $0x307efac5               // vmovq	(%rax), %xmm6                   # xmm6 = mem[0],zero
	LONG $0x787efac5; BYTE $0xc0   // vmovq	-64(%rax), %xmm7                # xmm7 = mem[0],zero
	LONG $0xf66cc1c5               // vpunpcklqdq	%xmm6, %xmm7, %xmm6     # xmm6 = xmm7[0],xmm6[0]
	LONG $0x787efac5; BYTE $0x80   // vmovq	-128(%rax), %xmm7               # xmm7 = mem[0],zero
	QUAD $0xffffff40887efac5       // vmovq	-192(%rax), %xmm1               # xmm1 = mem[0],zero
	LONG $0xcf6cf1c5               // vpunpcklqdq	%xmm7, %xmm1, %xmm1     # xmm1 = xmm1[0],xmm7[0]
	LONG $0x3875e3c4; WORD $0x01ce // vinserti128	$1, %xmm6, %ymm1, %ymm1
	LONG $0xf4db9dc5               // vpand	%ymm4, %ymm12, %ymm6
	LONG $0xfddb9dc5               // vpand	%ymm5, %ymm12, %ymm7
	LONG $0xf0db1dc5               // vpand	%ymm0, %ymm12, %ymm14
	LONG $0xf9db1dc5               // vpand	%ymm1, %ymm12, %ymm15
	LONG $0xd473ddc5; BYTE $0x01   // vpsrlq	$1, %ymm4, %ymm4
	LONG $0xd573d5c5; BYTE $0x01   // vpsrlq	$1, %ymm5, %ymm5
	LONG $0xd073fdc5; BYTE $0x01   // vpsrlq	$1, %ymm0, %ymm0
	LONG $0xd173f5c5; BYTE $0x01   // vpsrlq	$1, %ymm1, %ymm1
	LONG $0xe4db9dc5               // vpand	%ymm4, %ymm12, %ymm4
	LONG $0xe4d4cdc5               // vpaddq	%ymm4, %ymm6, %ymm4
	LONG $0xeddb9dc5               // vpand	%ymm5, %ymm12, %ymm5
	LONG $0xedd4c5c5               // vpaddq	%ymm5, %ymm7, %ymm5
	LONG $0xc0db9dc5               // vpand	%ymm0, %ymm12, %ymm0
	LONG $0xc0d48dc5               // vpaddq	%ymm0, %ymm14, %ymm0
	LONG $0xc9db9dc5               // vpand	%ymm1, %ymm12, %ymm1
	LONG $0xc9d485c5               // vpaddq	%ymm1, %ymm15, %ymm1
	LONG $0xf4db95c5               // vpand	%ymm4, %ymm13, %ymm6
	LONG $0xfddb95c5               // vpand	%ymm5, %ymm13, %ymm7
	LONG $0xf0db15c5               // vpand	%ymm0, %ymm13, %ymm14
	LONG $0xf9db15c5               // vpand	%ymm1, %ymm13, %ymm15
	LONG $0xd473ddc5; BYTE $0x02   // vpsrlq	$2, %ymm4, %ymm4
	LONG $0xd573d5c5; BYTE $0x02   // vpsrlq	$2, %ymm5, %ymm5
	LONG $0xd073fdc5; BYTE $0x02   // vpsrlq	$2, %ymm0, %ymm0
	LONG $0xd173f5c5; BYTE $0x02   // vpsrlq	$2, %ymm1, %ymm1
	LONG $0xe4db95c5               // vpand	%ymm4, %ymm13, %ymm4
	LONG $0xe6d4ddc5               // vpaddq	%ymm6, %ymm4, %ymm4
	LONG $0xeddb95c5               // vpand	%ymm5, %ymm13, %ymm5
	LONG $0xefd4d5c5               // vpaddq	%ymm7, %ymm5, %ymm5
	LONG $0xc0db95c5               // vpand	%ymm0, %ymm13, %ymm0
	LONG $0xc0d48dc5               // vpaddq	%ymm0, %ymm14, %ymm0
	LONG $0xc9db95c5               // vpand	%ymm1, %ymm13, %ymm1
	LONG $0xc9d485c5               // vpaddq	%ymm1, %ymm15, %ymm1
	LONG $0xf2dbddc5               // vpand	%ymm2, %ymm4, %ymm6
	LONG $0xfadbd5c5               // vpand	%ymm2, %ymm5, %ymm7
	LONG $0xf2db7dc5               // vpand	%ymm2, %ymm0, %ymm14
	LONG $0xfadb75c5               // vpand	%ymm2, %ymm1, %ymm15
	LONG $0xd473ddc5; BYTE $0x04   // vpsrlq	$4, %ymm4, %ymm4
	LONG $0xd573d5c5; BYTE $0x04   // vpsrlq	$4, %ymm5, %ymm5
	LONG $0xd073fdc5; BYTE $0x04   // vpsrlq	$4, %ymm0, %ymm0
	LONG $0xd173f5c5; BYTE $0x04   // vpsrlq	$4, %ymm1, %ymm1
	LONG $0xe2dbddc5               // vpand	%ymm2, %ymm4, %ymm4
	LONG $0xe6d4ddc5               // vpaddq	%ymm6, %ymm4, %ymm4
	LONG $0xeadbd5c5               // vpand	%ymm2, %ymm5, %ymm5
	LONG $0xefd4d5c5               // vpaddq	%ymm7, %ymm5, %ymm5
	LONG $0xc2dbfdc5               // vpand	%ymm2, %ymm0, %ymm0
	LONG $0xc0d48dc5               // vpaddq	%ymm0, %ymm14, %ymm0
	LONG $0xcadbf5c5               // vpand	%ymm2, %ymm1, %ymm1
	LONG $0xc9d485c5               // vpaddq	%ymm1, %ymm15, %ymm1
	LONG $0xd373cdc5; BYTE $0x20   // vpsrlq	$32, %ymm3, %ymm6
	LONG $0xfef4ddc5               // vpmuludq	%ymm6, %ymm4, %ymm7
	LONG $0xd4738dc5; BYTE $0x20   // vpsrlq	$32, %ymm4, %ymm14
	LONG $0xf3f40dc5               // vpmuludq	%ymm3, %ymm14, %ymm14
	LONG $0xffd48dc5               // vpaddq	%ymm7, %ymm14, %ymm7
	LONG $0xf773c5c5; BYTE $0x20   // vpsllq	$32, %ymm7, %ymm7
	LONG $0xe3f4ddc5               // vpmuludq	%ymm3, %ymm4, %ymm4
	LONG $0xe7d4ddc5               // vpaddq	%ymm7, %ymm4, %ymm4
	LONG $0xfef4d5c5               // vpmuludq	%ymm6, %ymm5, %ymm7
	LONG $0xd5738dc5; BYTE $0x20   // vpsrlq	$32, %ymm5, %ymm14
	LONG $0xf3f40dc5               // vpmuludq	%ymm3, %ymm14, %ymm14
	LONG $0xffd48dc5               // vpaddq	%ymm7, %ymm14, %ymm7
	LONG $0xf773c5c5; BYTE $0x20   // vpsllq	$32, %ymm7, %ymm7
	LONG $0xebf4d5c5               // vpmuludq	%ymm3, %ymm5, %ymm5
	LONG $0xefd4d5c5               // vpaddq	%ymm7, %ymm5, %ymm5
	LONG $0xfef4fdc5               // vpmuludq	%ymm6, %ymm0, %ymm7
	LONG $0xd0738dc5; BYTE $0x20   // vpsrlq	$32, %ymm0, %ymm14
	LONG $0xf3f40dc5               // vpmuludq	%ymm3, %ymm14, %ymm14
	LONG $0xffd48dc5               // vpaddq	%ymm7, %ymm14, %ymm7
	LONG $0xf773c5c5; BYTE $0x20   // vpsllq	$32, %ymm7, %ymm7
	LONG $0xc3f4fdc5               // vpmuludq	%ymm3, %ymm0, %ymm0
	LONG $0xc7d4fdc5               // vpaddq	%ymm7, %ymm0, %ymm0
	LONG $0xf6f4f5c5               // vpmuludq	%ymm6, %ymm1, %ymm6
	LONG $0xd173c5c5; BYTE $0x20   // vpsrlq	$32, %ymm1, %ymm7
	LONG $0xfbf4c5c5               // vpmuludq	%ymm3, %ymm7, %ymm7
	LONG $0xf7d4cdc5               // vpaddq	%ymm7, %ymm6, %ymm6
	LONG $0xf673cdc5; BYTE $0x20   // vpsllq	$32, %ymm6, %ymm6
	LONG $0xcbf4f5c5               // vpmuludq	%ymm3, %ymm1, %ymm1
	LONG $0xced4f5c5               // vpaddq	%ymm6, %ymm1, %ymm1
	LONG $0xd473ddc5; BYTE $0x38   // vpsrlq	$56, %ymm4, %ymm4
	LONG $0xc4d43dc5               // vpaddq	%ymm4, %ymm8, %ymm8
	LONG $0xd573ddc5; BYTE $0x38   // vpsrlq	$56, %ymm5, %ymm4
	LONG $0xccd435c5               // vpaddq	%ymm4, %ymm9, %ymm9
	LONG $0xd073fdc5; BYTE $0x38   // vpsrlq	$56, %ymm0, %ymm0
	LONG $0xd0d42dc5               // vpaddq	%ymm0, %ymm10, %ymm10
	LONG $0xd173fdc5; BYTE $0x38   // vpsrlq	$56, %ymm1, %ymm0
	LONG $0xd8d425c5               // vpaddq	%ymm0, %ymm11, %ymm11
	LONG $0x04000548; WORD $0x0000 // addq	$1024, %rax                     # imm = 0x400
	LONG $0xf0c38348               // addq	$-16, %rbx
	JNE  LBB0_6
	LONG $0xd435c1c4; BYTE $0xc0   // vpaddq	%ymm8, %ymm9, %ymm0
	LONG $0xc0d4adc5               // vpaddq	%ymm0, %ymm10, %ymm0
	LONG $0xc0d4a5c5               // vpaddq	%ymm0, %ymm11, %ymm0
	LONG $0x397de3c4; WORD $0x01c1 // vextracti128	$1, %ymm0, %xmm1
	LONG $0xc1d4f9c5               // vpaddq	%xmm1, %xmm0, %xmm0
	LONG $0xc870f9c5; BYTE $0xee   // vpshufd	$238, %xmm0, %xmm1              # xmm1 = xmm0[2,3,2,3]
	LONG $0xc1d4f9c5               // vpaddq	%xmm1, %xmm0, %xmm0
	LONG $0x7ef9e1c4; BYTE $0xc0   // vmovq	%xmm0, %rax
	WORD $0x394d; BYTE $0xfe       // cmpq	%r15, %r14
	JE   LBB0_8

LBB0_4:
	LONG $0xcf1c8b48         // movq	(%rdi,%rcx,8), %rbx
	WORD $0x894c; BYTE $0xee // movq	%r13, %rsi
	WORD $0x2148; BYTE $0xde // andq	%rbx, %rsi
	WORD $0xd148; BYTE $0xeb // shrq	%rbx
	WORD $0x214c; BYTE $0xeb // andq	%r13, %rbx
	WORD $0x0148; BYTE $0xf3 // addq	%rsi, %rbx
	WORD $0x8948; BYTE $0xde // movq	%rbx, %rsi
	WORD $0x214c; BYTE $0xd6 // andq	%r10, %rsi
	LONG $0x02ebc148         // shrq	$2, %rbx
	WORD $0x214c; BYTE $0xd3 // andq	%r10, %rbx
	WORD $0x0148; BYTE $0xf3 // addq	%rsi, %rbx
	WORD $0x8948; BYTE $0xde // movq	%rbx, %rsi
	WORD $0x214c; BYTE $0xde // andq	%r11, %rsi
	LONG $0x04ebc148         // shrq	$4, %rbx
	WORD $0x214c; BYTE $0xdb // andq	%r11, %rbx
	WORD $0x0148; BYTE $0xf3 // addq	%rsi, %rbx
	LONG $0xdcaf0f49         // imulq	%r12, %rbx
	LONG $0x38ebc148         // shrq	$56, %rbx
	WORD $0x0148; BYTE $0xd8 // addq	%rbx, %rax
	LONG $0x08c18348         // addq	$8, %rcx
	WORD $0x394c; BYTE $0xc9 // cmpq	%r9, %rcx
	JB   LBB0_4

LBB0_8:
	WORD $0x894d; BYTE $0xc1     // movq	%r8, %r9
	WORD $0x2949; BYTE $0xc9     // subq	%rcx, %r9
	JBE  LBB0_14
	LONG $0x10f98349             // cmpq	$16, %r9
	JB   LBB0_13
	WORD $0x894d; BYTE $0xca     // movq	%r9, %r10
	LONG $0xf0e28349             // andq	$-16, %r10
	LONG $0x6ef961c4; BYTE $0xe0 // vmovq	%rax, %xmm12
	LONG $0x39048d48             // leaq	(%rcx,%rdi), %rax
	LONG $0x0fc08348             // addq	$15, %rax
	WORD $0x014c; BYTE $0xd1     // addq	%r10, %rcx
	LONG $0xef0941c4; BYTE $0xf6 // vpxor	%xmm14, %xmm14, %xmm14
	WORD $0xdb31                 // xorl	%ebx, %ebx
	LONG $0xd2efe9c5             // vpxor	%xmm2, %xmm2, %xmm2
	LONG $0xdbefe1c5             // vpxor	%xmm3, %xmm3, %xmm3

LBB0_11:
	LONG $0x1874b60f; BYTE $0xf4   // movzbl	-12(%rax,%rbx), %esi
	LONG $0x1c7e7ac5; BYTE $0xf2   // vmovq	(%rdx,%rsi,8), %xmm11           # xmm11 = mem[0],zero
	LONG $0x1874b60f; BYTE $0xf3   // movzbl	-13(%rax,%rbx), %esi
	LONG $0x2c7e7ac5; BYTE $0xf2   // vmovq	(%rdx,%rsi,8), %xmm13           # xmm13 = mem[0],zero
	LONG $0x1874b60f; BYTE $0xf2   // movzbl	-14(%rax,%rbx), %esi
	LONG $0x0410fbc5; BYTE $0xf2   // vmovsd	(%rdx,%rsi,8), %xmm0            # xmm0 = mem[0],zero
	LONG $0x4429f8c5; WORD $0x3024 // vmovaps	%xmm0, 48(%rsp)                 # 16-byte Spill
	LONG $0x1874b60f; BYTE $0xf1   // movzbl	-15(%rax,%rbx), %esi
	LONG $0x0410fbc5; BYTE $0xf2   // vmovsd	(%rdx,%rsi,8), %xmm0            # xmm0 = mem[0],zero
	LONG $0x4429f8c5; WORD $0x4024 // vmovaps	%xmm0, 64(%rsp)                 # 16-byte Spill
	LONG $0x1874b60f; BYTE $0xf8   // movzbl	-8(%rax,%rbx), %esi
	LONG $0x0410fbc5; BYTE $0xf2   // vmovsd	(%rdx,%rsi,8), %xmm0            # xmm0 = mem[0],zero
	LONG $0x4429f8c5; WORD $0x2024 // vmovaps	%xmm0, 32(%rsp)                 # 16-byte Spill
	LONG $0x1874b60f; BYTE $0xf7   // movzbl	-9(%rax,%rbx), %esi
	LONG $0x0410fbc5; BYTE $0xf2   // vmovsd	(%rdx,%rsi,8), %xmm0            # xmm0 = mem[0],zero
	LONG $0x4429f8c5; WORD $0x1024 // vmovaps	%xmm0, 16(%rsp)                 # 16-byte Spill
	LONG $0x1874b60f; BYTE $0xf6   // movzbl	-10(%rax,%rbx), %esi
	LONG $0x0410fbc5; BYTE $0xf2   // vmovsd	(%rdx,%rsi,8), %xmm0            # xmm0 = mem[0],zero
	LONG $0x0429f8c5; BYTE $0x24   // vmovaps	%xmm0, (%rsp)                   # 16-byte Spill
	LONG $0x1874b60f; BYTE $0xf5   // movzbl	-11(%rax,%rbx), %esi
	LONG $0x3c7e7ac5; BYTE $0xf2   // vmovq	(%rdx,%rsi,8), %xmm15           # xmm15 = mem[0],zero
	LONG $0x1874b60f; BYTE $0xfc   // movzbl	-4(%rax,%rbx), %esi
	LONG $0x247efac5; BYTE $0xf2   // vmovq	(%rdx,%rsi,8), %xmm4            # xmm4 = mem[0],zero
	LONG $0x1874b60f; BYTE $0xfb   // movzbl	-5(%rax,%rbx), %esi
	LONG $0x2c7efac5; BYTE $0xf2   // vmovq	(%rdx,%rsi,8), %xmm5            # xmm5 = mem[0],zero
	LONG $0x1874b60f; BYTE $0xfa   // movzbl	-6(%rax,%rbx), %esi
	LONG $0x347efac5; BYTE $0xf2   // vmovq	(%rdx,%rsi,8), %xmm6            # xmm6 = mem[0],zero
	LONG $0x1874b60f; BYTE $0xf9   // movzbl	-7(%rax,%rbx), %esi
	LONG $0x3c7efac5; BYTE $0xf2   // vmovq	(%rdx,%rsi,8), %xmm7            # xmm7 = mem[0],zero
	LONG $0x1834b60f               // movzbl	(%rax,%rbx), %esi
	LONG $0x047efac5; BYTE $0xf2   // vmovq	(%rdx,%rsi,8), %xmm0            # xmm0 = mem[0],zero
	LONG $0x1874b60f; BYTE $0xff   // movzbl	-1(%rax,%rbx), %esi
	LONG $0x047e7ac5; BYTE $0xf2   // vmovq	(%rdx,%rsi,8), %xmm8            # xmm8 = mem[0],zero
	LONG $0x1874b60f; BYTE $0xfe   // movzbl	-2(%rax,%rbx), %esi
	LONG $0x0c7e7ac5; BYTE $0xf2   // vmovq	(%rdx,%rsi,8), %xmm9            # xmm9 = mem[0],zero
	LONG $0x1874b60f; BYTE $0xfd   // movzbl	-3(%rax,%rbx), %esi
	LONG $0x147e7ac5; BYTE $0xf2   // vmovq	(%rdx,%rsi,8), %xmm10           # xmm10 = mem[0],zero
	LONG $0x6c1141c4; BYTE $0xdb   // vpunpcklqdq	%xmm11, %xmm13, %xmm11  # xmm11 = xmm13[0],xmm11[0]
	LONG $0x4c6ff9c5; WORD $0x4024 // vmovdqa	64(%rsp), %xmm1                 # 16-byte Reload
	LONG $0x6c6c71c5; WORD $0x3024 // vpunpcklqdq	48(%rsp), %xmm1, %xmm13 # 16-byte Folded Reload
	LONG $0x381543c4; WORD $0x01db // vinserti128	$1, %xmm11, %ymm13, %ymm11
	LONG $0xd42541c4; BYTE $0xe4   // vpaddq	%ymm12, %ymm11, %ymm12
	LONG $0x4c6ff9c5; WORD $0x1024 // vmovdqa	16(%rsp), %xmm1                 # 16-byte Reload
	LONG $0x4c6cf1c5; WORD $0x2024 // vpunpcklqdq	32(%rsp), %xmm1, %xmm1  # 16-byte Folded Reload
	LONG $0x1c6c01c5; BYTE $0x24   // vpunpcklqdq	(%rsp), %xmm15, %xmm11  # 16-byte Folded Reload
	LONG $0x3825e3c4; WORD $0x01c9 // vinserti128	$1, %xmm1, %ymm11, %ymm1
	LONG $0xf1d40dc5               // vpaddq	%ymm1, %ymm14, %ymm14
	LONG $0xcc6cd1c5               // vpunpcklqdq	%xmm4, %xmm5, %xmm1     # xmm1 = xmm5[0],xmm4[0]
	LONG $0xe66cc1c5               // vpunpcklqdq	%xmm6, %xmm7, %xmm4     # xmm4 = xmm7[0],xmm6[0]
	LONG $0x385de3c4; WORD $0x01c9 // vinserti128	$1, %xmm1, %ymm4, %ymm1
	LONG $0xd2d4f5c5               // vpaddq	%ymm2, %ymm1, %ymm2
	LONG $0xc06cb9c5               // vpunpcklqdq	%xmm0, %xmm8, %xmm0     # xmm0 = xmm8[0],xmm0[0]
	LONG $0x6c29c1c4; BYTE $0xc9   // vpunpcklqdq	%xmm9, %xmm10, %xmm1    # xmm1 = xmm10[0],xmm9[0]
	LONG $0x3875e3c4; WORD $0x01c0 // vinserti128	$1, %xmm0, %ymm1, %ymm0
	LONG $0xdbd4fdc5               // vpaddq	%ymm3, %ymm0, %ymm3
	LONG $0x10c38348               // addq	$16, %rbx
	WORD $0x3949; BYTE $0xda       // cmpq	%rbx, %r10
	JNE  LBB0_11
	LONG $0xd40dc1c4; BYTE $0xc4   // vpaddq	%ymm12, %ymm14, %ymm0
	LONG $0xc0d4edc5               // vpaddq	%ymm0, %ymm2, %ymm0
	LONG $0xc0d4e5c5               // vpaddq	%ymm0, %ymm3, %ymm0
	LONG $0x397de3c4; WORD $0x01c1 // vextracti128	$1, %ymm0, %xmm1
	LONG $0xc1d4f9c5               // vpaddq	%xmm1, %xmm0, %xmm0
	LONG $0xc870f9c5; BYTE $0xee   // vpshufd	$238, %xmm0, %xmm1              # xmm1 = xmm0[2,3,2,3]
	LONG $0xc1d4f9c5               // vpaddq	%xmm1, %xmm0, %xmm0
	LONG $0x7ef9e1c4; BYTE $0xc0   // vmovq	%xmm0, %rax
	WORD $0x394d; BYTE $0xd1       // cmpq	%r10, %r9
	JE   LBB0_14

LBB0_13:
	LONG $0x0f34b60f         // movzbl	(%rdi,%rcx), %esi
	LONG $0xf2040348         // addq	(%rdx,%rsi,8), %rax
	LONG $0x01c18348         // addq	$1, %rcx
	WORD $0x3949; BYTE $0xc8 // cmpq	%rcx, %r8
	JNE  LBB0_13

LBB0_14:
	LONG $0xd8658d48         // leaq	-40(%rbp), %rsp
	BYTE $0x5b               // popq	%rbx
	WORD $0x5c41             // popq	%r12
	WORD $0x5d41             // popq	%r13
	WORD $0x5e41             // popq	%r14
	WORD $0x5f41             // popq	%r15
	BYTE $0x5d               // popq	%rbp
	WORD $0xf8c5; BYTE $0x77 // vzeroupper
	BYTE $0xc3               // retq

TEXT ·popcount(SB), $0-32
	MOVQ x+0(FP), DI
	MOVQ lookup64bit+8(FP), SI
	BYTE $0x55                 // pushq	%rbp
	WORD $0x8948; BYTE $0xe5   // movq	%rsp, %rbp
	LONG $0xf8e48348           // andq	$-8, %rsp
	WORD $0xb60f; BYTE $0x0f   // movzbl	(%rdi), %ecx
	LONG $0x0147b60f           // movzbl	1(%rdi), %eax
	LONG $0xc6048b48           // movq	(%rsi,%rax,8), %rax
	LONG $0xce040348           // addq	(%rsi,%rcx,8), %rax
	LONG $0x024fb60f           // movzbl	2(%rdi), %ecx
	LONG $0xce040348           // addq	(%rsi,%rcx,8), %rax
	LONG $0x034fb60f           // movzbl	3(%rdi), %ecx
	LONG $0xce040348           // addq	(%rsi,%rcx,8), %rax
	LONG $0x044fb60f           // movzbl	4(%rdi), %ecx
	LONG $0xce040348           // addq	(%rsi,%rcx,8), %rax
	LONG $0x054fb60f           // movzbl	5(%rdi), %ecx
	LONG $0xce040348           // addq	(%rsi,%rcx,8), %rax
	LONG $0x064fb60f           // movzbl	6(%rdi), %ecx
	LONG $0xce040348           // addq	(%rsi,%rcx,8), %rax
	LONG $0x074fb60f           // movzbl	7(%rdi), %ecx
	LONG $0xce040348           // addq	(%rsi,%rcx,8), %rax
	WORD $0x8948; BYTE $0xec   // movq	%rbp, %rsp
	BYTE $0x5d                 // popq	%rbp
	BYTE $0xc3                 // retq

TEXT ·hamming_bitwise_256(SB), $0-32
	MOVQ a+0(FP), DI
	MOVQ b+8(FP), SI
	MOVQ res+16(FP), DX
	MOVQ len+24(FP), CX
	MOVQ lookup64bit+32(FP), R8
	MOVQ popcnt_constants+40(FP), R9
	BYTE $0x55                       // pushq	%rbp
	WORD $0x8948; BYTE $0xe5         // movq	%rsp, %rbp
	WORD $0x5741                     // pushq	%r15
	WORD $0x5641                     // pushq	%r14
	WORD $0x5541                     // pushq	%r13
	WORD $0x5441                     // pushq	%r12
	BYTE $0x53                       // pushq	%rbx
	LONG $0xf8e48348                 // andq	$-8, %rsp
	WORD $0x8b4c; BYTE $0x29         // movq	(%rcx), %r13
	LONG $0x08fd8341                 // cmpl	$8, %r13d
	JGE  LBB2_1
	WORD $0x3145; BYTE $0xc9         // xorl	%r9d, %r9d
	WORD $0x3145; BYTE $0xff         // xorl	%r15d, %r15d

LBB2_11:
	LONG $0xce0c8b4a               // movq	(%rsi,%r9,8), %rcx
	LONG $0xcf0c334a               // xorq	(%rdi,%r9,8), %rcx
	WORD $0xb60f; BYTE $0xd9       // movzbl	%cl, %ebx
	LONG $0xd83c034d               // addq	(%r8,%rbx,8), %r15
	WORD $0xb60f; BYTE $0xdd       // movzbl	%ch, %ebx
	LONG $0xd83c034d               // addq	(%r8,%rbx,8), %r15
	WORD $0x8948; BYTE $0xc8       // movq	%rcx, %rax
	LONG $0x0de8c148               // shrq	$13, %rax
	LONG $0x0007f825; BYTE $0x00   // andl	$2040, %eax                     # imm = 0x7F8
	LONG $0x003c034d               // addq	(%r8,%rax), %r15
	WORD $0x8948; BYTE $0xc8       // movq	%rcx, %rax
	LONG $0x15e8c148               // shrq	$21, %rax
	LONG $0x0007f825; BYTE $0x00   // andl	$2040, %eax                     # imm = 0x7F8
	LONG $0x003c034d               // addq	(%r8,%rax), %r15
	WORD $0x8948; BYTE $0xc8       // movq	%rcx, %rax
	LONG $0x1de8c148               // shrq	$29, %rax
	LONG $0x0007f825; BYTE $0x00   // andl	$2040, %eax                     # imm = 0x7F8
	LONG $0x003c034d               // addq	(%r8,%rax), %r15
	WORD $0x8948; BYTE $0xc8       // movq	%rcx, %rax
	LONG $0x25e8c148               // shrq	$37, %rax
	LONG $0x0007f825; BYTE $0x00   // andl	$2040, %eax                     # imm = 0x7F8
	LONG $0x003c034d               // addq	(%r8,%rax), %r15
	WORD $0x8948; BYTE $0xc8       // movq	%rcx, %rax
	LONG $0x2de9c148               // shrq	$45, %rcx
	LONG $0x07f8e181; WORD $0x0000 // andl	$2040, %ecx                     # imm = 0x7F8
	LONG $0x083c034d               // addq	(%r8,%rcx), %r15
	LONG $0x38e8c148               // shrq	$56, %rax
	LONG $0xc03c034d               // addq	(%r8,%rax,8), %r15
	LONG $0x01c18349               // addq	$1, %r9
	WORD $0x3945; BYTE $0xcd       // cmpl	%r9d, %r13d
	JNE  LBB2_11
	JMP  LBB2_17

LBB2_1:
	LONG $0x6f7ec1c4; BYTE $0x00   // vmovdqu	(%r8), %ymm0
	LONG $0x597dc2c4; WORD $0x2049 // vpbroadcastq	32(%r9), %ymm1
	WORD $0x3145; BYTE $0xff       // xorl	%r15d, %r15d
	LONG $0x10fd8341               // cmpl	$16, %r13d
	JB   LBB2_5
	LONG $0xd2efe9c5               // vpxor	%xmm2, %xmm2, %xmm2

LBB2_3:
	LONG $0x1e6ffec5               // vmovdqu	(%rsi), %ymm3
	LONG $0x666ffec5; BYTE $0x20   // vmovdqu	32(%rsi), %ymm4
	LONG $0x6e6ffec5; BYTE $0x40   // vmovdqu	64(%rsi), %ymm5
	LONG $0x766ffec5; BYTE $0x60   // vmovdqu	96(%rsi), %ymm6
	LONG $0x3fefe5c5               // vpxor	(%rdi), %ymm3, %ymm7
	LONG $0x47ef5dc5; BYTE $0x20   // vpxor	32(%rdi), %ymm4, %ymm8
	LONG $0x67efd5c5; BYTE $0x40   // vpxor	64(%rdi), %ymm5, %ymm4
	LONG $0x5fefcdc5; BYTE $0x60   // vpxor	96(%rdi), %ymm6, %ymm3
	LONG $0xe9dbc5c5               // vpand	%ymm1, %ymm7, %ymm5
	LONG $0xd771cdc5; BYTE $0x04   // vpsrlw	$4, %ymm7, %ymm6
	LONG $0xf6dbf5c5               // vpand	%ymm6, %ymm1, %ymm6
	LONG $0x007de2c4; BYTE $0xed   // vpshufb	%ymm5, %ymm0, %ymm5
	LONG $0x007de2c4; BYTE $0xf6   // vpshufb	%ymm6, %ymm0, %ymm6
	LONG $0xf5fccdc5               // vpaddb	%ymm5, %ymm6, %ymm6
	LONG $0xe9dbbdc5               // vpand	%ymm1, %ymm8, %ymm5
	LONG $0x7145c1c4; WORD $0x04d0 // vpsrlw	$4, %ymm8, %ymm7
	LONG $0xffdbf5c5               // vpand	%ymm7, %ymm1, %ymm7
	LONG $0x007de2c4; BYTE $0xed   // vpshufb	%ymm5, %ymm0, %ymm5
	LONG $0x007de2c4; BYTE $0xff   // vpshufb	%ymm7, %ymm0, %ymm7
	LONG $0xedfcc5c5               // vpaddb	%ymm5, %ymm7, %ymm5
	LONG $0xeaf6d5c5               // vpsadbw	%ymm2, %ymm5, %ymm5
	LONG $0x16f9e3c4; WORD $0x01e8 // vpextrq	$1, %xmm5, %rax
	LONG $0xf2f6cdc5               // vpsadbw	%ymm2, %ymm6, %ymm6
	LONG $0x397de3c4; WORD $0x01f7 // vextracti128	$1, %ymm6, %xmm7
	LONG $0xf7d4c9c5               // vpaddq	%xmm7, %xmm6, %xmm6
	LONG $0xfe70f9c5; BYTE $0xee   // vpshufd	$238, %xmm6, %xmm7              # xmm7 = xmm6[2,3,2,3]
	LONG $0xf7d4c9c5               // vpaddq	%xmm7, %xmm6, %xmm6
	LONG $0x7ef9e1c4; BYTE $0xf1   // vmovq	%xmm6, %rcx
	WORD $0x0148; BYTE $0xc1       // addq	%rax, %rcx
	LONG $0x7ef9e1c4; BYTE $0xe8   // vmovq	%xmm5, %rax
	LONG $0x397de3c4; WORD $0x01ed // vextracti128	$1, %ymm5, %xmm5
	WORD $0x0148; BYTE $0xc1       // addq	%rax, %rcx
	LONG $0x7ef9e1c4; BYTE $0xe8   // vmovq	%xmm5, %rax
	WORD $0x0148; BYTE $0xc1       // addq	%rax, %rcx
	LONG $0x16f9e3c4; WORD $0x01e8 // vpextrq	$1, %xmm5, %rax
	LONG $0xe9dbddc5               // vpand	%ymm1, %ymm4, %ymm5
	LONG $0xd471ddc5; BYTE $0x04   // vpsrlw	$4, %ymm4, %ymm4
	LONG $0xe4dbf5c5               // vpand	%ymm4, %ymm1, %ymm4
	LONG $0x007de2c4; BYTE $0xed   // vpshufb	%ymm5, %ymm0, %ymm5
	LONG $0x007de2c4; BYTE $0xe4   // vpshufb	%ymm4, %ymm0, %ymm4
	LONG $0xe5fcddc5               // vpaddb	%ymm5, %ymm4, %ymm4
	LONG $0xe2f6ddc5               // vpsadbw	%ymm2, %ymm4, %ymm4
	WORD $0x014c; BYTE $0xf9       // addq	%r15, %rcx
	LONG $0x7ef9e1c4; BYTE $0xe3   // vmovq	%xmm4, %rbx
	WORD $0x0148; BYTE $0xc1       // addq	%rax, %rcx
	LONG $0x16f9e3c4; WORD $0x01e0 // vpextrq	$1, %xmm4, %rax
	LONG $0x397de3c4; WORD $0x01e4 // vextracti128	$1, %ymm4, %xmm4
	WORD $0x0148; BYTE $0xc1       // addq	%rax, %rcx
	LONG $0x7ef9e1c4; BYTE $0xe0   // vmovq	%xmm4, %rax
	WORD $0x0148; BYTE $0xd9       // addq	%rbx, %rcx
	LONG $0x16f9e3c4; WORD $0x01e3 // vpextrq	$1, %xmm4, %rbx
	WORD $0x0148; BYTE $0xc1       // addq	%rax, %rcx
	LONG $0xe1dbe5c5               // vpand	%ymm1, %ymm3, %ymm4
	LONG $0xd371e5c5; BYTE $0x04   // vpsrlw	$4, %ymm3, %ymm3
	LONG $0xdbdbf5c5               // vpand	%ymm3, %ymm1, %ymm3
	LONG $0x007de2c4; BYTE $0xe4   // vpshufb	%ymm4, %ymm0, %ymm4
	LONG $0x007de2c4; BYTE $0xdb   // vpshufb	%ymm3, %ymm0, %ymm3
	LONG $0xdcfce5c5               // vpaddb	%ymm4, %ymm3, %ymm3
	LONG $0xdaf6e5c5               // vpsadbw	%ymm2, %ymm3, %ymm3
	WORD $0x0148; BYTE $0xd9       // addq	%rbx, %rcx
	LONG $0x16f9e3c4; WORD $0x01d8 // vpextrq	$1, %xmm3, %rax
	WORD $0x0148; BYTE $0xc1       // addq	%rax, %rcx
	LONG $0x7ef9e1c4; BYTE $0xd8   // vmovq	%xmm3, %rax
	LONG $0x397de3c4; WORD $0x01db // vextracti128	$1, %ymm3, %xmm3
	WORD $0x0148; BYTE $0xc1       // addq	%rax, %rcx
	LONG $0x7ef9e1c4; BYTE $0xd8   // vmovq	%xmm3, %rax
	WORD $0x0148; BYTE $0xc1       // addq	%rax, %rcx
	LONG $0x16f9e3c4; WORD $0x01d8 // vpextrq	$1, %xmm3, %rax
	WORD $0x8949; BYTE $0xcf       // movq	%rcx, %r15
	WORD $0x0149; BYTE $0xc7       // addq	%rax, %r15
	LONG $0xf0c58341               // addl	$-16, %r13d
	LONG $0x80ef8348               // subq	$-128, %rdi
	LONG $0x80ee8348               // subq	$-128, %rsi
	LONG $0x0ffd8341               // cmpl	$15, %r13d
	JA   LBB2_3
	LONG $0x04fd8341               // cmpl	$4, %r13d
	JB   LBB2_7

LBB2_5:
	LONG $0xd2efe9c5 // vpxor	%xmm2, %xmm2, %xmm2

LBB2_6:
	LONG $0x1e6ffec5               // vmovdqu	(%rsi), %ymm3
	LONG $0x1fefe5c5               // vpxor	(%rdi), %ymm3, %ymm3
	LONG $0xe1dbe5c5               // vpand	%ymm1, %ymm3, %ymm4
	LONG $0xd371e5c5; BYTE $0x04   // vpsrlw	$4, %ymm3, %ymm3
	LONG $0xdbdbf5c5               // vpand	%ymm3, %ymm1, %ymm3
	LONG $0x007de2c4; BYTE $0xe4   // vpshufb	%ymm4, %ymm0, %ymm4
	LONG $0x007de2c4; BYTE $0xdb   // vpshufb	%ymm3, %ymm0, %ymm3
	LONG $0xdcfce5c5               // vpaddb	%ymm4, %ymm3, %ymm3
	LONG $0xdaf6e5c5               // vpsadbw	%ymm2, %ymm3, %ymm3
	LONG $0x397de3c4; WORD $0x01dc // vextracti128	$1, %ymm3, %xmm4
	LONG $0xdcd4e1c5               // vpaddq	%xmm4, %xmm3, %xmm3
	LONG $0xe370f9c5; BYTE $0xee   // vpshufd	$238, %xmm3, %xmm4              # xmm4 = xmm3[2,3,2,3]
	LONG $0xdcd4e1c5               // vpaddq	%xmm4, %xmm3, %xmm3
	LONG $0x7ef9e1c4; BYTE $0xd8   // vmovq	%xmm3, %rax
	WORD $0x0149; BYTE $0xc7       // addq	%rax, %r15
	LONG $0xfcc58341               // addl	$-4, %r13d
	LONG $0x20c78348               // addq	$32, %rdi
	LONG $0x20c68348               // addq	$32, %rsi
	LONG $0x03fd8341               // cmpl	$3, %r13d
	JA   LBB2_6

LBB2_7:
	WORD $0x8545; BYTE $0xed // testl	%r13d, %r13d
	JE   LBB2_17
	WORD $0x8b4d; BYTE $0x01 // movq	(%r9), %r8
	LONG $0x08518b4d         // movq	8(%r9), %r10
	LONG $0x10598b4d         // movq	16(%r9), %r11
	LONG $0x18498b4d         // movq	24(%r9), %r9
	LONG $0xff458d41         // leal	-1(%r13), %eax
	WORD $0xf883; BYTE $0x07 // cmpl	$7, %eax
	JAE  LBB2_12
	WORD $0x8949; BYTE $0xfe // movq	%rdi, %r14
	WORD $0x8949; BYTE $0xf4 // movq	%rsi, %r12
	JMP  LBB2_15

LBB2_12:
	LONG $0x01c08348             // addq	$1, %rax
	WORD $0x8948; BYTE $0xc3     // movq	%rax, %rbx
	LONG $0xf8e38348             // andq	$-8, %rbx
	LONG $0xdf348d4c             // leaq	(%rdi,%rbx,8), %r14
	LONG $0xde248d4c             // leaq	(%rsi,%rbx,8), %r12
	WORD $0x2941; BYTE $0xdd     // subl	%ebx, %r13d
	LONG $0x6ef9c1c4; BYTE $0xc7 // vmovq	%r15, %xmm0
	LONG $0x6ef9c1c4; BYTE $0xc8 // vmovq	%r8, %xmm1
	LONG $0x597de2c4; BYTE $0xc9 // vpbroadcastq	%xmm1, %ymm1
	LONG $0x6ef9c1c4; BYTE $0xd2 // vmovq	%r10, %xmm2
	LONG $0x597de2c4; BYTE $0xd2 // vpbroadcastq	%xmm2, %ymm2
	LONG $0x6ef9c1c4; BYTE $0xdb // vmovq	%r11, %xmm3
	LONG $0x597de2c4; BYTE $0xdb // vpbroadcastq	%xmm3, %ymm3
	LONG $0x6ef9c1c4; BYTE $0xe1 // vmovq	%r9, %xmm4
	LONG $0x597de2c4; BYTE $0xec // vpbroadcastq	%xmm4, %ymm5
	LONG $0xe4efd9c5             // vpxor	%xmm4, %xmm4, %xmm4
	WORD $0xc931                 // xorl	%ecx, %ecx
	LONG $0xd573cdc5; BYTE $0x20 // vpsrlq	$32, %ymm5, %ymm6

LBB2_13:
	LONG $0x3c6ffec5; BYTE $0xce   // vmovdqu	(%rsi,%rcx,8), %ymm7
	LONG $0x446f7ec5; WORD $0x20ce // vmovdqu	32(%rsi,%rcx,8), %ymm8
	LONG $0x3cefc5c5; BYTE $0xcf   // vpxor	(%rdi,%rcx,8), %ymm7, %ymm7
	LONG $0x44ef3dc5; WORD $0x20cf // vpxor	32(%rdi,%rcx,8), %ymm8, %ymm8
	LONG $0xcfdb75c5               // vpand	%ymm7, %ymm1, %ymm9
	LONG $0xd1db3dc5               // vpand	%ymm1, %ymm8, %ymm10
	LONG $0xd773c5c5; BYTE $0x01   // vpsrlq	$1, %ymm7, %ymm7
	LONG $0x733dc1c4; WORD $0x01d0 // vpsrlq	$1, %ymm8, %ymm8
	LONG $0xf9dbc5c5               // vpand	%ymm1, %ymm7, %ymm7
	LONG $0xffd4b5c5               // vpaddq	%ymm7, %ymm9, %ymm7
	LONG $0xc1db3dc5               // vpand	%ymm1, %ymm8, %ymm8
	LONG $0xd43d41c4; BYTE $0xc2   // vpaddq	%ymm10, %ymm8, %ymm8
	LONG $0xcadb45c5               // vpand	%ymm2, %ymm7, %ymm9
	LONG $0xd2db3dc5               // vpand	%ymm2, %ymm8, %ymm10
	LONG $0xd773c5c5; BYTE $0x02   // vpsrlq	$2, %ymm7, %ymm7
	LONG $0x733dc1c4; WORD $0x02d0 // vpsrlq	$2, %ymm8, %ymm8
	LONG $0xfadbc5c5               // vpand	%ymm2, %ymm7, %ymm7
	LONG $0xffd4b5c5               // vpaddq	%ymm7, %ymm9, %ymm7
	LONG $0xc2db3dc5               // vpand	%ymm2, %ymm8, %ymm8
	LONG $0xd43d41c4; BYTE $0xc2   // vpaddq	%ymm10, %ymm8, %ymm8
	LONG $0xcbdb45c5               // vpand	%ymm3, %ymm7, %ymm9
	LONG $0xd3db3dc5               // vpand	%ymm3, %ymm8, %ymm10
	LONG $0xd773c5c5; BYTE $0x04   // vpsrlq	$4, %ymm7, %ymm7
	LONG $0x733dc1c4; WORD $0x04d0 // vpsrlq	$4, %ymm8, %ymm8
	LONG $0xfbdbc5c5               // vpand	%ymm3, %ymm7, %ymm7
	LONG $0xffd4b5c5               // vpaddq	%ymm7, %ymm9, %ymm7
	LONG $0xc3db3dc5               // vpand	%ymm3, %ymm8, %ymm8
	LONG $0xd43d41c4; BYTE $0xc2   // vpaddq	%ymm10, %ymm8, %ymm8
	LONG $0xcef445c5               // vpmuludq	%ymm6, %ymm7, %ymm9
	LONG $0xd773adc5; BYTE $0x20   // vpsrlq	$32, %ymm7, %ymm10
	LONG $0xd5f42dc5               // vpmuludq	%ymm5, %ymm10, %ymm10
	LONG $0xd43541c4; BYTE $0xca   // vpaddq	%ymm10, %ymm9, %ymm9
	LONG $0x7335c1c4; WORD $0x20f1 // vpsllq	$32, %ymm9, %ymm9
	LONG $0xfdf4c5c5               // vpmuludq	%ymm5, %ymm7, %ymm7
	LONG $0xffd4b5c5               // vpaddq	%ymm7, %ymm9, %ymm7
	LONG $0xcef43dc5               // vpmuludq	%ymm6, %ymm8, %ymm9
	LONG $0x732dc1c4; WORD $0x20d0 // vpsrlq	$32, %ymm8, %ymm10
	LONG $0xd5f42dc5               // vpmuludq	%ymm5, %ymm10, %ymm10
	LONG $0xd43541c4; BYTE $0xca   // vpaddq	%ymm10, %ymm9, %ymm9
	LONG $0x7335c1c4; WORD $0x20f1 // vpsllq	$32, %ymm9, %ymm9
	LONG $0xc5f43dc5               // vpmuludq	%ymm5, %ymm8, %ymm8
	LONG $0xd43d41c4; BYTE $0xc1   // vpaddq	%ymm9, %ymm8, %ymm8
	LONG $0xd773c5c5; BYTE $0x38   // vpsrlq	$56, %ymm7, %ymm7
	LONG $0xc0d4c5c5               // vpaddq	%ymm0, %ymm7, %ymm0
	LONG $0x7345c1c4; WORD $0x38d0 // vpsrlq	$56, %ymm8, %ymm7
	LONG $0xe4d4c5c5               // vpaddq	%ymm4, %ymm7, %ymm4
	LONG $0x08c18348               // addq	$8, %rcx
	WORD $0x3948; BYTE $0xcb       // cmpq	%rcx, %rbx
	JNE  LBB2_13
	LONG $0xc0d4ddc5               // vpaddq	%ymm0, %ymm4, %ymm0
	LONG $0x397de3c4; WORD $0x01c1 // vextracti128	$1, %ymm0, %xmm1
	LONG $0xc1d4f9c5               // vpaddq	%xmm1, %xmm0, %xmm0
	LONG $0xc870f9c5; BYTE $0xee   // vpshufd	$238, %xmm0, %xmm1              # xmm1 = xmm0[2,3,2,3]
	LONG $0xc1d4f9c5               // vpaddq	%xmm1, %xmm0, %xmm0
	LONG $0x7ef9c1c4; BYTE $0xc7   // vmovq	%xmm0, %r15
	WORD $0x3948; BYTE $0xd8       // cmpq	%rbx, %rax
	JE   LBB2_17

LBB2_15:
	WORD $0x8944; BYTE $0xe8 // movl	%r13d, %eax
	WORD $0xf631             // xorl	%esi, %esi

LBB2_16:
	LONG $0xf43c8b49         // movq	(%r12,%rsi,8), %rdi
	LONG $0xf63c3349         // xorq	(%r14,%rsi,8), %rdi
	WORD $0x894c; BYTE $0xc1 // movq	%r8, %rcx
	WORD $0x2148; BYTE $0xf9 // andq	%rdi, %rcx
	WORD $0xd148; BYTE $0xef // shrq	%rdi
	WORD $0x214c; BYTE $0xc7 // andq	%r8, %rdi
	WORD $0x0148; BYTE $0xcf // addq	%rcx, %rdi
	WORD $0x8948; BYTE $0xf9 // movq	%rdi, %rcx
	WORD $0x214c; BYTE $0xd1 // andq	%r10, %rcx
	LONG $0x02efc148         // shrq	$2, %rdi
	WORD $0x214c; BYTE $0xd7 // andq	%r10, %rdi
	WORD $0x0148; BYTE $0xcf // addq	%rcx, %rdi
	WORD $0x8948; BYTE $0xf9 // movq	%rdi, %rcx
	WORD $0x214c; BYTE $0xd9 // andq	%r11, %rcx
	LONG $0x04efc148         // shrq	$4, %rdi
	WORD $0x214c; BYTE $0xdf // andq	%r11, %rdi
	WORD $0x0148; BYTE $0xcf // addq	%rcx, %rdi
	LONG $0xf9af0f49         // imulq	%r9, %rdi
	LONG $0x38efc148         // shrq	$56, %rdi
	WORD $0x0149; BYTE $0xff // addq	%rdi, %r15
	LONG $0x01c68348         // addq	$1, %rsi
	WORD $0xf039             // cmpl	%esi, %eax
	JNE  LBB2_16

LBB2_17:
	WORD $0x894c; BYTE $0x3a // movq	%r15, (%rdx)
	LONG $0xd8658d48         // leaq	-40(%rbp), %rsp
	BYTE $0x5b               // popq	%rbx
	WORD $0x5c41             // popq	%r12
	WORD $0x5d41             // popq	%r13
	WORD $0x5e41             // popq	%r14
	WORD $0x5f41             // popq	%r15
	BYTE $0x5d               // popq	%rbp
	WORD $0xf8c5; BYTE $0x77 // vzeroupper
	BYTE $0xc3               // retq
