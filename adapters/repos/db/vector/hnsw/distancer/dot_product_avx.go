package distancer

//go:generate python3 -m peachpy.x86_64 dot_product_avx.py -S -o dot_product_avx_amd64.s -mabi=goasm
func DotProductAVX(x uintptr, y uintptr, length uintptr) float32
