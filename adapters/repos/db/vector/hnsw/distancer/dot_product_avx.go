package distancer

//go:generate python3 -m peachpy.x86_64 dot_product_avx.py -S -o dot_product_avx_amd64.s -mabi=goasm
func DotProductAVX(a, b []float32) float32

//go:generate python3 -m peachpy.x86_64 last_element.py -S -o last_element_amd64.s -mabi=goasm
func LastElement(a, b []float32) float32
