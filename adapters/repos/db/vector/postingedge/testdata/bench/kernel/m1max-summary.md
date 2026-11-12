| benchmark | rounds | median | min | max |
|---|---|---|---|---|
| BenchmarkKernel/dims=768/current_distance | 10 | 33.59 | 33.42 | 36.00 |
| BenchmarkKernel/dims=768/current_five_calls | 10 | 33.66 | 33.52 | 34.02 |
| BenchmarkKernel/dims=768/fused_go | 10 | 26.05 | 25.99 | 26.41 |
| BenchmarkKernel/dims=768/fused_neon | 10 | 12.54 | 12.48 | 13.59 |
| BenchmarkKernel/dims=768/prototype_distance | 10 | 14.28 | 14.22 | 14.56 |
| BenchmarkKernel/dims=1536/current_distance | 10 | 44.71 | 43.24 | 45.55 |
| BenchmarkKernel/dims=1536/current_five_calls | 10 | 45.71 | 44.23 | 53.63 |
| BenchmarkKernel/dims=1536/fused_go | 10 | 48.43 | 48.34 | 48.63 |
| BenchmarkKernel/dims=1536/fused_neon | 10 | 18.76 | 18.67 | 19.11 |
| BenchmarkKernel/dims=1536/prototype_distance | 10 | 21.03 | 20.91 | 21.08 |
| BenchmarkPostingScan/dims=768/current_distance | 10 | 33.34 | 32.63 | 35.08 |
| BenchmarkPostingScan/dims=768/prototype_distance | 10 | 13.29 | 13.19 | 16.89 |
| BenchmarkPostingScan/dims=768/fastscan | 10 | 6.61 | 6.59 | 6.66 |
| BenchmarkPostingScan/dims=1536/current_distance | 10 | 42.88 | 42.33 | 43.78 |
| BenchmarkPostingScan/dims=1536/prototype_distance | 10 | 19.80 | 19.64 | 23.39 |
| BenchmarkPostingScan/dims=1536/fastscan | 10 | 10.76 | 10.71 | 10.86 |
| BenchmarkScattered/dims=768/current_distance | 10 | 47.59 | 46.59 | 50.61 |
| BenchmarkScattered/dims=768/prototype_distance | 10 | 28.20 | 26.88 | 29.70 |
| BenchmarkScattered/dims=1536/current_distance | 10 | 55.07 | 54.41 | 67.27 |
| BenchmarkScattered/dims=1536/prototype_distance | 10 | 32.94 | 31.28 | 35.25 |
| BenchmarkQueryLayout/dims=768/current_NewDistancer | 10 | 4572.50 | 4540.00 | 4891.00 |
| BenchmarkQueryLayout/dims=768/prototype_layout | 10 | 15761.50 | 15228.00 | 16032.00 |
| BenchmarkQueryLayout/dims=1536/current_NewDistancer | 10 | 9255.50 | 9113.00 | 9676.00 |
| BenchmarkQueryLayout/dims=1536/prototype_layout | 10 | 37826.00 | 36652.00 | 45019.00 |

| current | prototype | median ratio | min | max | median saving ns |
|---|---|---|---|---|---|
| dims=768/current_five_calls | fused_neon | 2.68x | 2.50x | 2.72x | 21.1 |
| dims=768/current_five_calls | fused_go | 1.29x | 1.27x | 1.30x | 7.6 |
| dims=768/current_distance | prototype_distance | 2.35x | 2.32x | 2.52x | 19.3 |
| dims=768/current_distance | prototype_distance | 2.49x | 2.08x | 2.52x | 19.9 |
| dims=768/current_distance | fastscan | 5.04x | 4.95x | 5.29x | 26.7 |
| dims=768/current_distance | prototype_distance | 1.73x | 1.59x | 1.82x | 20.1 |
| dims=1536/current_five_calls | fused_neon | 2.44x | 2.32x | 2.85x | 27.0 |
| dims=1536/current_five_calls | fused_go | 0.94x | 0.91x | 1.11x | -2.7 |
| dims=1536/current_distance | prototype_distance | 2.13x | 2.06x | 2.17x | 23.7 |
| dims=1536/current_distance | prototype_distance | 2.16x | 1.86x | 2.20x | 22.7 |
| dims=1536/current_distance | fastscan | 3.99x | 3.90x | 4.08x | 32.1 |
| dims=1536/current_distance | prototype_distance | 1.71x | 1.57x | 2.05x | 23.0 |
