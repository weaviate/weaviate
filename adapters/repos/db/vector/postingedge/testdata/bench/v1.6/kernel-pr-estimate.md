# The estimate of 2026-11-12, restated for the kernel of #13880 (2026-11-13, before the night)

`python3 v1.6/scripts/kernel_pr_estimate.py v1.6/kernel-pr-rounds.txt kernel/wikidpr-10m-hnsw-work.json`

Per code, before / after the kernel of the PR, median of the rounds (ns):

| benchmark | before | after | before / after |
|---|---|---|---|
| Kernel/dims=1536/current_distance | 44.6 | 25.7 | 1.74x |
| Kernel/dims=768/current_distance | 34.0 | 18.9 | 1.79x |
| PostingScan/dims=1536/current_distance | 44.0 | 24.6 | 1.79x |
| PostingScan/dims=768/current_distance | 33.2 | 18.0 | 1.85x |
| Scattered/dims=1536/current_distance | 56.8 | 37.1 | 1.54x |
| Scattered/dims=768/current_distance | 49.2 | 32.7 | 1.50x |

savings per code: endpoint 16.4 ns (10 rounds), scanned vector 15.2 ns (10 rounds); no query layout
distances to codes per query of HNSW at 10M (TestSearchWork): ef=10 696, ef=16 876, ef=24 1109, ef=32 1338, ef=48 1780, ef=64 2209, ef=96 3052, ef=128 3874, ef=192 5462, ef=256 7000, ef=384 9969, ef=512 12827, ef=768 18275, ef=1024 23477
  the TestSearchWork build reaches 0.95 at ef 45, with 1693 distances
  the TestSearchWork build reaches 0.99 at ef 308, with 8215 distances

| recall | threads | HNSW build | PE today | PE, kernel of the PR | HNSW today | HNSW, kernel of the PR | ratio today | ratio, kernel of the PR | ratio, prototype estimate (2026-11-12) |
|---|---|---|---|---|---|---|---|---|---|
| 0.95 | 1 | tarde (ef 48, 1780 distances) | 870 | 1009 (1.16x) | 1859 | 1965 (1.06x) | 2.14x | 1.95x | 1.91x |
| 0.95 | 1 | mañana (ef 64, 2209 distances) | 870 | 1009 (1.16x) | 1828 | 1957 (1.07x) | 2.10x | 1.94x | 1.90x |
| 0.95 | 8 | tarde (ef 48, 1780 distances) | 5968 | 6920 (1.16x) | 13122 | 13873 (1.06x) | 2.20x | 2.00x | 1.96x |
| 0.95 | 8 | mañana (ef 64, 2209 distances) | 5968 | 6920 (1.16x) | 12467 | 13349 (1.07x) | 2.09x | 1.93x | 1.89x |
| 0.99 | 1 | tarde (ef 256, 7000 distances) | 261 | 319 (1.22x) | 448 | 472 (1.05x) | 1.72x | 1.48x | 1.41x |
| 0.99 | 1 | mañana (ef 384, 9969 distances) | 261 | 319 (1.22x) | 355 | 377 (1.06x) | 1.36x | 1.18x | 1.12x |
| 0.99 | 8 | tarde (ef 256, 7000 distances) | 1770 | 2166 (1.22x) | 3226 | 3400 (1.05x) | 1.82x | 1.57x | 1.49x |
| 0.99 | 8 | mañana (ef 384, 9969 distances) | 1770 | 2166 (1.22x) | 2363 | 2508 (1.06x) | 1.34x | 1.16x | 1.10x |
