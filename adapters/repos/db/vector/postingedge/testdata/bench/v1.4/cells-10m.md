## Cells at their best configuration

Each cell at the timed configuration that gives it the most QPS at each recall, read off its ef curve as in the frontier table and given with its rescore limit and source margin, and with its load batch where that is not the default. The ratios compare with the control (10M) and with HNSW, each at its own best.

| threads | cell | code B/vector | index heap MB | QPS@0.90 | QPS@0.95 | QPS@0.99 | cell / control @0.90 | cell / control @0.95 | cell / control @0.99 | HNSW / cell @0.90 | HNSW / cell @0.95 | HNSW / cell @0.99 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 10M | 104 | 1712.4 | 1096 (200, 0.06) | 870 (200, 0.06) | 261 (200, 0.06) | 1.00x | 1.00x | 1.00x | 2.67x | 2.14x | 1.72x |
| 1 | 10M-512 | 72 | 1407.2 | 911 (400, 0.06) | 654 (400, 0.06) | 182 (800, 0.06) | 0.83x | 0.75x | 0.70x | 3.21x | 2.84x | 2.46x |
| 1 | HNSW | — | 3417.8 | 2925 | 1859 | 448 | — | — | — | — | — | — |
| 8 | 10M | 104 | 1712.4 | 7250 (200, 0.06) | 5968 (200, 0.06) | 1770 (200, 0.06) | 1.00x | 1.00x | 1.00x | 2.66x | 2.20x | 1.82x |
| 8 | 10M-512 | 72 | 1407.2 | 5819 (400, 0.06) | 4185 (400, 0.06) | 1271 (800, 0.06) | 0.80x | 0.70x | 0.72x | 3.32x | 3.14x | 2.54x |
| 8 | HNSW | — | 3417.8 | 19294 | 13122 | 3226 | — | — | — | — | — | — |

## QPS at each recall level

Each cell at the timed configuration that gives it the most QPS at the level, read off its ef curve, with its ratio to the control (10M). A QPS given with a recall is that of the fastest point of the curve, which has that recall and not the one of the level: the cell has no cheaper configuration.

Threads: 1

| recall | 10M | 10M-512 | HNSW |
|---|---|---|---|
| 0.90 | 1096 at 0.9139 | 911 (0.83x) | 2925 |
| 0.92 | 1054 | 831 (0.79x) | 2642 |
| 0.93 | 989 | 794 (0.80x) | 2407 |
| 0.94 | 927 | 723 (0.78x) | 2116 |
| 0.95 | 870 | 654 (0.75x) | 1859 |
| 0.96 | 761 | 591 (0.78x) | 1593 |
| 0.97 | 631 | 480 (0.76x) | 1167 |
| 0.98 | 459 | 364 (0.79x) | 698 |
| 0.99 | 261 | 182 (0.70x) | 448 |

Threads: 8

| recall | 10M | 10M-512 | HNSW |
|---|---|---|---|
| 0.90 | 7250 at 0.9139 | 5819 (0.80x) | 19294 |
| 0.92 | 7015 | 5273 (0.75x) | 17629 |
| 0.93 | 6647 | 5019 (0.76x) | 16288 |
| 0.94 | 6299 | 4598 (0.73x) | 14620 |
| 0.95 | 5968 | 4185 (0.70x) | 13122 |
| 0.96 | 5253 | 3810 (0.73x) | 11098 |
| 0.97 | 4372 | 3102 (0.71x) | 8482 |
| 0.98 | 3123 | 2365 (0.76x) | 5263 |
| 0.99 | 1770 | 1271 (0.72x) | 3226 |

## Cells: build and memory

Code bytes include the header word. Structure is the index heap without the codes. The margin is the one the calibration of the cell took, or the one it was timed with if it was not calibrated, one per rescore limit where they differ. A cell whose postings have scan codes of their own holds two codes per vector, the one of the sidecar and the shorter one the scan reads: its code columns are both together.

| cell | code B/vector | postings | mean posting | P | R | candidates/vector | edge recall | edges/vector | margin | index heap MB | codes MB | structure MB | build s | assign s | edges s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 104 | 42373 | 236 | 320 | 256 | 66060 | 0.9470 | 11.41 | 0.06 | 1712.4 | 991.8 | 720.6 | 3698 | 0 | 3668 |
| 10M-512 | 72 | 42373 | 236 | 320 | 2048 | 66060 | 0.9497 | 11.41 | 0.06 | 1407.2 | 686.6 | 720.6 | 5739 | 0 | 5711 |

Memory by component and build by stage. The rest of the index heap is what the components do not account for.

| cell | centroid vectors MB | codes MB | edges MB | postings MB | rest MB | index heap MB | partition s | assign s | encode s | edges s | centroid graph s | build s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 124.1 | 991.8 | 473.4 | 114.6 | 8.5 | 1712.4 | 0 | 0 | 19 | 3668 | 3.0 | 3698 |
| 10M-512 | 124.1 | 686.6 | 473.4 | 114.6 | 8.5 | 1407.2 | 0 | 0 | 16 | 5711 | 2.3 | 5739 |

## QPS at equal recall

Read off the ef curve of each cell and rescore limit, interpolated linearly in recall and log QPS. The fast end is the best QPS of a measured point with recall of at least 0.92. The ratios compare with the best configuration of the control (10M) and of HNSW at the same recall.

| threads | cell | rescore | batch | margin | QPS@0.90 | QPS@0.92 | QPS@0.93 | QPS@0.94 | QPS@0.95 | QPS@0.96 | QPS@0.97 | QPS@0.98 | QPS@0.99 | fast end | cell / control @0.95 | cell / control @0.99 | HNSW / cell @0.95 | HNSW / cell @0.99 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 10M | 200 | 4 | 0.06 | 1096 | 1054 | 989 | 927 | 870 | 761 | 631 | 459 | 261 | 846 | 1.00x | 1.00x | 2.14x | 1.72x |
| 1 | 10M | 400 | 4 | 0.06 | 930 | 900 | 853 | 809 | 766 | 679 | 573 | 427 | 251 | 749 | 0.88x | 0.96x | 2.43x | 1.78x |
| 1 | 10M-512 | 400 | 4 | 0.06 | 911 | 831 | 794 | 723 | 654 | 591 | 480 | 364 | 167 | 789 | 0.75x | 0.64x | 2.84x | 2.68x |
| 1 | 10M-512 | 800 | 4 | 0.06 | 694 | 645 | 621 | 579 | 536 | 496 | 415 | 331 | 182 | 618 | 0.62x | 0.70x | 3.47x | 2.46x |
| 1 | HNSW | best | — | — | 2925 | 2642 | 2407 | 2116 | 1859 | 1593 | 1167 | 698 | 448 | — | — | — | — | — |
| 8 | 10M | 200 | 4 | 0.06 | 7250 | 7015 | 6647 | 6299 | 5968 | 5253 | 4372 | 3123 | 1770 | 5832 | 1.00x | 1.00x | 2.20x | 1.82x |
| 8 | 10M | 400 | 4 | 0.06 | 6290 | 6071 | 5729 | 5406 | 5101 | 4534 | 3851 | 2902 | 1721 | 4976 | 0.85x | 0.97x | 2.57x | 1.87x |
| 8 | 10M-512 | 400 | 4 | 0.06 | 5819 | 5273 | 5019 | 4598 | 4185 | 3810 | 3102 | 2365 | 1052 | 4985 | 0.70x | 0.59x | 3.14x | 3.07x |
| 8 | 10M-512 | 800 | 4 | 0.06 | 4844 | 4482 | 4311 | 4017 | 3722 | 3448 | 2897 | 2321 | 1271 | 4286 | 0.62x | 0.72x | 3.53x | 2.54x |
| 8 | HNSW | best | — | — | 19294 | 17629 | 16288 | 14620 | 13122 | 11098 | 8482 | 5263 | 3226 | — | — | — | — | — |

## Recall reached by raising ef

With the source margin of the cell. A configuration that stays below 0.99 over its sweep gets untimed passes that keep doubling ef until it gets there or a doubling adds less than 0.0005. The ceiling is the best recall measured, and it saturates at the smallest ef within 0.0005 of it. Without margin is the recall at the largest ef measured, with no source margin.

| cell | rescore | batch | margin | first ef with recall ≥ 0.99 | recall there | ceiling | saturates at ef | largest ef | recall there | without margin |
|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 200 | 4 | 0.06 | 160 | 0.9909 | 0.9964 | 1024 | 1024 | 0.9964 | — |
| 10M | 400 | 4 | 0.06 | 160 | 0.9909 | 0.9965 | 1024 | 1024 | 0.9965 | — |
| 10M-512 | 400 | 4 | 0.06 | 320 | 0.9909 | 0.9946 | 1024 | 1024 | 0.9946 | — |
| 10M-512 | 800 | 4 | 0.06 | 320 | 0.9922 | 0.9961 | 1024 | 1024 | 0.9961 | — |

## Work and time per query at equal recall

One thread, read along the ef curve of each configuration like the QPS. The recall column is the level, or the recall of the first point of the curve where the level is below it: the row is then that point, the cheapest search of the configuration. Sources expanded are the scanned vectors within the source margin, the ones whose edges are followed. The times come from the instrumented pass, which is slower than the timed ones, and the rest is the time no phase accounts for. Postings loaded are also given against the control at the same rescore limit, or at its largest one where the control was not timed at that limit.

### Recall 0.90

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 200 | 4 | 0.06 | 0.9139 | 10 | 9 | 2 | 2493 | 743 | 8000 | 4539 | 225 | 132 | 335 | 5 | 140 | 97 | 934 | 1.00x |
| 10M | 400 | 4 | 0.06 | 0.9139 | 10 | 9 | 2 | 2493 | 743 | 8000 | 4539 | 217 | 153 | 329 | 5 | 266 | 108 | 1078 | 1.00x |
| 10M-512 | 400 | 4 | 0.06 | 0.90 | 12 | 11 | 2 | 2879 | 832 | 8943 | 5096 | 223 | 166 | 328 | 6 | 270 | 120 | 1116 | 1.16x |
| 10M-512 | 800 | 4 | 0.06 | 0.90 | 12 | 11 | 2 | 2878 | 831 | 8940 | 5095 | 229 | 198 | 334 | 6 | 528 | 152 | 1452 | 1.16x |

### Recall 0.92

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 200 | 4 | 0.06 | 0.92 | 11 | 10 | 2 | 2615 | 800 | 8636 | 4869 | 226 | 138 | 359 | 5 | 140 | 103 | 973 | 1.00x |
| 10M | 400 | 4 | 0.06 | 0.92 | 11 | 10 | 2 | 2615 | 800 | 8636 | 4869 | 218 | 159 | 352 | 5 | 267 | 113 | 1116 | 1.00x |
| 10M-512 | 400 | 4 | 0.06 | 0.92 | 17 | 13 | 3 | 3316 | 1039 | 11274 | 6313 | 226 | 186 | 405 | 7 | 271 | 139 | 1239 | 1.28x |
| 10M-512 | 800 | 4 | 0.06 | 0.92 | 17 | 13 | 3 | 3315 | 1039 | 11265 | 6308 | 231 | 223 | 410 | 7 | 532 | 170 | 1579 | 1.28x |

### Recall 0.93

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 200 | 4 | 0.06 | 0.93 | 13 | 11 | 2 | 2827 | 904 | 9790 | 5463 | 227 | 147 | 401 | 6 | 140 | 112 | 1039 | 1.00x |
| 10M | 400 | 4 | 0.06 | 0.93 | 13 | 11 | 2 | 2827 | 904 | 9790 | 5463 | 219 | 169 | 394 | 6 | 267 | 122 | 1182 | 1.00x |
| 10M-512 | 400 | 4 | 0.06 | 0.93 | 20 | 14 | 3 | 3560 | 1162 | 12659 | 7026 | 227 | 197 | 450 | 8 | 272 | 149 | 1305 | 1.27x |
| 10M-512 | 800 | 4 | 0.06 | 0.93 | 20 | 14 | 3 | 3557 | 1161 | 12645 | 7019 | 232 | 237 | 455 | 8 | 534 | 180 | 1646 | 1.27x |

### Recall 0.94

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 200 | 4 | 0.06 | 0.94 | 16 | 12 | 3 | 3057 | 1021 | 11098 | 6130 | 228 | 158 | 449 | 7 | 141 | 122 | 1109 | 1.00x |
| 10M | 400 | 4 | 0.06 | 0.94 | 16 | 12 | 3 | 3057 | 1021 | 11098 | 6130 | 220 | 181 | 440 | 6 | 268 | 131 | 1251 | 1.00x |
| 10M-512 | 400 | 4 | 0.06 | 0.94 | 24 | 15 | 4 | 4035 | 1379 | 15099 | 8244 | 229 | 218 | 526 | 10 | 273 | 168 | 1431 | 1.33x |
| 10M-512 | 800 | 4 | 0.06 | 0.94 | 24 | 15 | 4 | 4030 | 1377 | 15071 | 8230 | 233 | 261 | 531 | 10 | 534 | 199 | 1776 | 1.33x |

### Recall 0.95

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 200 | 4 | 0.06 | 0.95 | 19 | 13 | 3 | 3306 | 1153 | 12581 | 6878 | 229 | 168 | 502 | 8 | 141 | 134 | 1184 | 1.00x |
| 10M | 400 | 4 | 0.06 | 0.95 | 19 | 13 | 3 | 3306 | 1153 | 12581 | 6878 | 221 | 193 | 492 | 7 | 268 | 142 | 1325 | 1.00x |
| 10M-512 | 400 | 4 | 0.06 | 0.95 | 31 | 18 | 4 | 4615 | 1653 | 18187 | 9756 | 231 | 243 | 620 | 12 | 274 | 191 | 1579 | 1.40x |
| 10M-512 | 800 | 4 | 0.06 | 0.95 | 31 | 18 | 4 | 4609 | 1650 | 18153 | 9740 | 234 | 290 | 625 | 12 | 534 | 222 | 1928 | 1.40x |

### Recall 0.96

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 200 | 4 | 0.06 | 0.96 | 25 | 15 | 3 | 3878 | 1444 | 15864 | 8494 | 231 | 193 | 617 | 10 | 141 | 158 | 1356 | 1.00x |
| 10M | 400 | 4 | 0.06 | 0.96 | 25 | 15 | 3 | 3878 | 1444 | 15864 | 8494 | 224 | 220 | 609 | 10 | 270 | 167 | 1507 | 1.00x |
| 10M-512 | 400 | 4 | 0.06 | 0.96 | 39 | 20 | 5 | 5278 | 1982 | 21907 | 11546 | 232 | 271 | 730 | 15 | 275 | 218 | 1743 | 1.37x |
| 10M-512 | 800 | 4 | 0.06 | 0.96 | 39 | 20 | 5 | 5271 | 1978 | 21866 | 11526 | 234 | 323 | 736 | 15 | 534 | 248 | 2092 | 1.36x |

### Recall 0.97

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 200 | 4 | 0.06 | 0.97 | 37 | 19 | 4 | 4837 | 1954 | 21676 | 11277 | 232 | 234 | 812 | 14 | 141 | 197 | 1635 | 1.00x |
| 10M | 400 | 4 | 0.06 | 0.97 | 37 | 19 | 4 | 4837 | 1954 | 21676 | 11277 | 230 | 264 | 813 | 14 | 273 | 210 | 1808 | 1.00x |
| 10M-512 | 400 | 4 | 0.06 | 0.97 | 57 | 27 | 6 | 6935 | 2751 | 30719 | 15641 | 235 | 341 | 992 | 23 | 277 | 286 | 2168 | 1.44x |
| 10M-512 | 800 | 4 | 0.06 | 0.97 | 56 | 27 | 6 | 6884 | 2728 | 30445 | 15516 | 235 | 396 | 982 | 23 | 533 | 313 | 2499 | 1.43x |

### Recall 0.98

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 200 | 4 | 0.06 | 0.98 | 63 | 27 | 7 | 6972 | 3053 | 34322 | 17019 | 233 | 322 | 1216 | 25 | 141 | 284 | 2232 | 1.00x |
| 10M | 400 | 4 | 0.06 | 0.98 | 63 | 27 | 7 | 6972 | 3053 | 34322 | 17019 | 235 | 359 | 1231 | 25 | 277 | 301 | 2441 | 1.00x |
| 10M-512 | 400 | 4 | 0.06 | 0.98 | 91 | 38 | 9 | 9799 | 4137 | 46725 | 22750 | 239 | 457 | 1451 | 40 | 280 | 403 | 2879 | 1.41x |
| 10M-512 | 800 | 4 | 0.06 | 0.98 | 87 | 37 | 9 | 9525 | 4004 | 45195 | 22092 | 236 | 509 | 1391 | 38 | 534 | 418 | 3134 | 1.37x |

### Recall 0.99

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 200 | 4 | 0.06 | 0.99 | 146 | 51 | 13 | 13081 | 6357 | 72734 | 33021 | 247 | 579 | 2431 | 69 | 150 | 559 | 4043 | 1.00x |
| 10M | 400 | 4 | 0.06 | 0.99 | 146 | 51 | 13 | 13081 | 6357 | 72734 | 33021 | 240 | 615 | 2390 | 68 | 278 | 558 | 4159 | 1.00x |
| 10M-512 | 400 | 4 | 0.06 | 0.99 | 274 | 92 | 23 | 23394 | 11028 | 126973 | 54577 | 246 | 993 | 3563 | 165 | 291 | 964 | 6238 | 1.80x |
| 10M-512 | 800 | 4 | 0.06 | 0.99 | 228 | 79 | 20 | 20167 | 9362 | 107497 | 47227 | 244 | 952 | 3054 | 130 | 548 | 854 | 5811 | 1.55x |

## Disk projection

What a query would read with the postings on disk, counted in memory and read along the ef curve of each configuration like the QPS, the 95th percentiles too. A posting loaded is a read, a round is a batch of reads issued together, so rounds plus one is the serial depth. The candidates of the final rescore are located among the loaded postings: every one of them is a scanned member of a posting the query has read, so the rescore needs no posting of its own, and the full-precision vectors come from as many places as the postings counted here if they are stored by posting.

### Recall 0.90

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 200 | 4 | 0.06 | 0.9139 | 9.5 | 17.0 | 2.1 | 4.0 | 2492.7 | 200.0 | 9.5 | 17.0 | 0.0 |
| 10M | 400 | 4 | 0.06 | 0.9139 | 9.5 | 17.0 | 2.1 | 4.0 | 2492.7 | 398.7 | 9.5 | 17.0 | 0.0 |
| 10M-512 | 400 | 4 | 0.06 | 0.90 | 11.0 | 22.1 | 2.5 | 5.3 | 2878.5 | 399.3 | 11.0 | 22.1 | 0.0 |
| 10M-512 | 800 | 4 | 0.06 | 0.90 | 11.0 | 22.1 | 2.5 | 5.3 | 2877.9 | 795.2 | 11.0 | 22.1 | 0.0 |

### Recall 0.92

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 200 | 4 | 0.06 | 0.92 | 9.9 | 18.0 | 2.2 | 4.3 | 2614.5 | 200.0 | 9.9 | 18.0 | 0.0 |
| 10M | 400 | 4 | 0.06 | 0.92 | 9.9 | 18.0 | 2.2 | 4.3 | 2614.5 | 398.8 | 9.9 | 18.0 | 0.0 |
| 10M-512 | 400 | 4 | 0.06 | 0.92 | 12.7 | 23.9 | 2.9 | 5.7 | 3316.5 | 399.6 | 12.7 | 23.9 | 0.0 |
| 10M-512 | 800 | 4 | 0.06 | 0.92 | 12.7 | 23.9 | 2.9 | 5.7 | 3314.8 | 797.2 | 12.7 | 23.9 | 0.0 |

### Recall 0.93

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 200 | 4 | 0.06 | 0.93 | 10.8 | 19.8 | 2.4 | 4.7 | 2827.3 | 200.0 | 10.8 | 19.8 | 0.0 |
| 10M | 400 | 4 | 0.06 | 0.93 | 10.8 | 19.8 | 2.4 | 4.7 | 2827.3 | 399.0 | 10.8 | 19.8 | 0.0 |
| 10M-512 | 400 | 4 | 0.06 | 0.93 | 13.6 | 24.9 | 3.2 | 6.0 | 3559.9 | 399.8 | 13.6 | 24.9 | 0.0 |
| 10M-512 | 800 | 4 | 0.06 | 0.93 | 13.6 | 24.9 | 3.2 | 6.0 | 3557.4 | 798.2 | 13.6 | 24.9 | 0.0 |

### Recall 0.94

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 200 | 4 | 0.06 | 0.94 | 11.7 | 21.8 | 2.7 | 5.2 | 3057.4 | 200.0 | 11.7 | 21.8 | 0.0 |
| 10M | 400 | 4 | 0.06 | 0.94 | 11.7 | 21.8 | 2.7 | 5.2 | 3057.4 | 399.2 | 11.7 | 21.8 | 0.0 |
| 10M-512 | 400 | 4 | 0.06 | 0.94 | 15.5 | 28.0 | 3.6 | 6.7 | 4035.2 | 399.8 | 15.5 | 28.0 | 0.0 |
| 10M-512 | 800 | 4 | 0.06 | 0.94 | 15.5 | 27.9 | 3.6 | 6.7 | 4029.8 | 798.7 | 15.5 | 27.9 | 0.0 |

### Recall 0.95

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 200 | 4 | 0.06 | 0.95 | 12.6 | 24.0 | 2.9 | 5.7 | 3306.2 | 200.0 | 12.6 | 24.0 | 0.0 |
| 10M | 400 | 4 | 0.06 | 0.95 | 12.6 | 24.0 | 2.9 | 5.7 | 3306.2 | 399.4 | 12.6 | 24.0 | 0.0 |
| 10M-512 | 400 | 4 | 0.06 | 0.95 | 17.7 | 31.9 | 4.2 | 7.7 | 4614.9 | 399.9 | 17.7 | 31.9 | 0.0 |
| 10M-512 | 800 | 4 | 0.06 | 0.95 | 17.7 | 31.8 | 4.2 | 7.7 | 4608.7 | 799.1 | 17.7 | 31.8 | 0.0 |

### Recall 0.96

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 200 | 4 | 0.06 | 0.96 | 14.9 | 28.3 | 3.5 | 6.8 | 3878.1 | 200.0 | 14.9 | 28.3 | 0.0 |
| 10M | 400 | 4 | 0.06 | 0.96 | 14.9 | 28.3 | 3.5 | 6.8 | 3878.1 | 399.6 | 14.9 | 28.3 | 0.0 |
| 10M-512 | 400 | 4 | 0.06 | 0.96 | 20.3 | 36.3 | 4.8 | 8.8 | 5277.8 | 399.9 | 20.3 | 36.3 | 0.0 |
| 10M-512 | 800 | 4 | 0.06 | 0.96 | 20.3 | 36.3 | 4.8 | 8.8 | 5270.7 | 799.5 | 20.3 | 36.3 | 0.0 |

### Recall 0.97

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 200 | 4 | 0.06 | 0.97 | 18.6 | 35.3 | 4.4 | 8.6 | 4836.9 | 200.0 | 18.6 | 35.3 | 0.0 |
| 10M | 400 | 4 | 0.06 | 0.97 | 18.6 | 35.3 | 4.4 | 8.6 | 4836.9 | 399.8 | 18.6 | 35.3 | 0.0 |
| 10M-512 | 400 | 4 | 0.06 | 0.97 | 26.9 | 49.1 | 6.5 | 12.0 | 6934.8 | 400.0 | 26.9 | 49.1 | 0.0 |
| 10M-512 | 800 | 4 | 0.06 | 0.97 | 26.7 | 48.7 | 6.4 | 11.9 | 6884.5 | 799.8 | 26.7 | 48.7 | 0.0 |

### Recall 0.98

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 200 | 4 | 0.06 | 0.98 | 27.1 | 51.1 | 6.5 | 12.5 | 6972.0 | 200.0 | 27.1 | 51.1 | 0.0 |
| 10M | 400 | 4 | 0.06 | 0.98 | 27.1 | 51.1 | 6.5 | 12.5 | 6972.0 | 399.9 | 27.1 | 51.1 | 0.0 |
| 10M-512 | 400 | 4 | 0.06 | 0.98 | 38.2 | 71.3 | 9.3 | 17.6 | 9799.1 | 400.0 | 38.2 | 71.3 | 0.0 |
| 10M-512 | 800 | 4 | 0.06 | 0.98 | 37.1 | 69.3 | 9.0 | 17.1 | 9525.4 | 800.0 | 37.1 | 69.3 | 0.0 |

### Recall 0.99

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 200 | 4 | 0.06 | 0.99 | 51.3 | 100.7 | 12.6 | 24.9 | 13080.8 | 200.0 | 51.2 | 100.7 | 0.0 |
| 10M | 400 | 4 | 0.06 | 0.99 | 51.3 | 100.7 | 12.6 | 24.9 | 13080.8 | 400.0 | 51.3 | 100.7 | 0.0 |
| 10M-512 | 400 | 4 | 0.06 | 0.99 | 92.1 | 175.1 | 22.8 | 43.5 | 23393.8 | 400.0 | 92.0 | 175.1 | 0.0 |
| 10M-512 | 800 | 4 | 0.06 | 0.99 | 79.3 | 149.0 | 19.6 | 37.0 | 20167.4 | 800.0 | 79.3 | 149.0 | 0.0 |

## Postings loaded at equal recall, untimed configurations

Configurations measured over a sweep of ef without timing: recall and work at each ef, no QPS. Read along the ef curve like the timed ones, and set against the control the same way. The last columns are the largest ef measured and the recall there.

| cell | rescore | batch | margin | loads @0.90 | loads / control @0.90 | loads @0.95 | loads / control @0.95 | loads @0.99 | loads / control @0.99 | largest ef | recall there |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 64 | 4 | 0.06 | 9 | 1.00x | 13 | 1.01x | 146 | 2.85x | 1024 | 0.9918 |
| 10M | 200 | 4 | none | 10 | 1.00x | 13 | 1.01x | 50 | 0.98x | 160 | 0.9912 |
| 10M-512 | 200 | 4 | 0.06 | 11 | 1.16x | 18 | 1.42x | not reached | — | 1024 | 0.9892 |

## Source margin of each cell

The recall each margin costs at equal ef, against no margin: the worst ef and the ef it happens at. A cell takes the smallest margin within its tolerance, for each rescore limit it was calibrated at.

| cell | rescore | margin | worst recall change | at ef | endpoints scored at that ef, against no margin | taken |
|---|---|---|---|---|---|---|

## Noise

Noisy are the timed points whose passes are more than 10% of their median apart. Outworked are the ones that give at least 2% less QPS than a point that does more work than them: the same threads, load batch, source margin and ef with a larger rescore limit. A stretch of points taken while the machine was slow shows in neither unless it falls behind another curve; a repeat of the cell is what shows it.

| cell | timed points | noisy | which (threads, rescore, batch, margin, ef: QPS of each pass) | outworked | which (threads, batch, margin, ef: rescore and QPS of the point, of the busier one) |
|---|---|---|---|---|---|
| 10M | 32 | 0 | — | 0 | — |
| 10M-512 | 32 | 1 | 8 threads, rescore 400, batch 4, margin 0.06, ef 80: 2577 / 2310 / 2565 | 3 | 8 threads, batch 4, margin 0.06, ef 320: rescore 400 927, rescore 800 1008; 8 threads, batch 4, margin 0.06, ef 640: rescore 400 553, rescore 800 598; 8 threads, batch 4, margin 0.06, ef 1024: rescore 400 381, rescore 800 394 |

## Repeats

The timed points of a cell against the same points of another run of it, one row per ef curve. The change is the QPS of the cell over that of the other run. Repeated are the points within 2% of the other run; the others are listed by ef. The recall difference is the largest of the curve.

| cell | threads | rescore | batch | margin | points | repeated | median change | the others (ef: change) | recall difference |
|---|---|---|---|---|---|---|---|---|---|
| 10M | 1 | 200 | 4 | 0.06 | 8 | 7 | -1.2% | 320: -2.0% | 0.0000 |
| 10M | 1 | 400 | 4 | 0.06 | 8 | 8 | +0.2% | — | 0.0000 |
| 10M | 8 | 200 | 4 | 0.06 | 8 | 6 | -1.3% | 10: -4.8%; 80: -4.5% | 0.0000 |
| 10M | 8 | 400 | 4 | 0.06 | 8 | 6 | -1.1% | 20: -4.0%; 40: -2.1% | 0.0000 |
| 10M-512 | 1 | 400 | 4 | 0.06 | 8 | 6 | -1.7% | 320: -2.7%; 1024: -2.5% | 0.0000 |
| 10M-512 | 1 | 800 | 4 | 0.06 | 8 | 6 | +0.2% | 10: -3.4%; 20: +2.2% | 0.0000 |
| 10M-512 | 8 | 400 | 4 | 0.06 | 8 | 0 | -8.1% | 10: -6.6%; 20: -8.3%; 40: -8.5%; 80: -7.8%; 160: -6.2%; 320: -13.5%; 640: -10.6%; 1024: -5.6% | 0.0000 |
| 10M-512 | 8 | 800 | 4 | 0.06 | 8 | 6 | -0.1% | 20: +3.1%; 1024: -2.8% | 0.0000 |

## Operating points

| cell | ef | rescore | batch | margin | threads | recall@10 | QPS | p50 ms | p95 ms | postings loaded | rounds |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 10M | 10 | 200 | 4 | 0.06 | 1 | 0.9139 | 1096 | 0.80 | 1.74 | 9.5 | 2.1 |
| 10M | 20 | 200 | 4 | 0.06 | 1 | 0.9543 | 846 | 1.02 | 2.45 | 13.1 | 3.0 |
| 10M | 40 | 200 | 4 | 0.06 | 1 | 0.9721 | 607 | 1.38 | 3.78 | 19.6 | 4.6 |
| 10M | 80 | 200 | 4 | 0.06 | 1 | 0.9843 | 394 | 2.06 | 6.11 | 32.3 | 7.8 |
| 10M | 160 | 200 | 4 | 0.06 | 1 | 0.9909 | 245 | 3.27 | 10.26 | 55.2 | 13.5 |
| 10M | 320 | 200 | 4 | 0.06 | 1 | 0.9928 | 146 | 5.43 | 17.10 | 95.8 | 23.7 |
| 10M | 640 | 200 | 4 | 0.06 | 1 | 0.9952 | 87 | 9.31 | 27.11 | 168.5 | 41.9 |
| 10M | 1024 | 200 | 4 | 0.06 | 1 | 0.9964 | 60 | 13.76 | 37.40 | 246.9 | 61.5 |
| 10M | 10 | 200 | 4 | 0.06 | 8 | 0.9139 | 7250 | 0.93 | 2.18 | 9.5 | 2.1 |
| 10M | 20 | 200 | 4 | 0.06 | 8 | 0.9543 | 5832 | 1.16 | 2.85 | 13.1 | 3.0 |
| 10M | 40 | 200 | 4 | 0.06 | 8 | 0.9721 | 4207 | 1.56 | 4.22 | 19.6 | 4.6 |
| 10M | 80 | 200 | 4 | 0.06 | 8 | 0.9843 | 2656 | 2.37 | 7.18 | 32.3 | 7.8 |
| 10M | 160 | 200 | 4 | 0.06 | 8 | 0.9909 | 1660 | 3.74 | 12.10 | 55.2 | 13.5 |
| 10M | 320 | 200 | 4 | 0.06 | 8 | 0.9928 | 1017 | 6.22 | 19.71 | 95.8 | 23.7 |
| 10M | 640 | 200 | 4 | 0.06 | 8 | 0.9952 | 590 | 10.75 | 32.13 | 168.5 | 41.9 |
| 10M | 1024 | 200 | 4 | 0.06 | 8 | 0.9964 | 401 | 16.10 | 45.45 | 246.9 | 61.5 |
| 10M | 10 | 400 | 4 | 0.06 | 1 | 0.9139 | 930 | 0.97 | 1.91 | 9.5 | 2.1 |
| 10M | 20 | 400 | 4 | 0.06 | 1 | 0.9543 | 749 | 1.17 | 2.56 | 13.1 | 3.0 |
| 10M | 40 | 400 | 4 | 0.06 | 1 | 0.9721 | 552 | 1.55 | 3.94 | 19.6 | 4.6 |
| 10M | 80 | 400 | 4 | 0.06 | 1 | 0.9843 | 372 | 2.22 | 6.28 | 32.3 | 7.8 |
| 10M | 160 | 400 | 4 | 0.06 | 1 | 0.9909 | 236 | 3.42 | 10.43 | 55.2 | 13.5 |
| 10M | 320 | 400 | 4 | 0.06 | 1 | 0.9929 | 144 | 5.58 | 16.98 | 95.8 | 23.7 |
| 10M | 640 | 400 | 4 | 0.06 | 1 | 0.9953 | 85 | 9.47 | 27.37 | 168.5 | 41.9 |
| 10M | 1024 | 400 | 4 | 0.06 | 1 | 0.9965 | 59 | 13.97 | 37.87 | 246.9 | 61.5 |
| 10M | 10 | 400 | 4 | 0.06 | 8 | 0.9139 | 6290 | 1.12 | 2.23 | 9.5 | 2.1 |
| 10M | 20 | 400 | 4 | 0.06 | 8 | 0.9543 | 4976 | 1.39 | 3.18 | 13.1 | 3.0 |
| 10M | 40 | 400 | 4 | 0.06 | 8 | 0.9721 | 3721 | 1.80 | 4.63 | 19.6 | 4.6 |
| 10M | 80 | 400 | 4 | 0.06 | 8 | 0.9843 | 2535 | 2.54 | 7.47 | 32.3 | 7.8 |
| 10M | 160 | 400 | 4 | 0.06 | 8 | 0.9909 | 1619 | 3.89 | 12.04 | 55.2 | 13.5 |
| 10M | 320 | 400 | 4 | 0.06 | 8 | 0.9929 | 991 | 6.40 | 20.12 | 95.8 | 23.7 |
| 10M | 640 | 400 | 4 | 0.06 | 8 | 0.9953 | 586 | 10.88 | 32.55 | 168.5 | 41.9 |
| 10M | 1024 | 400 | 4 | 0.06 | 8 | 0.9965 | 389 | 16.60 | 46.93 | 246.9 | 61.5 |
| 10M | 10 | 64 | 4 | 0.06 | 1 | 0.9131 | untimed | — | — | 9.5 | 2.1 |
| 10M | 20 | 64 | 4 | 0.06 | 1 | 0.9532 | untimed | — | — | 13.1 | 3.0 |
| 10M | 40 | 64 | 4 | 0.06 | 1 | 0.9700 | untimed | — | — | 19.6 | 4.6 |
| 10M | 80 | 64 | 4 | 0.06 | 1 | 0.9802 | untimed | — | — | 32.3 | 7.8 |
| 10M | 160 | 64 | 4 | 0.06 | 1 | 0.9863 | untimed | — | — | 55.2 | 13.5 |
| 10M | 320 | 64 | 4 | 0.06 | 1 | 0.9882 | untimed | — | — | 95.8 | 23.7 |
| 10M | 640 | 64 | 4 | 0.06 | 1 | 0.9906 | untimed | — | — | 168.5 | 41.9 |
| 10M | 1024 | 64 | 4 | 0.06 | 1 | 0.9918 | untimed | — | — | 246.9 | 61.5 |
| 10M | 10 | 200 | 4 | none | 1 | 0.9153 | untimed | — | — | 9.5 | 2.1 |
| 10M | 20 | 200 | 4 | none | 1 | 0.9532 | untimed | — | — | 13.1 | 3.0 |
| 10M | 40 | 200 | 4 | none | 1 | 0.9740 | untimed | — | — | 19.8 | 4.7 |
| 10M | 80 | 200 | 4 | none | 1 | 0.9848 | untimed | — | — | 32.6 | 7.9 |
| 10M | 160 | 200 | 4 | none | 1 | 0.9912 | untimed | — | — | 55.4 | 13.6 |
| 10M-512 | 10 | 400 | 4 | 0.06 | 1 | 0.8866 | 968 | 0.95 | 1.71 | 10.0 | 2.2 |
| 10M-512 | 20 | 400 | 4 | 0.06 | 1 | 0.9314 | 789 | 1.12 | 2.33 | 13.8 | 3.2 |
| 10M-512 | 40 | 400 | 4 | 0.06 | 1 | 0.9614 | 583 | 1.49 | 3.42 | 20.7 | 4.9 |
| 10M-512 | 80 | 400 | 4 | 0.06 | 1 | 0.9785 | 395 | 2.14 | 5.57 | 34.7 | 8.4 |
| 10M-512 | 160 | 400 | 4 | 0.06 | 1 | 0.9869 | 249 | 3.38 | 9.23 | 59.3 | 14.6 |
| 10M-512 | 320 | 400 | 4 | 0.06 | 1 | 0.9909 | 149 | 5.51 | 15.56 | 104.6 | 25.9 |
| 10M-512 | 640 | 400 | 4 | 0.06 | 1 | 0.9929 | 90 | 9.27 | 25.13 | 185.6 | 46.1 |
| 10M-512 | 1024 | 400 | 4 | 0.06 | 1 | 0.9946 | 60 | 14.18 | 36.35 | 273.5 | 68.1 |
| 10M-512 | 10 | 400 | 4 | 0.06 | 8 | 0.8866 | 6216 | 1.11 | 2.51 | 10.0 | 2.2 |
| 10M-512 | 20 | 400 | 4 | 0.06 | 8 | 0.9314 | 4985 | 1.32 | 3.21 | 13.8 | 3.2 |
| 10M-512 | 40 | 400 | 4 | 0.06 | 8 | 0.9614 | 3760 | 1.71 | 4.57 | 20.7 | 4.9 |
| 10M-512 | 80 | 400 | 4 | 0.06 | 8 | 0.9785 | 2565 | 2.50 | 7.03 | 34.7 | 8.4 |
| 10M-512 | 160 | 400 | 4 | 0.06 | 8 | 0.9869 | 1626 | 3.89 | 11.42 | 59.3 | 14.6 |
| 10M-512 | 320 | 400 | 4 | 0.06 | 8 | 0.9909 | 927 | 6.67 | 20.25 | 104.6 | 25.9 |
| 10M-512 | 640 | 400 | 4 | 0.06 | 8 | 0.9929 | 553 | 11.32 | 32.86 | 185.6 | 46.1 |
| 10M-512 | 1024 | 400 | 4 | 0.06 | 8 | 0.9946 | 381 | 17.29 | 45.48 | 273.5 | 68.1 |
| 10M-512 | 10 | 800 | 4 | 0.06 | 1 | 0.8866 | 730 | 1.28 | 2.13 | 10.0 | 2.2 |
| 10M-512 | 20 | 800 | 4 | 0.06 | 1 | 0.9315 | 618 | 1.47 | 2.73 | 13.8 | 3.2 |
| 10M-512 | 40 | 800 | 4 | 0.06 | 1 | 0.9615 | 491 | 1.82 | 3.80 | 20.7 | 4.9 |
| 10M-512 | 80 | 800 | 4 | 0.06 | 1 | 0.9789 | 349 | 2.47 | 5.93 | 34.7 | 8.4 |
| 10M-512 | 160 | 800 | 4 | 0.06 | 1 | 0.9877 | 232 | 3.66 | 9.54 | 59.3 | 14.6 |
| 10M-512 | 320 | 800 | 4 | 0.06 | 1 | 0.9922 | 144 | 5.79 | 15.40 | 104.6 | 25.9 |
| 10M-512 | 640 | 800 | 4 | 0.06 | 1 | 0.9944 | 84 | 10.00 | 26.12 | 185.6 | 46.1 |
| 10M-512 | 1024 | 800 | 4 | 0.06 | 1 | 0.9961 | 60 | 14.13 | 35.40 | 273.5 | 68.1 |
| 10M-512 | 10 | 800 | 4 | 0.06 | 8 | 0.8866 | 5103 | 1.46 | 2.40 | 10.0 | 2.2 |
| 10M-512 | 20 | 800 | 4 | 0.06 | 8 | 0.9315 | 4286 | 1.68 | 3.12 | 13.8 | 3.2 |
| 10M-512 | 40 | 800 | 4 | 0.06 | 8 | 0.9615 | 3409 | 2.10 | 4.29 | 20.7 | 4.9 |
| 10M-512 | 80 | 800 | 4 | 0.06 | 8 | 0.9789 | 2443 | 2.82 | 6.74 | 34.7 | 8.4 |
| 10M-512 | 160 | 800 | 4 | 0.06 | 8 | 0.9877 | 1618 | 4.16 | 10.81 | 59.3 | 14.6 |
| 10M-512 | 320 | 800 | 4 | 0.06 | 8 | 0.9922 | 1008 | 6.51 | 18.11 | 104.6 | 25.9 |
| 10M-512 | 640 | 800 | 4 | 0.06 | 8 | 0.9944 | 598 | 11.06 | 29.92 | 185.6 | 46.1 |
| 10M-512 | 1024 | 800 | 4 | 0.06 | 8 | 0.9961 | 394 | 16.79 | 43.96 | 273.5 | 68.1 |
| 10M-512 | 10 | 200 | 4 | 0.06 | 1 | 0.8861 | untimed | — | — | 10.0 | 2.2 |
| 10M-512 | 20 | 200 | 4 | 0.06 | 1 | 0.9308 | untimed | — | — | 13.8 | 3.2 |
| 10M-512 | 40 | 200 | 4 | 0.06 | 1 | 0.9603 | untimed | — | — | 20.7 | 4.9 |
| 10M-512 | 80 | 200 | 4 | 0.06 | 1 | 0.9761 | untimed | — | — | 34.7 | 8.4 |
| 10M-512 | 160 | 200 | 4 | 0.06 | 1 | 0.9827 | untimed | — | — | 59.3 | 14.6 |
| 10M-512 | 320 | 200 | 4 | 0.06 | 1 | 0.9855 | untimed | — | — | 104.6 | 25.9 |
| 10M-512 | 640 | 200 | 4 | 0.06 | 1 | 0.9875 | untimed | — | — | 185.6 | 46.1 |
| 10M-512 | 1024 | 200 | 4 | 0.06 | 1 | 0.9892 | untimed | — | — | 273.5 | 68.1 |

