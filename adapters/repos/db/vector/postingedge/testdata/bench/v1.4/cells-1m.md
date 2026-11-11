## Cells at their best configuration

Each cell at the timed configuration that gives it the most QPS at each recall, read off its ef curve as in the frontier table and given with its rescore limit and source margin, and with its load batch where that is not the default. The ratios compare with the control (1M) and with HNSW, each at its own best.

| threads | cell | code B/vector | index heap MB | QPS@0.90 | QPS@0.95 | QPS@0.99 | cell / control @0.90 | cell / control @0.95 | cell / control @0.99 | HNSW / cell @0.90 | HNSW / cell @0.95 | HNSW / cell @0.99 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1M | 104 | 165.1 | 1567 (200, 0.06) | 1112 (200, 0.06) | 412 (200, 0.06) | 1.00x | 1.00x | 1.00x | 1.99x | 1.53x | 0.77x |
| 1 | 1M-512 | 72 | 134.5 | 1142 (400, 0.06) | 778 (400, 0.06) | 278 (800, 0.06) | 0.73x | 0.70x | 0.67x | 2.72x | 2.19x | 1.13x |
| 1 | HNSW | — | 335.0 | 3111 | 1702 | 316 | — | — | — | — | — | — |
| 8 | 1M | 104 | 165.1 | 10978 (200, 0.06) | 7778 (200, 0.06) | 2974 (200, 0.06) | 1.00x | 1.00x | 1.00x | 1.55x | 1.26x | 0.62x |
| 8 | 1M-512 | 72 | 134.5 | 8492 (400, 0.06) | 5914 (400, 0.06) | 1997 (800, 0.06) | 0.77x | 0.76x | 0.67x | 2.00x | 1.66x | 0.93x |
| 8 | HNSW | — | 335.0 | 16969 | 9818 | 1850 | — | — | — | — | — | — |

## QPS at each recall level

Each cell at the timed configuration that gives it the most QPS at the level, read off its ef curve, with its ratio to the control (1M). A QPS given with a recall is that of the fastest point of the curve, which has that recall and not the one of the level: the cell has no cheaper configuration.

Threads: 1

| recall | 1M | 1M-512 | HNSW |
|---|---|---|---|
| 0.90 | 1567 at 0.9061 | 1142 (0.73x) | 3111 |
| 0.92 | 1426 | 1034 (0.73x) | 2591 |
| 0.93 | 1332 | 943 (0.71x) | 2291 |
| 0.94 | 1245 | 859 (0.69x) | 1976 |
| 0.95 | 1112 | 778 (0.70x) | 1702 |
| 0.96 | 985 | 680 (0.69x) | 1463 |
| 0.97 | 836 | 594 (0.71x) | 1089 |
| 0.98 | 656 | 435 (0.66x) | 652 |
| 0.99 | 412 | 278 (0.67x) | 316 |

Threads: 8

| recall | 1M | 1M-512 | HNSW |
|---|---|---|---|
| 0.90 | 10978 at 0.9061 | 8492 (0.77x) | 16969 |
| 0.92 | 10028 | 7763 (0.77x) | 14662 |
| 0.93 | 9396 | 7111 (0.76x) | 13106 |
| 0.94 | 8804 | 6515 (0.74x) | 11361 |
| 0.95 | 7778 | 5914 (0.76x) | 9818 |
| 0.96 | 6806 | 5133 (0.75x) | 8437 |
| 0.97 | 5792 | 4455 (0.77x) | 6402 |
| 0.98 | 4690 | 3265 (0.70x) | 3970 |
| 0.99 | 2974 | 1997 (0.67x) | 1850 |

## Cells: build and memory

Code bytes include the header word. Structure is the index heap without the codes. The margin is the one the calibration of the cell took, or the one it was timed with if it was not calibrated, one per rescore limit where they differ. A cell whose postings have scan codes of their own holds two codes per vector, the one of the sidecar and the shorter one the scan reads: its code columns are both together.

| cell | code B/vector | postings | mean posting | P | R | candidates/vector | edge recall | edges/vector | margin | index heap MB | codes MB | structure MB | build s | assign s | edges s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 104 | 4238 | 236 | 256 | 64 | 54565 | 0.9461 | 9.74 | 0.06 | 165.1 | 99.2 | 65.9 | 280 | 0 | 279 |
| 1M-512 | 72 | 4238 | 236 | 256 | 256 | 54565 | 0.9593 | 9.74 | 0.06 | 134.5 | 68.7 | 65.9 | 290 | 0 | 288 |

Memory by component and build by stage. The rest of the index heap is what the components do not account for.

| cell | centroid vectors MB | codes MB | edges MB | postings MB | rest MB | index heap MB | partition s | assign s | encode s | edges s | centroid graph s | build s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 12.4 | 99.2 | 41.0 | 11.5 | 1.0 | 165.1 | 0 | 0 | 1 | 279 | 0.1 | 280 |
| 1M-512 | 12.4 | 68.7 | 41.0 | 11.5 | 1.0 | 134.5 | 0 | 0 | 1 | 288 | 0.2 | 290 |

## QPS at equal recall

Read off the ef curve of each cell and rescore limit, interpolated linearly in recall and log QPS. The fast end is the best QPS of a measured point with recall of at least 0.92. The ratios compare with the best configuration of the control (1M) and of HNSW at the same recall.

| threads | cell | rescore | batch | margin | QPS@0.90 | QPS@0.92 | QPS@0.93 | QPS@0.94 | QPS@0.95 | QPS@0.96 | QPS@0.97 | QPS@0.98 | QPS@0.99 | fast end | cell / control @0.95 | cell / control @0.99 | HNSW / cell @0.95 | HNSW / cell @0.99 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1M | 200 | 4 | 0.06 | 1567 | 1426 | 1332 | 1245 | 1112 | 985 | 836 | 656 | 412 | 1233 | 1.00x | 1.00x | 1.53x | 0.77x |
| 1 | 1M | 400 | 4 | 0.06 | 1267 | 1170 | 1105 | 1044 | 941 | 841 | 727 | 592 | 403 | 1035 | 0.85x | 0.98x | 1.81x | 0.78x |
| 1 | 1M-512 | 400 | 4 | 0.06 | 1142 | 1034 | 943 | 859 | 778 | 680 | 594 | 435 | 184 | 796 | 0.70x | 0.45x | 2.19x | 1.71x |
| 1 | 1M-512 | 800 | 4 | 0.06 | 894 | 828 | 770 | 715 | 661 | 588 | 522 | 416 | 278 | 671 | 0.59x | 0.67x | 2.57x | 1.13x |
| 1 | HNSW | best | — | — | 3111 | 2591 | 2291 | 1976 | 1702 | 1463 | 1089 | 652 | 316 | — | — | — | — | — |
| 8 | 1M | 200 | 4 | 0.06 | 10978 | 10028 | 9396 | 8804 | 7778 | 6806 | 5792 | 4690 | 2974 | 8724 | 1.00x | 1.00x | 1.26x | 0.62x |
| 8 | 1M | 400 | 4 | 0.06 | 8800 | 8183 | 7767 | 7371 | 6669 | 5983 | 5191 | 4233 | 2895 | 7314 | 0.86x | 0.97x | 1.47x | 0.64x |
| 8 | 1M-512 | 400 | 4 | 0.06 | 8492 | 7763 | 7111 | 6515 | 5914 | 5133 | 4455 | 3265 | 1329 | 6058 | 0.76x | 0.45x | 1.66x | 1.39x |
| 8 | 1M-512 | 800 | 4 | 0.06 | 6249 | 5844 | 5436 | 5057 | 4678 | 4154 | 3688 | 2953 | 1997 | 4745 | 0.60x | 0.67x | 2.10x | 0.93x |
| 8 | HNSW | best | — | — | 16969 | 14662 | 13106 | 11361 | 9818 | 8437 | 6402 | 3970 | 1850 | — | — | — | — | — |

## Recall reached by raising ef

With the source margin of the cell. A configuration that stays below 0.99 over its sweep gets untimed passes that keep doubling ef until it gets there or a doubling adds less than 0.0005. The ceiling is the best recall measured, and it saturates at the smallest ef within 0.0005 of it. Without margin is the recall at the largest ef measured, with no source margin.

| cell | rescore | batch | margin | first ef with recall ≥ 0.99 | recall there | ceiling | saturates at ef | largest ef | recall there | without margin |
|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 160 | 0.9904 | 0.9961 | 1024 | 1024 | 0.9961 | — |
| 1M | 400 | 4 | 0.06 | 160 | 0.9914 | 0.9978 | 1024 | 1024 | 0.9978 | — |
| 1M-512 | 400 | 4 | 0.06 | 640 | 0.9904 | 0.9913 | 1024 | 1024 | 0.9913 | — |
| 1M-512 | 800 | 4 | 0.06 | 320 | 0.9930 | 0.9959 | 1024 | 1024 | 0.9959 | — |

## Work and time per query at equal recall

One thread, read along the ef curve of each configuration like the QPS. The recall column is the level, or the recall of the first point of the curve where the level is below it: the row is then that point, the cheapest search of the configuration. Sources expanded are the scanned vectors within the source margin, the ones whose edges are followed. The times come from the instrumented pass, which is slower than the timed ones, and the rest is the time no phase accounts for. Postings loaded are also given against the control at the same rescore limit, or at its largest one where the control was not timed at that limit.

### Recall 0.90

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.9061 | 10 | 9 | 2 | 2199 | 684 | 6840 | 3485 | 126 | 120 | 243 | 3 | 123 | 75 | 689 | 1.00x |
| 1M | 400 | 4 | 0.06 | 0.9061 | 10 | 9 | 2 | 2199 | 684 | 6840 | 3485 | 118 | 138 | 236 | 3 | 228 | 84 | 806 | 1.00x |
| 1M-512 | 400 | 4 | 0.06 | 0.90 | 15 | 11 | 2 | 2729 | 864 | 8660 | 4369 | 137 | 161 | 267 | 4 | 247 | 104 | 921 | 1.24x |
| 1M-512 | 800 | 4 | 0.06 | 0.90 | 15 | 11 | 2 | 2725 | 862 | 8639 | 4360 | 125 | 187 | 257 | 3 | 447 | 127 | 1149 | 1.24x |

### Recall 0.92

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.92 | 13 | 10 | 2 | 2455 | 821 | 8239 | 4099 | 123 | 130 | 284 | 3 | 122 | 82 | 748 | 1.00x |
| 1M | 400 | 4 | 0.06 | 0.92 | 13 | 10 | 2 | 2454 | 821 | 8234 | 4097 | 120 | 151 | 279 | 3 | 230 | 94 | 880 | 1.00x |
| 1M-512 | 400 | 4 | 0.06 | 0.92 | 21 | 13 | 3 | 3148 | 1088 | 10945 | 5360 | 145 | 181 | 331 | 4 | 252 | 120 | 1035 | 1.28x |
| 1M-512 | 800 | 4 | 0.06 | 0.92 | 21 | 13 | 3 | 3135 | 1082 | 10885 | 5334 | 127 | 211 | 316 | 4 | 452 | 140 | 1253 | 1.28x |

### Recall 0.93

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.93 | 16 | 11 | 2 | 2657 | 936 | 9419 | 4607 | 122 | 139 | 317 | 3 | 122 | 88 | 794 | 1.00x |
| 1M | 400 | 4 | 0.06 | 0.93 | 16 | 11 | 2 | 2655 | 935 | 9411 | 4604 | 122 | 161 | 315 | 3 | 232 | 101 | 938 | 1.00x |
| 1M-512 | 400 | 4 | 0.06 | 0.93 | 26 | 14 | 3 | 3586 | 1294 | 13074 | 6255 | 143 | 200 | 386 | 5 | 249 | 134 | 1125 | 1.35x |
| 1M-512 | 800 | 4 | 0.06 | 0.93 | 26 | 14 | 3 | 3568 | 1286 | 12987 | 6219 | 128 | 233 | 370 | 5 | 453 | 154 | 1349 | 1.34x |

### Recall 0.94

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.94 | 19 | 12 | 3 | 2875 | 1068 | 10769 | 5178 | 120 | 148 | 355 | 4 | 121 | 94 | 842 | 1.00x |
| 1M | 400 | 4 | 0.06 | 0.94 | 19 | 12 | 3 | 2873 | 1066 | 10755 | 5172 | 124 | 172 | 356 | 4 | 233 | 109 | 999 | 1.00x |
| 1M-512 | 400 | 4 | 0.06 | 0.94 | 33 | 17 | 4 | 4084 | 1540 | 15617 | 7300 | 141 | 222 | 450 | 6 | 246 | 151 | 1222 | 1.42x |
| 1M-512 | 800 | 4 | 0.06 | 0.94 | 33 | 16 | 4 | 4061 | 1528 | 15495 | 7251 | 128 | 258 | 432 | 6 | 454 | 168 | 1453 | 1.42x |

### Recall 0.95

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.95 | 25 | 13 | 3 | 3319 | 1299 | 13153 | 6161 | 122 | 167 | 423 | 5 | 121 | 109 | 950 | 1.00x |
| 1M | 400 | 4 | 0.06 | 0.95 | 25 | 13 | 3 | 3314 | 1296 | 13125 | 6150 | 126 | 193 | 425 | 5 | 235 | 124 | 1113 | 1.00x |
| 1M-512 | 400 | 4 | 0.06 | 0.95 | 42 | 19 | 5 | 4707 | 1852 | 18865 | 8602 | 140 | 249 | 531 | 8 | 244 | 170 | 1344 | 1.42x |
| 1M-512 | 800 | 4 | 0.06 | 0.95 | 41 | 19 | 4 | 4656 | 1828 | 18614 | 8503 | 129 | 288 | 509 | 8 | 455 | 186 | 1575 | 1.41x |

### Recall 0.96

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.96 | 33 | 16 | 4 | 3872 | 1596 | 16238 | 7400 | 124 | 191 | 510 | 6 | 121 | 127 | 1082 | 1.00x |
| 1M | 400 | 4 | 0.06 | 0.96 | 33 | 16 | 4 | 3866 | 1592 | 16204 | 7386 | 128 | 219 | 514 | 6 | 237 | 143 | 1251 | 1.00x |
| 1M-512 | 400 | 4 | 0.06 | 0.96 | 57 | 23 | 6 | 5746 | 2352 | 24072 | 10618 | 141 | 293 | 659 | 10 | 245 | 204 | 1560 | 1.49x |
| 1M-512 | 800 | 4 | 0.06 | 0.96 | 55 | 23 | 5 | 5636 | 2299 | 23513 | 10405 | 132 | 334 | 627 | 10 | 460 | 218 | 1790 | 1.46x |

### Recall 0.97

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.97 | 47 | 19 | 5 | 4732 | 2059 | 21062 | 9264 | 127 | 227 | 644 | 8 | 122 | 156 | 1287 | 1.00x |
| 1M | 400 | 4 | 0.06 | 0.97 | 47 | 19 | 5 | 4713 | 2049 | 20957 | 9224 | 131 | 257 | 647 | 8 | 238 | 173 | 1459 | 1.00x |
| 1M-512 | 400 | 4 | 0.06 | 0.97 | 77 | 29 | 7 | 7013 | 2987 | 30718 | 13108 | 142 | 345 | 819 | 14 | 245 | 245 | 1811 | 1.49x |
| 1M-512 | 800 | 4 | 0.06 | 0.97 | 74 | 28 | 7 | 6823 | 2890 | 29702 | 12732 | 136 | 388 | 774 | 13 | 464 | 255 | 2033 | 1.45x |

### Recall 0.98

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.98 | 73 | 25 | 6 | 6280 | 2898 | 29821 | 12487 | 132 | 292 | 882 | 12 | 124 | 210 | 1654 | 1.00x |
| 1M | 400 | 4 | 0.06 | 0.98 | 72 | 25 | 6 | 6232 | 2871 | 29544 | 12388 | 137 | 324 | 881 | 12 | 241 | 226 | 1824 | 1.00x |
| 1M-512 | 400 | 4 | 0.06 | 0.98 | 133 | 43 | 10 | 10440 | 4698 | 48682 | 19486 | 159 | 498 | 1260 | 25 | 256 | 377 | 2582 | 1.68x |
| 1M-512 | 800 | 4 | 0.06 | 0.98 | 118 | 39 | 9 | 9534 | 4239 | 43847 | 17808 | 142 | 512 | 1115 | 21 | 479 | 345 | 2627 | 1.54x |

### Recall 0.99

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.99 | 155 | 44 | 11 | 10697 | 5334 | 55409 | 21244 | 144 | 477 | 1540 | 26 | 127 | 360 | 2674 | 1.00x |
| 1M | 400 | 4 | 0.06 | 0.99 | 143 | 41 | 10 | 10138 | 5018 | 52080 | 20145 | 143 | 490 | 1454 | 24 | 245 | 362 | 2722 | 1.00x |
| 1M-512 | 400 | 4 | 0.06 | 0.99 | 508 | 119 | 30 | 29064 | 14420 | 151263 | 52019 | 156 | 1230 | 3405 | 102 | 253 | 979 | 6138 | 2.88x |
| 1M-512 | 800 | 4 | 0.06 | 0.99 | 241 | 67 | 16 | 16289 | 7686 | 80132 | 29980 | 149 | 794 | 1921 | 46 | 488 | 571 | 3985 | 1.61x |

## Disk projection

What a query would read with the postings on disk, counted in memory and read along the ef curve of each configuration like the QPS, the 95th percentiles too. A posting loaded is a read, a round is a batch of reads issued together, so rounds plus one is the serial depth. The candidates of the final rescore are located among the loaded postings: every one of them is a scanned member of a posting the query has read, so the rescore needs no posting of its own, and the full-precision vectors come from as many places as the postings counted here if they are stored by posting.

### Recall 0.90

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.9061 | 8.8 | 17.0 | 2.0 | 4.0 | 2199.1 | 200.0 | 8.8 | 17.0 | 0.0 |
| 1M | 400 | 4 | 0.06 | 0.9061 | 8.8 | 17.0 | 2.0 | 4.0 | 2199.1 | 398.1 | 8.8 | 17.0 | 0.0 |
| 1M-512 | 400 | 4 | 0.06 | 0.90 | 11.0 | 19.3 | 2.5 | 4.6 | 2728.9 | 398.8 | 11.0 | 19.3 | 0.0 |
| 1M-512 | 800 | 4 | 0.06 | 0.90 | 11.0 | 19.2 | 2.5 | 4.6 | 2725.1 | 792.9 | 11.0 | 19.2 | 0.0 |

### Recall 0.92

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.92 | 9.9 | 18.5 | 2.2 | 4.4 | 2454.6 | 200.0 | 9.9 | 18.5 | 0.0 |
| 1M | 400 | 4 | 0.06 | 0.92 | 9.9 | 18.5 | 2.2 | 4.4 | 2453.8 | 398.5 | 9.9 | 18.5 | 0.0 |
| 1M-512 | 400 | 4 | 0.06 | 0.92 | 12.7 | 21.6 | 2.9 | 5.2 | 3147.9 | 399.4 | 12.7 | 21.6 | 0.0 |
| 1M-512 | 800 | 4 | 0.06 | 0.92 | 12.6 | 21.5 | 2.9 | 5.1 | 3135.2 | 796.6 | 12.6 | 21.5 | 0.0 |

### Recall 0.93

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.93 | 10.7 | 19.6 | 2.4 | 4.7 | 2656.6 | 200.0 | 10.7 | 19.6 | 0.0 |
| 1M | 400 | 4 | 0.06 | 0.93 | 10.7 | 19.6 | 2.4 | 4.7 | 2655.2 | 398.8 | 10.7 | 19.6 | 0.0 |
| 1M-512 | 400 | 4 | 0.06 | 0.93 | 14.5 | 25.1 | 3.4 | 6.0 | 3585.6 | 399.6 | 14.5 | 25.1 | 0.0 |
| 1M-512 | 800 | 4 | 0.06 | 0.93 | 14.4 | 25.0 | 3.3 | 6.0 | 3568.1 | 797.6 | 14.4 | 25.0 | 0.0 |

### Recall 0.94

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.94 | 11.6 | 20.8 | 2.6 | 5.0 | 2875.2 | 200.0 | 11.6 | 20.8 | 0.0 |
| 1M | 400 | 4 | 0.06 | 0.94 | 11.6 | 20.8 | 2.6 | 5.0 | 2873.0 | 399.1 | 11.6 | 20.8 | 0.0 |
| 1M-512 | 400 | 4 | 0.06 | 0.94 | 16.5 | 29.2 | 3.9 | 7.0 | 4084.2 | 399.8 | 16.5 | 29.2 | 0.0 |
| 1M-512 | 800 | 4 | 0.06 | 0.94 | 16.4 | 29.0 | 3.8 | 7.0 | 4060.7 | 798.6 | 16.4 | 29.0 | 0.0 |

### Recall 0.95

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.95 | 13.4 | 24.5 | 3.1 | 5.9 | 3319.2 | 200.0 | 13.4 | 24.5 | 0.0 |
| 1M | 400 | 4 | 0.06 | 0.95 | 13.4 | 24.5 | 3.1 | 5.9 | 3314.1 | 399.4 | 13.4 | 24.5 | 0.0 |
| 1M-512 | 400 | 4 | 0.06 | 0.95 | 19.1 | 34.2 | 4.5 | 8.3 | 4707.1 | 399.9 | 19.1 | 34.2 | 0.0 |
| 1M-512 | 800 | 4 | 0.06 | 0.95 | 18.9 | 33.8 | 4.5 | 8.2 | 4655.8 | 799.5 | 18.9 | 33.8 | 0.0 |

### Recall 0.96

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.96 | 15.7 | 29.4 | 3.7 | 7.1 | 3872.2 | 200.0 | 15.6 | 29.4 | 0.0 |
| 1M | 400 | 4 | 0.06 | 0.96 | 15.6 | 29.3 | 3.7 | 7.1 | 3866.2 | 399.7 | 15.6 | 29.3 | 0.0 |
| 1M-512 | 400 | 4 | 0.06 | 0.96 | 23.3 | 41.9 | 5.6 | 10.2 | 5745.6 | 400.0 | 23.3 | 41.9 | 0.0 |
| 1M-512 | 800 | 4 | 0.06 | 0.96 | 22.9 | 41.1 | 5.5 | 10.0 | 5636.2 | 799.7 | 22.9 | 41.1 | 0.0 |

### Recall 0.97

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.97 | 19.2 | 36.1 | 4.5 | 8.8 | 4731.9 | 200.0 | 19.2 | 36.1 | 0.0 |
| 1M | 400 | 4 | 0.06 | 0.97 | 19.1 | 36.0 | 4.5 | 8.7 | 4712.7 | 399.9 | 19.1 | 36.0 | 0.0 |
| 1M-512 | 400 | 4 | 0.06 | 0.97 | 28.5 | 51.5 | 6.9 | 12.6 | 7013.3 | 400.0 | 28.5 | 51.5 | 0.0 |
| 1M-512 | 800 | 4 | 0.06 | 0.97 | 27.7 | 50.1 | 6.7 | 12.3 | 6823.0 | 799.9 | 27.7 | 50.1 | 0.0 |

### Recall 0.98

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.98 | 25.5 | 46.4 | 6.1 | 11.3 | 6279.8 | 200.0 | 25.5 | 46.4 | 0.0 |
| 1M | 400 | 4 | 0.06 | 0.98 | 25.3 | 46.1 | 6.1 | 11.3 | 6232.2 | 400.0 | 25.3 | 46.1 | 0.0 |
| 1M-512 | 400 | 4 | 0.06 | 0.98 | 42.6 | 75.0 | 10.4 | 18.5 | 10440.1 | 400.0 | 42.5 | 75.0 | 0.0 |
| 1M-512 | 800 | 4 | 0.06 | 0.98 | 38.8 | 68.9 | 9.5 | 17.0 | 9534.2 | 800.0 | 38.8 | 68.9 | 0.0 |

### Recall 0.99

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.99 | 43.7 | 79.0 | 10.7 | 19.5 | 10696.5 | 200.0 | 43.5 | 79.0 | 0.0 |
| 1M | 400 | 4 | 0.06 | 0.99 | 41.4 | 74.8 | 10.1 | 18.5 | 10137.7 | 400.0 | 41.4 | 74.8 | 0.0 |
| 1M-512 | 400 | 4 | 0.06 | 0.99 | 119.3 | 201.6 | 29.6 | 50.1 | 29063.7 | 400.0 | 93.8 | 161.6 | 0.0 |
| 1M-512 | 800 | 4 | 0.06 | 0.99 | 66.6 | 114.5 | 16.4 | 28.4 | 16288.5 | 800.0 | 66.6 | 114.5 | 0.0 |

## Postings loaded at equal recall, untimed configurations

Configurations measured over a sweep of ef without timing: recall and work at each ef, no QPS. Read along the ef curve like the timed ones, and set against the control the same way. The last columns are the largest ef measured and the recall there.

| cell | rescore | batch | margin | loads @0.90 | loads / control @0.90 | loads @0.95 | loads / control @0.95 | loads @0.99 | loads / control @0.99 | largest ef | recall there |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 64 | 4 | 0.06 | 9 | 1.00x | 15 | 1.13x | not reached | — | 1024 | 0.9806 |
| 1M | 200 | 4 | none | 9 | 1.01x | 13 | 0.97x | 42 | 0.97x | 160 | 0.9910 |
| 1M-512 | 200 | 4 | 0.06 | 11 | 1.25x | 21 | 1.57x | not reached | — | 1024 | 0.9772 |

## Source margin of each cell

The recall each margin costs at equal ef, against no margin: the worst ef and the ef it happens at. A cell takes the smallest margin within its tolerance, for each rescore limit it was calibrated at.

| cell | rescore | margin | worst recall change | at ef | endpoints scored at that ef, against no margin | taken |
|---|---|---|---|---|---|---|

## Noise

Noisy are the timed points whose passes are more than 10% of their median apart. Outworked are the ones that give at least 2% less QPS than a point that does more work than them: the same threads, load batch, source margin and ef with a larger rescore limit. A stretch of points taken while the machine was slow shows in neither unless it falls behind another curve; a repeat of the cell is what shows it.

| cell | timed points | noisy | which (threads, rescore, batch, margin, ef: QPS of each pass) | outworked | which (threads, batch, margin, ef: rescore and QPS of the point, of the busier one) |
|---|---|---|---|---|---|
| 1M | 32 | 0 | — | 0 | — |
| 1M-512 | 32 | 0 | — | 0 | — |

## Repeats

The timed points of a cell against the same points of another run of it, one row per ef curve. The change is the QPS of the cell over that of the other run. Repeated are the points within 2% of the other run; the others are listed by ef. The recall difference is the largest of the curve.

| cell | threads | rescore | batch | margin | points | repeated | median change | the others (ef: change) | recall difference |
|---|---|---|---|---|---|---|---|---|---|
| 1M | 1 | 200 | 4 | 0.06 | 8 | 7 | +0.5% | 160: +2.2% | 0.0000 |
| 1M | 1 | 400 | 4 | 0.06 | 8 | 8 | +0.3% | — | 0.0000 |
| 1M | 8 | 200 | 4 | 0.06 | 8 | 5 | +1.2% | 80: +3.0%; 320: +2.8%; 1024: +2.8% | 0.0000 |
| 1M | 8 | 400 | 4 | 0.06 | 8 | 5 | +1.8% | 40: +2.5%; 640: +2.1%; 1024: +4.1% | 0.0000 |
| 1M-512 | 1 | 400 | 4 | 0.06 | 8 | 4 | -2.0% | 20: -3.9%; 40: -5.5%; 80: -2.1%; 160: -2.5% | 0.0000 |
| 1M-512 | 1 | 800 | 4 | 0.06 | 8 | 4 | +1.7% | 10: +3.0%; 20: +2.6%; 40: +2.8%; 80: +2.4% | 0.0000 |
| 1M-512 | 8 | 400 | 4 | 0.06 | 8 | 0 | +9.3% | 10: +9.6%; 20: +12.6%; 40: +8.4%; 80: +9.0%; 160: +8.9%; 320: +8.2%; 640: +11.5%; 1024: +10.5% | 0.0000 |
| 1M-512 | 8 | 800 | 4 | 0.06 | 8 | 8 | -0.0% | — | 0.0000 |

## Operating points

| cell | ef | rescore | batch | margin | threads | recall@10 | QPS | p50 ms | p95 ms | postings loaded | rounds |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 10 | 200 | 4 | 0.06 | 1 | 0.9061 | 1567 | 0.56 | 1.25 | 8.8 | 2.0 |
| 1M | 20 | 200 | 4 | 0.06 | 1 | 0.9414 | 1233 | 0.71 | 1.64 | 11.7 | 2.7 |
| 1M | 40 | 200 | 4 | 0.06 | 1 | 0.9664 | 912 | 0.96 | 2.28 | 17.3 | 4.1 |
| 1M | 80 | 200 | 4 | 0.06 | 1 | 0.9822 | 622 | 1.43 | 3.25 | 27.1 | 6.5 |
| 1M | 160 | 200 | 4 | 0.06 | 1 | 0.9904 | 404 | 2.24 | 4.96 | 44.8 | 10.9 |
| 1M | 320 | 200 | 4 | 0.06 | 1 | 0.9933 | 253 | 3.69 | 7.60 | 74.9 | 18.5 |
| 1M | 640 | 200 | 4 | 0.06 | 1 | 0.9951 | 153 | 6.15 | 12.54 | 127.6 | 31.7 |
| 1M | 1024 | 200 | 4 | 0.06 | 1 | 0.9961 | 108 | 8.74 | 16.82 | 184.2 | 45.8 |
| 1M | 10 | 200 | 4 | 0.06 | 8 | 0.9061 | 10978 | 0.64 | 1.40 | 8.8 | 2.0 |
| 1M | 20 | 200 | 4 | 0.06 | 8 | 0.9414 | 8724 | 0.80 | 1.81 | 11.7 | 2.7 |
| 1M | 40 | 200 | 4 | 0.06 | 8 | 0.9664 | 6249 | 1.10 | 2.62 | 17.3 | 4.1 |
| 1M | 80 | 200 | 4 | 0.06 | 8 | 0.9822 | 4478 | 1.57 | 3.60 | 27.1 | 6.5 |
| 1M | 160 | 200 | 4 | 0.06 | 8 | 0.9904 | 2912 | 2.47 | 5.54 | 44.8 | 10.9 |
| 1M | 320 | 200 | 4 | 0.06 | 8 | 0.9933 | 1840 | 4.00 | 8.39 | 74.9 | 18.5 |
| 1M | 640 | 200 | 4 | 0.06 | 8 | 0.9951 | 1102 | 6.75 | 14.00 | 127.6 | 31.7 |
| 1M | 1024 | 200 | 4 | 0.06 | 8 | 0.9961 | 781 | 9.58 | 18.82 | 184.2 | 45.8 |
| 1M | 10 | 400 | 4 | 0.06 | 1 | 0.9061 | 1267 | 0.71 | 1.43 | 8.8 | 2.0 |
| 1M | 20 | 400 | 4 | 0.06 | 1 | 0.9415 | 1035 | 0.87 | 1.83 | 11.7 | 2.7 |
| 1M | 40 | 400 | 4 | 0.06 | 1 | 0.9665 | 782 | 1.13 | 2.52 | 17.3 | 4.1 |
| 1M | 80 | 400 | 4 | 0.06 | 1 | 0.9825 | 562 | 1.61 | 3.45 | 27.1 | 6.5 |
| 1M | 160 | 400 | 4 | 0.06 | 1 | 0.9914 | 378 | 2.42 | 5.18 | 44.8 | 10.9 |
| 1M | 320 | 400 | 4 | 0.06 | 1 | 0.9948 | 242 | 3.84 | 7.76 | 74.9 | 18.5 |
| 1M | 640 | 400 | 4 | 0.06 | 1 | 0.9968 | 150 | 6.30 | 12.52 | 127.6 | 31.7 |
| 1M | 1024 | 400 | 4 | 0.06 | 1 | 0.9978 | 106 | 8.89 | 17.19 | 184.2 | 45.8 |
| 1M | 10 | 400 | 4 | 0.06 | 8 | 0.9061 | 8800 | 0.82 | 1.62 | 8.8 | 2.0 |
| 1M | 20 | 400 | 4 | 0.06 | 8 | 0.9415 | 7314 | 0.99 | 2.05 | 11.7 | 2.7 |
| 1M | 40 | 400 | 4 | 0.06 | 8 | 0.9665 | 5575 | 1.26 | 2.78 | 17.3 | 4.1 |
| 1M | 80 | 400 | 4 | 0.06 | 8 | 0.9825 | 4023 | 1.77 | 3.81 | 27.1 | 6.5 |
| 1M | 160 | 400 | 4 | 0.06 | 8 | 0.9914 | 2723 | 2.68 | 5.68 | 44.8 | 10.9 |
| 1M | 320 | 400 | 4 | 0.06 | 8 | 0.9948 | 1750 | 4.20 | 8.69 | 74.9 | 18.5 |
| 1M | 640 | 400 | 4 | 0.06 | 8 | 0.9968 | 1075 | 6.95 | 13.97 | 127.6 | 31.7 |
| 1M | 1024 | 400 | 4 | 0.06 | 8 | 0.9978 | 764 | 9.82 | 19.25 | 184.2 | 45.8 |
| 1M | 10 | 64 | 4 | 0.06 | 1 | 0.9033 | untimed | — | — | 8.8 | 2.0 |
| 1M | 20 | 64 | 4 | 0.06 | 1 | 0.9362 | untimed | — | — | 11.7 | 2.7 |
| 1M | 40 | 64 | 4 | 0.06 | 1 | 0.9574 | untimed | — | — | 17.3 | 4.1 |
| 1M | 80 | 64 | 4 | 0.06 | 1 | 0.9688 | untimed | — | — | 27.1 | 6.5 |
| 1M | 160 | 64 | 4 | 0.06 | 1 | 0.9751 | untimed | — | — | 44.8 | 10.9 |
| 1M | 320 | 64 | 4 | 0.06 | 1 | 0.9777 | untimed | — | — | 74.9 | 18.5 |
| 1M | 640 | 64 | 4 | 0.06 | 1 | 0.9796 | untimed | — | — | 127.6 | 31.7 |
| 1M | 1024 | 64 | 4 | 0.06 | 1 | 0.9806 | untimed | — | — | 184.2 | 45.8 |
| 1M | 10 | 200 | 4 | none | 1 | 0.9111 | untimed | — | — | 8.9 | 2.0 |
| 1M | 20 | 200 | 4 | none | 1 | 0.9439 | untimed | — | — | 11.8 | 2.7 |
| 1M | 40 | 200 | 4 | none | 1 | 0.9680 | untimed | — | — | 17.4 | 4.1 |
| 1M | 80 | 200 | 4 | none | 1 | 0.9833 | untimed | — | — | 27.3 | 6.6 |
| 1M | 160 | 200 | 4 | none | 1 | 0.9910 | untimed | — | — | 45.2 | 11.1 |
| 1M-512 | 10 | 400 | 4 | 0.06 | 1 | 0.8742 | 1283 | 0.73 | 1.33 | 9.3 | 2.1 |
| 1M-512 | 20 | 400 | 4 | 0.06 | 1 | 0.9180 | 1054 | 0.87 | 1.66 | 12.3 | 2.8 |
| 1M-512 | 40 | 400 | 4 | 0.06 | 1 | 0.9483 | 796 | 1.14 | 2.33 | 18.4 | 4.4 |
| 1M-512 | 80 | 400 | 4 | 0.06 | 1 | 0.9714 | 583 | 1.56 | 3.25 | 29.3 | 7.1 |
| 1M-512 | 160 | 400 | 4 | 0.06 | 1 | 0.9831 | 392 | 2.35 | 4.77 | 48.7 | 11.9 |
| 1M-512 | 320 | 400 | 4 | 0.06 | 1 | 0.9892 | 253 | 3.74 | 7.18 | 82.9 | 20.5 |
| 1M-512 | 640 | 400 | 4 | 0.06 | 1 | 0.9904 | 157 | 6.11 | 11.39 | 143.1 | 35.5 |
| 1M-512 | 1024 | 400 | 4 | 0.06 | 1 | 0.9913 | 111 | 8.61 | 15.81 | 207.7 | 51.7 |
| 1M-512 | 10 | 400 | 4 | 0.06 | 8 | 0.8742 | 9419 | 0.78 | 1.41 | 9.3 | 2.1 |
| 1M-512 | 20 | 400 | 4 | 0.06 | 8 | 0.9180 | 7900 | 0.92 | 1.74 | 12.3 | 2.8 |
| 1M-512 | 40 | 400 | 4 | 0.06 | 8 | 0.9483 | 6058 | 1.18 | 2.43 | 18.4 | 4.4 |
| 1M-512 | 80 | 400 | 4 | 0.06 | 8 | 0.9714 | 4368 | 1.64 | 3.46 | 29.3 | 7.1 |
| 1M-512 | 160 | 400 | 4 | 0.06 | 8 | 0.9831 | 2939 | 2.46 | 5.04 | 48.7 | 11.9 |
| 1M-512 | 320 | 400 | 4 | 0.06 | 8 | 0.9892 | 1827 | 4.05 | 8.41 | 82.9 | 20.5 |
| 1M-512 | 640 | 400 | 4 | 0.06 | 8 | 0.9904 | 1133 | 6.73 | 12.71 | 143.1 | 35.5 |
| 1M-512 | 1024 | 400 | 4 | 0.06 | 8 | 0.9913 | 800 | 9.50 | 17.86 | 207.7 | 51.7 |
| 1M-512 | 10 | 800 | 4 | 0.06 | 1 | 0.8743 | 977 | 0.97 | 1.59 | 9.3 | 2.1 |
| 1M-512 | 20 | 800 | 4 | 0.06 | 1 | 0.9183 | 839 | 1.12 | 1.93 | 12.3 | 2.8 |
| 1M-512 | 40 | 800 | 4 | 0.06 | 1 | 0.9488 | 671 | 1.38 | 2.58 | 18.4 | 4.4 |
| 1M-512 | 80 | 800 | 4 | 0.06 | 1 | 0.9729 | 505 | 1.83 | 3.51 | 29.3 | 7.1 |
| 1M-512 | 160 | 800 | 4 | 0.06 | 1 | 0.9857 | 356 | 2.59 | 4.99 | 48.7 | 11.9 |
| 1M-512 | 320 | 800 | 4 | 0.06 | 1 | 0.9930 | 234 | 4.05 | 7.49 | 82.9 | 20.5 |
| 1M-512 | 640 | 800 | 4 | 0.06 | 1 | 0.9950 | 148 | 6.51 | 11.84 | 143.1 | 35.5 |
| 1M-512 | 1024 | 800 | 4 | 0.06 | 1 | 0.9959 | 106 | 9.06 | 16.41 | 207.7 | 51.7 |
| 1M-512 | 10 | 800 | 4 | 0.06 | 8 | 0.8743 | 6749 | 1.12 | 1.81 | 9.3 | 2.1 |
| 1M-512 | 20 | 800 | 4 | 0.06 | 8 | 0.9183 | 5916 | 1.26 | 2.15 | 12.3 | 2.8 |
| 1M-512 | 40 | 800 | 4 | 0.06 | 8 | 0.9488 | 4745 | 1.54 | 2.88 | 18.4 | 4.4 |
| 1M-512 | 80 | 800 | 4 | 0.06 | 8 | 0.9729 | 3563 | 2.03 | 3.94 | 29.3 | 7.1 |
| 1M-512 | 160 | 800 | 4 | 0.06 | 8 | 0.9857 | 2540 | 2.89 | 5.59 | 48.7 | 11.9 |
| 1M-512 | 320 | 800 | 4 | 0.06 | 8 | 0.9930 | 1688 | 4.46 | 8.33 | 82.9 | 20.5 |
| 1M-512 | 640 | 800 | 4 | 0.06 | 8 | 0.9950 | 1072 | 7.11 | 13.28 | 143.1 | 35.5 |
| 1M-512 | 1024 | 800 | 4 | 0.06 | 8 | 0.9959 | 756 | 10.03 | 18.79 | 207.7 | 51.7 |
| 1M-512 | 10 | 200 | 4 | 0.06 | 1 | 0.8731 | untimed | — | — | 9.3 | 2.1 |
| 1M-512 | 20 | 200 | 4 | 0.06 | 1 | 0.9161 | untimed | — | — | 12.3 | 2.8 |
| 1M-512 | 40 | 200 | 4 | 0.06 | 1 | 0.9441 | untimed | — | — | 18.4 | 4.4 |
| 1M-512 | 80 | 200 | 4 | 0.06 | 1 | 0.9642 | untimed | — | — | 29.3 | 7.1 |
| 1M-512 | 160 | 200 | 4 | 0.06 | 1 | 0.9719 | untimed | — | — | 48.7 | 11.9 |
| 1M-512 | 320 | 200 | 4 | 0.06 | 1 | 0.9745 | untimed | — | — | 82.9 | 20.5 |
| 1M-512 | 640 | 200 | 4 | 0.06 | 1 | 0.9766 | untimed | — | — | 143.1 | 35.5 |
| 1M-512 | 1024 | 200 | 4 | 0.06 | 1 | 0.9772 | untimed | — | — | 207.7 | 51.7 |

