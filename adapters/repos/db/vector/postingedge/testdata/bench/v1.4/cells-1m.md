## Cells at their best configuration

Each cell at the timed configuration that gives it the most QPS at each recall, read off its ef curve as in the frontier table and given with its rescore limit and source margin, and with its load batch where that is not the default. The ratios compare with the control (1M) and with HNSW, each at its own best.

| threads | cell | code B/vector | index heap MB | QPS@0.90 | QPS@0.95 | QPS@0.99 | cell / control @0.90 | cell / control @0.95 | cell / control @0.99 | HNSW / cell @0.90 | HNSW / cell @0.95 | HNSW / cell @0.99 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1M | 104 | 165.1 | 1567 (200, 0.06) | 1112 (200, 0.06) | 412 (200, 0.06) | 1.00x | 1.00x | 1.00x | 1.99x | 1.53x | 0.77x |
| 1 | HNSW | — | 335.0 | 3111 | 1702 | 316 | — | — | — | — | — | — |
| 8 | 1M | 104 | 165.1 | 10978 (200, 0.06) | 7778 (200, 0.06) | 2974 (200, 0.06) | 1.00x | 1.00x | 1.00x | 1.55x | 1.26x | 0.62x |
| 8 | HNSW | — | 335.0 | 16969 | 9818 | 1850 | — | — | — | — | — | — |

## QPS at each recall level

Each cell at the timed configuration that gives it the most QPS at the level, read off its ef curve, with its ratio to the control (1M). A QPS given with a recall is that of the fastest point of the curve, which has that recall and not the one of the level: the cell has no cheaper configuration.

Threads: 1

| recall | 1M | HNSW |
|---|---|---|
| 0.90 | 1567 at 0.9061 | 3111 |
| 0.92 | 1426 | 2591 |
| 0.93 | 1332 | 2291 |
| 0.94 | 1245 | 1976 |
| 0.95 | 1112 | 1702 |
| 0.96 | 985 | 1463 |
| 0.97 | 836 | 1089 |
| 0.98 | 656 | 652 |
| 0.99 | 412 | 316 |

Threads: 8

| recall | 1M | HNSW |
|---|---|---|
| 0.90 | 10978 at 0.9061 | 16969 |
| 0.92 | 10028 | 14662 |
| 0.93 | 9396 | 13106 |
| 0.94 | 8804 | 11361 |
| 0.95 | 7778 | 9818 |
| 0.96 | 6806 | 8437 |
| 0.97 | 5792 | 6402 |
| 0.98 | 4690 | 3970 |
| 0.99 | 2974 | 1850 |

## Cells: build and memory

Code bytes include the header word. Structure is the index heap without the codes. The margin is the one the calibration of the cell took, or the one it was timed with if it was not calibrated, one per rescore limit where they differ. A cell whose postings have scan codes of their own holds two codes per vector, the one of the sidecar and the shorter one the scan reads: its code columns are both together.

| cell | code B/vector | postings | mean posting | P | R | candidates/vector | edge recall | edges/vector | margin | index heap MB | codes MB | structure MB | build s | assign s | edges s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 104 | 4238 | 236 | 256 | 64 | 54565 | 0.9461 | 9.74 | 0.06 | 165.1 | 99.2 | 65.9 | 280 | 0 | 279 |

Memory by component and build by stage. The rest of the index heap is what the components do not account for.

| cell | centroid vectors MB | codes MB | edges MB | postings MB | rest MB | index heap MB | partition s | assign s | encode s | edges s | centroid graph s | build s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 12.4 | 99.2 | 41.0 | 11.5 | 1.0 | 165.1 | 0 | 0 | 1 | 279 | 0.1 | 280 |

## QPS at equal recall

Read off the ef curve of each cell and rescore limit, interpolated linearly in recall and log QPS. The fast end is the best QPS of a measured point with recall of at least 0.92. The ratios compare with the best configuration of the control (1M) and of HNSW at the same recall.

| threads | cell | rescore | batch | margin | QPS@0.90 | QPS@0.92 | QPS@0.93 | QPS@0.94 | QPS@0.95 | QPS@0.96 | QPS@0.97 | QPS@0.98 | QPS@0.99 | fast end | cell / control @0.95 | cell / control @0.99 | HNSW / cell @0.95 | HNSW / cell @0.99 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1M | 200 | 4 | 0.06 | 1567 | 1426 | 1332 | 1245 | 1112 | 985 | 836 | 656 | 412 | 1233 | 1.00x | 1.00x | 1.53x | 0.77x |
| 1 | 1M | 400 | 4 | 0.06 | 1267 | 1170 | 1105 | 1044 | 941 | 841 | 727 | 592 | 403 | 1035 | 0.85x | 0.98x | 1.81x | 0.78x |
| 1 | HNSW | best | — | — | 3111 | 2591 | 2291 | 1976 | 1702 | 1463 | 1089 | 652 | 316 | — | — | — | — | — |
| 8 | 1M | 200 | 4 | 0.06 | 10978 | 10028 | 9396 | 8804 | 7778 | 6806 | 5792 | 4690 | 2974 | 8724 | 1.00x | 1.00x | 1.26x | 0.62x |
| 8 | 1M | 400 | 4 | 0.06 | 8800 | 8183 | 7767 | 7371 | 6669 | 5983 | 5191 | 4233 | 2895 | 7314 | 0.86x | 0.97x | 1.47x | 0.64x |
| 8 | HNSW | best | — | — | 16969 | 14662 | 13106 | 11361 | 9818 | 8437 | 6402 | 3970 | 1850 | — | — | — | — | — |

## Recall reached by raising ef

With the source margin of the cell. A configuration that stays below 0.99 over its sweep gets untimed passes that keep doubling ef until it gets there or a doubling adds less than 0.0005. The ceiling is the best recall measured, and it saturates at the smallest ef within 0.0005 of it. Without margin is the recall at the largest ef measured, with no source margin.

| cell | rescore | batch | margin | first ef with recall ≥ 0.99 | recall there | ceiling | saturates at ef | largest ef | recall there | without margin |
|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 160 | 0.9904 | 0.9961 | 1024 | 1024 | 0.9961 | — |
| 1M | 400 | 4 | 0.06 | 160 | 0.9914 | 0.9978 | 1024 | 1024 | 0.9978 | — |

## Work and time per query at equal recall

One thread, read along the ef curve of each configuration like the QPS. The recall column is the level, or the recall of the first point of the curve where the level is below it: the row is then that point, the cheapest search of the configuration. Sources expanded are the scanned vectors within the source margin, the ones whose edges are followed. The times come from the instrumented pass, which is slower than the timed ones, and the rest is the time no phase accounts for. Postings loaded are also given against the control at the same rescore limit, or at its largest one where the control was not timed at that limit.

### Recall 0.90

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.9061 | 10 | 9 | 2 | 2199 | 684 | 6840 | 3485 | 126 | 120 | 243 | 3 | 123 | 75 | 689 | 1.00x |
| 1M | 400 | 4 | 0.06 | 0.9061 | 10 | 9 | 2 | 2199 | 684 | 6840 | 3485 | 118 | 138 | 236 | 3 | 228 | 84 | 806 | 1.00x |

### Recall 0.92

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.92 | 13 | 10 | 2 | 2455 | 821 | 8239 | 4099 | 123 | 130 | 284 | 3 | 122 | 82 | 748 | 1.00x |
| 1M | 400 | 4 | 0.06 | 0.92 | 13 | 10 | 2 | 2454 | 821 | 8234 | 4097 | 120 | 151 | 279 | 3 | 230 | 94 | 880 | 1.00x |

### Recall 0.93

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.93 | 16 | 11 | 2 | 2657 | 936 | 9419 | 4607 | 122 | 139 | 317 | 3 | 122 | 88 | 794 | 1.00x |
| 1M | 400 | 4 | 0.06 | 0.93 | 16 | 11 | 2 | 2655 | 935 | 9411 | 4604 | 122 | 161 | 315 | 3 | 232 | 101 | 938 | 1.00x |

### Recall 0.94

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.94 | 19 | 12 | 3 | 2875 | 1068 | 10769 | 5178 | 120 | 148 | 355 | 4 | 121 | 94 | 842 | 1.00x |
| 1M | 400 | 4 | 0.06 | 0.94 | 19 | 12 | 3 | 2873 | 1066 | 10755 | 5172 | 124 | 172 | 356 | 4 | 233 | 109 | 999 | 1.00x |

### Recall 0.95

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.95 | 25 | 13 | 3 | 3319 | 1299 | 13153 | 6161 | 122 | 167 | 423 | 5 | 121 | 109 | 950 | 1.00x |
| 1M | 400 | 4 | 0.06 | 0.95 | 25 | 13 | 3 | 3314 | 1296 | 13125 | 6150 | 126 | 193 | 425 | 5 | 235 | 124 | 1113 | 1.00x |

### Recall 0.96

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.96 | 33 | 16 | 4 | 3872 | 1596 | 16238 | 7400 | 124 | 191 | 510 | 6 | 121 | 127 | 1082 | 1.00x |
| 1M | 400 | 4 | 0.06 | 0.96 | 33 | 16 | 4 | 3866 | 1592 | 16204 | 7386 | 128 | 219 | 514 | 6 | 237 | 143 | 1251 | 1.00x |

### Recall 0.97

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.97 | 47 | 19 | 5 | 4732 | 2059 | 21062 | 9264 | 127 | 227 | 644 | 8 | 122 | 156 | 1287 | 1.00x |
| 1M | 400 | 4 | 0.06 | 0.97 | 47 | 19 | 5 | 4713 | 2049 | 20957 | 9224 | 131 | 257 | 647 | 8 | 238 | 173 | 1459 | 1.00x |

### Recall 0.98

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.98 | 73 | 25 | 6 | 6280 | 2898 | 29821 | 12487 | 132 | 292 | 882 | 12 | 124 | 210 | 1654 | 1.00x |
| 1M | 400 | 4 | 0.06 | 0.98 | 72 | 25 | 6 | 6232 | 2871 | 29544 | 12388 | 137 | 324 | 881 | 12 | 241 | 226 | 1824 | 1.00x |

### Recall 0.99

| cell | rescore | batch | margin | recall | ef | postings loaded | rounds | vectors scanned | sources expanded | edges followed | endpoints scored | centroid µs | scan µs | endpoints µs | select µs | rescore µs | rest µs | total µs | loads / control |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.99 | 155 | 44 | 11 | 10697 | 5334 | 55409 | 21244 | 144 | 477 | 1540 | 26 | 127 | 360 | 2674 | 1.00x |
| 1M | 400 | 4 | 0.06 | 0.99 | 143 | 41 | 10 | 10138 | 5018 | 52080 | 20145 | 143 | 490 | 1454 | 24 | 245 | 362 | 2722 | 1.00x |

## Disk projection

What a query would read with the postings on disk, counted in memory and read along the ef curve of each configuration like the QPS, the 95th percentiles too. A posting loaded is a read, a round is a batch of reads issued together, so rounds plus one is the serial depth. The candidates of the final rescore are located among the loaded postings: every one of them is a scanned member of a posting the query has read, so the rescore needs no posting of its own, and the full-precision vectors come from as many places as the postings counted here if they are stored by posting.

### Recall 0.90

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.9061 | 8.8 | 17.0 | 2.0 | 4.0 | 2199.1 | 200.0 | 8.8 | 17.0 | 0.0 |
| 1M | 400 | 4 | 0.06 | 0.9061 | 8.8 | 17.0 | 2.0 | 4.0 | 2199.1 | 398.1 | 8.8 | 17.0 | 0.0 |

### Recall 0.92

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.92 | 9.9 | 18.5 | 2.2 | 4.4 | 2454.6 | 200.0 | 9.9 | 18.5 | 0.0 |
| 1M | 400 | 4 | 0.06 | 0.92 | 9.9 | 18.5 | 2.2 | 4.4 | 2453.8 | 398.5 | 9.9 | 18.5 | 0.0 |

### Recall 0.93

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.93 | 10.7 | 19.6 | 2.4 | 4.7 | 2656.6 | 200.0 | 10.7 | 19.6 | 0.0 |
| 1M | 400 | 4 | 0.06 | 0.93 | 10.7 | 19.6 | 2.4 | 4.7 | 2655.2 | 398.8 | 10.7 | 19.6 | 0.0 |

### Recall 0.94

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.94 | 11.6 | 20.8 | 2.6 | 5.0 | 2875.2 | 200.0 | 11.6 | 20.8 | 0.0 |
| 1M | 400 | 4 | 0.06 | 0.94 | 11.6 | 20.8 | 2.6 | 5.0 | 2873.0 | 399.1 | 11.6 | 20.8 | 0.0 |

### Recall 0.95

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.95 | 13.4 | 24.5 | 3.1 | 5.9 | 3319.2 | 200.0 | 13.4 | 24.5 | 0.0 |
| 1M | 400 | 4 | 0.06 | 0.95 | 13.4 | 24.5 | 3.1 | 5.9 | 3314.1 | 399.4 | 13.4 | 24.5 | 0.0 |

### Recall 0.96

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.96 | 15.7 | 29.4 | 3.7 | 7.1 | 3872.2 | 200.0 | 15.6 | 29.4 | 0.0 |
| 1M | 400 | 4 | 0.06 | 0.96 | 15.6 | 29.3 | 3.7 | 7.1 | 3866.2 | 399.7 | 15.6 | 29.3 | 0.0 |

### Recall 0.97

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.97 | 19.2 | 36.1 | 4.5 | 8.8 | 4731.9 | 200.0 | 19.2 | 36.1 | 0.0 |
| 1M | 400 | 4 | 0.06 | 0.97 | 19.1 | 36.0 | 4.5 | 8.7 | 4712.7 | 399.9 | 19.1 | 36.0 | 0.0 |

### Recall 0.98

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.98 | 25.5 | 46.4 | 6.1 | 11.3 | 6279.8 | 200.0 | 25.5 | 46.4 | 0.0 |
| 1M | 400 | 4 | 0.06 | 0.98 | 25.3 | 46.1 | 6.1 | 11.3 | 6232.2 | 400.0 | 25.3 | 46.1 | 0.0 |

### Recall 0.99

| cell | rescore | batch | margin | recall | postings loaded | p95 | rounds | p95 | vectors scanned | rescored | loaded postings that hold them | p95 | rescored outside the loaded postings, most in a query |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 200 | 4 | 0.06 | 0.99 | 43.7 | 79.0 | 10.7 | 19.5 | 10696.5 | 200.0 | 43.5 | 79.0 | 0.0 |
| 1M | 400 | 4 | 0.06 | 0.99 | 41.4 | 74.8 | 10.1 | 18.5 | 10137.7 | 400.0 | 41.4 | 74.8 | 0.0 |

## Postings loaded at equal recall, untimed configurations

Configurations measured over a sweep of ef without timing: recall and work at each ef, no QPS. Read along the ef curve like the timed ones, and set against the control the same way. The last columns are the largest ef measured and the recall there.

| cell | rescore | batch | margin | loads @0.90 | loads / control @0.90 | loads @0.95 | loads / control @0.95 | loads @0.99 | loads / control @0.99 | largest ef | recall there |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1M | 64 | 4 | 0.06 | 9 | 1.00x | 15 | 1.13x | not reached | — | 1024 | 0.9806 |
| 1M | 200 | 4 | none | 9 | 1.01x | 13 | 0.97x | 42 | 0.97x | 160 | 0.9910 |

## Source margin of each cell

The recall each margin costs at equal ef, against no margin: the worst ef and the ef it happens at. A cell takes the smallest margin within its tolerance, for each rescore limit it was calibrated at.

| cell | rescore | margin | worst recall change | at ef | endpoints scored at that ef, against no margin | taken |
|---|---|---|---|---|---|---|

## Noise

Noisy are the timed points whose passes are more than 10% of their median apart. Outworked are the ones that give at least 2% less QPS than a point that does more work than them: the same threads, load batch, source margin and ef with a larger rescore limit. A stretch of points taken while the machine was slow shows in neither unless it falls behind another curve; a repeat of the cell is what shows it.

| cell | timed points | noisy | which (threads, rescore, batch, margin, ef: QPS of each pass) | outworked | which (threads, batch, margin, ef: rescore and QPS of the point, of the busier one) |
|---|---|---|---|---|---|
| 1M | 32 | 0 | — | 0 | — |

## Repeats

The timed points of a cell against the same points of another run of it, one row per ef curve. The change is the QPS of the cell over that of the other run. Repeated are the points within 2% of the other run; the others are listed by ef. The recall difference is the largest of the curve.

| cell | threads | rescore | batch | margin | points | repeated | median change | the others (ef: change) | recall difference |
|---|---|---|---|---|---|---|---|---|---|
| 1M | 1 | 200 | 4 | 0.06 | 8 | 7 | +0.5% | 160: +2.2% | 0.0000 |
| 1M | 1 | 400 | 4 | 0.06 | 8 | 8 | +0.3% | — | 0.0000 |
| 1M | 8 | 200 | 4 | 0.06 | 8 | 5 | +1.2% | 80: +3.0%; 320: +2.8%; 1024: +2.8% | 0.0000 |
| 1M | 8 | 400 | 4 | 0.06 | 8 | 5 | +1.8% | 40: +2.5%; 640: +2.1%; 1024: +4.1% | 0.0000 |

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

