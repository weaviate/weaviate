"""Estimate of QPS with the prototype kernels, from the phase profile.

usage: kernel_qps_estimate.py <rounds output>

An estimate, not a measurement. For posting-edge the time of a query falls by
what the kernel saves on each endpoint scored (the median saving of the
scattered benchmark: codes fetched from a sidecar of 1 GiB, 8 ahead) and on
each vector scanned (the median saving of the posting benchmark), and rises
by laying out the query (the prototype layout, as measured). For HNSW it
falls by the scattered saving on each distance to a code, with the distances
per query of TestSearchWork. The speed-up of one thread is applied to the
QPS of both thread counts.
"""
import collections, re, statistics, sys

rows = collections.defaultdict(dict)
rnd = 0
for line in open(sys.argv[1]):
    if line.startswith('== round'):
        rnd = int(line.split()[2])
        continue
    m = re.match(r'(Benchmark\S+?)-\d+\s+\d+\s+([\d.]+) ns/op(?:\s+([\d.]+) ns/code)?', line)
    if m:
        rows[m.group(1)][rnd] = float(m.group(3) or m.group(2))


def saving(a, b):
    common = set(rows[a]) & set(rows[b])
    return statistics.median(rows[a][r] - rows[b][r] for r in common)


D = 768
ep = saving(f'BenchmarkScattered/dims={D}/current_distance', f'BenchmarkScattered/dims={D}/prototype_distance')
scan_fused = saving(f'BenchmarkPostingScan/dims={D}/current_distance', f'BenchmarkPostingScan/dims={D}/prototype_distance')
scan_fast = saving(f'BenchmarkPostingScan/dims={D}/current_distance', f'BenchmarkPostingScan/dims={D}/fastscan')
layout = statistics.median(rows[f'BenchmarkQueryLayout/dims={D}/prototype_layout'].values()) / 1000  # µs
print('savings per code (ns): endpoint %.1f, scan fused %.1f, scan fastscan %.1f; layout %.1f µs per query' % (
    ep, scan_fused, scan_fast, layout))

# The phase profile of the report (1 thread, rescoreLimit 200, µs per query),
# and the QPS of the tables.
profile = {
    ('1M', 0.95): dict(endpoints=6161, scanned=3319, total=950, qps=(1112, 7778)),
    ('1M', 0.99): dict(endpoints=21244, scanned=10697, total=2674, qps=(412, 2974)),
    ('10M', 0.95): dict(endpoints=6878, scanned=3306, total=1184, qps=(870, 5968)),
    ('10M', 0.99): dict(endpoints=33021, scanned=13081, total=4043, qps=(261, 1770)),
}
# HNSW: QPS of each build, and distances to codes per query. 1M: TestSearchWork
# on wiki-dpr 1M, interpolated at the recall. 10M: the 1M count at the ef each
# 10M build needs (48 or 64 for 0.95, 256 or 384 for 0.99), an estimate.
hnsw = {
    ('1M', 0.95): [('tarde', (1702, 9818), 2461)],
    ('1M', 0.99): [('tarde', (316, 1850), 9423)],
    ('10M', 0.95): [('tarde', (1859, 13122), 1570), ('mañana', (1828, 12467), 1941)],
    ('10M', 0.99): [('tarde', (448, 3226), 5926), ('mañana', (355, 2363), 8303)],
}

print()
print('| N | recall | threads | PE hoy | PE, kernel fusionado | PE, fusionado + FastScan | HNSW hoy | HNSW, fusionado | ratio hoy | ratio, fusionado | ratio, + FastScan |')
print('|---|---|---|---|---|---|---|---|---|---|---|')
for key, p in profile.items():
    t = p['total']
    t_fused = t - p['endpoints'] * ep / 1000 - p['scanned'] * scan_fused / 1000 + layout
    t_fast = t - p['endpoints'] * ep / 1000 - p['scanned'] * scan_fast / 1000 + layout
    for th in (0, 1):
        pe = p['qps'][th]
        pe_fused, pe_fast = pe * t / t_fused, pe * t / t_fast
        for build, qps, dists in hnsw[key]:
            h = qps[th]
            th_us = 1e6 / qps[0]
            h_new = h * th_us / (th_us - dists * ep / 1000)
            print('| %s | %.2f | %d | %.0f | %.0f (%.2fx) | %.0f (%.2fx) | %.0f (%s) | %.0f (%.2fx) | %.2fx | %.2fx | %.2fx |' % (
                key[0], key[1], (1, 8)[th], pe, pe_fused, t / t_fused, pe_fast, t / t_fast, h, build,
                h_new, th_us / (th_us - dists * ep / 1000), h / pe, h_new / pe_fused, h_new / pe_fast))
