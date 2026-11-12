"""Summary of the interleaved kernel rounds.

usage: kernel_ab_summary.py <rounds output>

Per benchmark: median, min and max over the rounds of ns per code (ns/op,
or the ns/code metric where a benchmark reports it). Per pair of variants of
the same benchmark and width: the median of the per-round ratios, current
over prototype, so that a slow round counts against both.
"""
import collections, re, statistics, sys

rows = collections.defaultdict(dict)  # name -> round -> ns per code
round_no = 0
for line in open(sys.argv[1]):
    if line.startswith('== round'):
        round_no = int(line.split()[2])
        continue
    m = re.match(r'(Benchmark\S+?)-\d+\s+\d+\s+([\d.]+) ns/op(?:\s+([\d.]+) ns/code)?', line)
    if m:
        rows[m.group(1)][round_no] = float(m.group(3) or m.group(2))

print('| benchmark | rounds | median | min | max |')
print('|---|---|---|---|---|')
for name, by_round in rows.items():
    v = list(by_round.values())
    print('| %s | %d | %.2f | %.2f | %.2f |' % (name, len(v), statistics.median(v), min(v), max(v)))

pairs = [
    ('BenchmarkKernel/dims={d}/current_five_calls', 'BenchmarkKernel/dims={d}/fused_neon'),
    ('BenchmarkKernel/dims={d}/current_five_calls', 'BenchmarkKernel/dims={d}/fused_go'),
    ('BenchmarkKernel/dims={d}/current_distance', 'BenchmarkKernel/dims={d}/prototype_distance'),
    ('BenchmarkPostingScan/dims={d}/current_distance', 'BenchmarkPostingScan/dims={d}/prototype_distance'),
    ('BenchmarkPostingScan/dims={d}/current_distance', 'BenchmarkPostingScan/dims={d}/fastscan'),
    ('BenchmarkScattered/dims={d}/current_distance', 'BenchmarkScattered/dims={d}/prototype_distance'),
]
print()
print('| current | prototype | median ratio | min | max | median saving ns |')
print('|---|---|---|---|---|---|')
for d in (768, 1536):
    for a, b in pairs:
        a, b = a.format(d=d), b.format(d=d)
        common = sorted(set(rows[a]) & set(rows[b]))
        if not common:
            continue
        ratios = [rows[a][r] / rows[b][r] for r in common]
        savings = [rows[a][r] - rows[b][r] for r in common]
        print('| %s | %s | %.2fx | %.2fx | %.2fx | %.1f |' % (
            a.split('/', 1)[1], b.split('/')[-1], statistics.median(ratios), min(ratios), max(ratios),
            statistics.median(savings)))
