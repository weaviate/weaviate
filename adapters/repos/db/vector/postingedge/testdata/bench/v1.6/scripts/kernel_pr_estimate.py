"""The QPS estimate of 2026-11-12, restated for the kernel that runs on 2026-11-13.

usage: kernel_pr_estimate.py <rounds of run_kernel_pr_ab.sh> <TestSearchWork JSON at 10M>

The estimate of 2026-11-12 (kernel/scripts/kernel_qps_estimate.py) took the
savings per code of the prototype kernel, charged the prototype's query layout,
and took the distances of HNSW at 10M from TestSearchWork at 1M. This one
changes those three inputs and nothing else:

- the saving per code is that of the kernel of weaviate/weaviate#13880, before
  against after, from interleaved rounds: per endpoint, codes scattered over a
  sidecar of 1 GiB (BenchmarkScattered); per scanned vector, a posting of 236
  in cache (BenchmarkPostingScan);
- the kernel of the PR reads the five planes the query already has: no layout;
- the distances of HNSW are those of TestSearchWork at 10M, at the ef each
  HNSW build of v1.4 needs for the recall (48 and 64 at 0.95, 256 and 384 at
  0.99, as the estimate of 2026-11-12 took them).

It is still an estimate: the speed-up of one thread is applied to 8 threads,
and the scan is timed in cache.
"""
import collections
import json
import math
import re
import statistics
import sys

rows = collections.defaultdict(lambda: collections.defaultdict(dict))  # name -> side -> round -> ns
rnd, side = 0, ''
for line in open(sys.argv[1]):
    m = re.match(r'== round (\d+) (\w+)', line)
    if m:
        rnd, side = int(m.group(1)), m.group(2)
        continue
    m = re.match(r'(Benchmark\S+?)-\d+\s+\d+\s+([\d.]+) ns/op(?:\s+([\d.]+) ns/code)?', line)
    if m:
        rows[m.group(1)][side][rnd] = float(m.group(3) or m.group(2))


def saving(name):
    r = rows[name]
    common = sorted(set(r['before']) & set(r['after']))
    return statistics.median(r['before'][i] - r['after'][i] for i in common), len(common)


print('Per code, before / after the kernel of the PR, median of the rounds (ns):\n')
print('| benchmark | before | after | before / after |')
print('|---|---|---|---|')
for name in sorted(rows):
    if 'current_distance' not in name:
        continue
    r = rows[name]
    common = sorted(set(r['before']) & set(r['after']))
    if not common:
        continue
    ratio = statistics.median(r['before'][i] / r['after'][i] for i in common)
    print('| %s | %.1f | %.1f | %.2fx |' % (name.replace('Benchmark', ''), statistics.median(r['before'].values()),
                                          statistics.median(r['after'].values()), ratio))

D = 768
ep, n_ep = saving(f'BenchmarkScattered/dims={D}/current_distance')
scan, n_scan = saving(f'BenchmarkPostingScan/dims={D}/current_distance')
print('\nsavings per code: endpoint %.1f ns (%d rounds), scanned vector %.1f ns (%d rounds); no query layout' % (
    ep, n_ep, scan, n_scan))

work = json.load(open(sys.argv[2]))
count = {p['ef']: p['codeDistances'] for p in work['points'] if p.get('rescoreLimit') == 512}
print('distances to codes per query of HNSW at 10M (TestSearchWork): ' +
      ', '.join('ef=%d %.0f' % (ef, count[ef]) for ef in sorted(count)))


def at_recall(t):
    """The count at a recall on the curve of the TestSearchWork build."""
    pts = sorted((p for p in work['points'] if p.get('rescoreLimit') == 512), key=lambda p: p['ef'])
    prev = None
    for p in pts:
        if p['recall'] >= t:
            if prev is None:
                return p['codeDistances'], p['ef']
            f = (t - prev['recall']) / (p['recall'] - prev['recall'])
            return prev['codeDistances'] + f * (p['codeDistances'] - prev['codeDistances']), prev['ef'] + f * (p['ef'] - prev['ef'])
        prev = p
    return None, None


for t in (0.95, 0.99):
    c, ef = at_recall(t)
    if c:
        print('  the TestSearchWork build reaches %.2f at ef %.0f, with %.0f distances' % (t, ef, c))

# As in kernel_qps_estimate.py: the phase profile (1 thread, rescoreLimit 200,
# µs per query) and the QPS of the v1.4 tables, posting-edge and the two HNSW
# builds, with the ef each build needs.
profile = {
    0.95: dict(endpoints=6878, scanned=3306, total=1184, qps=(870, 5968)),
    0.99: dict(endpoints=33021, scanned=13081, total=4043, qps=(261, 1770)),
}
hnsw = {
    0.95: [('tarde', (1859, 13122), 48), ('mañana', (1828, 12467), 64)],
    0.99: [('tarde', (448, 3226), 256), ('mañana', (355, 2363), 384)],
}
# The 2026-11-12 estimate with the prototype, for the comparison.
before = {(0.95, 0, 'tarde'): 1.91, (0.95, 0, 'mañana'): 1.90, (0.95, 1, 'tarde'): 1.96, (0.95, 1, 'mañana'): 1.89,
          (0.99, 0, 'tarde'): 1.41, (0.99, 0, 'mañana'): 1.12, (0.99, 1, 'tarde'): 1.49, (0.99, 1, 'mañana'): 1.10}

print('\n| recall | threads | HNSW build | PE today | PE, kernel of the PR | HNSW today | HNSW, kernel of the PR | ratio today | '
      'ratio, kernel of the PR | ratio, prototype estimate (2026-11-12) |')
print('|---|---|---|---|---|---|---|---|---|---|')
for t, p in profile.items():
    total = p['total']
    new_total = total - p['endpoints'] * ep / 1000 - p['scanned'] * scan / 1000
    for th in (0, 1):
        pe = p['qps'][th]
        pe_new = pe * total / new_total
        for build, qps, ef in hnsw[t]:
            h = qps[th]
            one = 1e6 / qps[0]  # µs per query with one thread
            h_new = h * one / (one - count[ef] * ep / 1000)
            print('| %.2f | %d | %s (ef %d, %.0f distances) | %.0f | %.0f (%.2fx) | %.0f | %.0f (%.2fx) | %.2fx | %.2fx | %.2fx |' % (
                t, (1, 8)[th], build, ef, count[ef], pe, pe_new, total / new_total, h, h_new, one / (one - count[ef] * ep / 1000),
                h / pe, h_new / pe_new, before[(t, th, build)]))
