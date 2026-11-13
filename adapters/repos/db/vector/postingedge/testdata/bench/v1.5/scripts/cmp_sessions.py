"""Point-by-point QPS of one result file against another.

usage: cmp_sessions.py <reference.json> <other.json>

Points are matched by ef, threads, rescore limit, load batch and margin.
Prints, per curve, the other/reference QPS ratio of every point, the pass
spread of both, and the median and range of the ratios.
"""
import json, sys, statistics, collections


def key(p):
    return (p['threads'], p.get('rescoreLimit'), p.get('loadBatch'), p.get('sourceMargin'), p['ef'])


def spread(p):
    runs = p.get('qpsRuns') or []
    return (max(runs) - min(runs)) / p['qps'] if runs and p['qps'] > 0 else 0.0


ref = {key(p): p for p in json.load(open(sys.argv[1]))['points'] if p['qps'] > 0}
oth = {key(p): p for p in json.load(open(sys.argv[2]))['points'] if p['qps'] > 0}
curves = collections.defaultdict(list)
for k in sorted(set(ref) & set(oth)):
    curves[k[:4]].append(k)
print('== %s / %s' % (sys.argv[2].split('/')[-1], sys.argv[1].split('/')[-1]))
for c, ks in curves.items():
    ratios = []
    line = []
    for k in ks:
        a, b = ref[k], oth[k]
        r = b['qps'] / a['qps']
        ratios.append(r)
        flag = '!' if max(spread(a), spread(b)) > 0.10 else ''
        line.append('ef%d %+.1f%%%s' % (k[4], 100 * (r - 1), flag))
    print(' threads=%s rescore=%s batch=%s margin=%s: median %+.1f%%, range %+.1f%% .. %+.1f%%' % (
        c[0], c[1], c[2], c[3], 100 * (statistics.median(ratios) - 1), 100 * (min(ratios) - 1), 100 * (max(ratios) - 1)))
    print('   ' + ', '.join(line))
