"""The v1.5 degree-cap cells read against their pre-registration.

usage: cap_verdict.py <dir with the v1.5 jsons>

Reads wikidpr-10m-degree-cap{0,10,8,0-2}.json and their -repeat twins, in the
order they ran. QPS and work at a recall are read on each cell's ef curve as
the report's tables read them (statAtRecall). Prints Markdown.

Pre-registration (bench-report.md, "v1.5"):
- a cap wins with QPS@0.95 at least 10% above the first anchor, with 1 and with
  8 threads, and 0.99 reachable by raising only ef (rescoreLimit 200);
- if it wins with one thread count and not the other, 8 threads decides;
- stop-and-report if the cap's edge recall (against the k nearest true edges)
  is more than 0.01 below the full edges' (0.9470);
- drift if the two anchors differ by more than 2% in median.
"""
import json
import math
import os
import statistics
import sys

CELLS = [('cap0', 'sin cap (ancla)'), ('cap10', 'cap 10'), ('cap8', 'cap 8'), ('cap0-2', 'sin cap (ancla del final)')]
TARGETS = (0.90, 0.95, 0.99)
WIN, STOP, DRIFT, FULL_EDGE_RECALL = 0.10, 0.01, 0.02, 0.9470


def load(d, cell, repeat=False):
    return json.load(open(os.path.join(d, 'wikidpr-10m-degree-%s%s.json' % (cell, '-repeat' if repeat else ''))))


def curve(doc, threads):
    pts = [p for p in doc['points'] if p['threads'] == threads and p['qps'] > 0]
    return sorted(pts, key=lambda p: p['ef'])


def value(p, field):
    if field in p:
        return p[field]
    if field == 'distances':
        return p['stats']['endpointsMean'] + p['stats']['scannedMean']
    return p['stats'][field]


def at(pts, target, field='qps'):
    """The value at a recall level, as statAtRecall in bench_cells_report_test.go
    reads it: linearly in recall and in the logarithm of the value."""
    prev = None
    for p in sorted(pts, key=lambda p: p['ef']):
        if p['recall'] >= target:
            if prev is None:
                return value(p, field)
            f = (target - prev['recall']) / (p['recall'] - prev['recall'])
            a, b = value(prev, field), value(p, field)
            if a <= 0 or b <= 0:
                return a + f * (b - a)
            return math.exp(math.log(a) + f * (math.log(b) - math.log(a)))
        prev = p
    return None


def first_ef(pts, target):
    for p in pts:
        if p['recall'] >= target:
            return p['ef']
    return None


def pct(a, b):
    return 100 * (a / b - 1)


def main(d):
    docs = {c: (load(d, c), load(d, c, True)) for c, _ in CELLS}
    anchor, anchor2 = docs['cap0'][0], docs['cap0-2'][0]

    print('### Aristas de cada celda\n')
    print('| celda | aristas por vector | vectores recortados | aristas quitadas | recall de arista (contra las k más cercanas) | recall de arista (contra todas) | memoria de aristas, MB | heap del índice, MB | self found |')
    print('|---|---|---|---|---|---|---|---|---|')
    for c, name in CELLS:
        g = docs[c][0]['diagnostics']
        stop = ' **stop**' if g['edgeRecall'] < FULL_EDGE_RECALL - STOP else ''
        print('| %s | %.2f | %.1f %% | %.1f %% | %.4f%s | %.4f | %.1f | %.1f | %.4f |' % (
            name, g['edgesPerVector'], 100 * g.get('vectorsCapped', 0), 100 * g.get('edgesDropped', 0),
            g['edgeRecall'], stop, g.get('edgeRecallOfAllEdges', g['edgeRecall']),
            docs[c][0]['componentsMB']['edges'], docs[c][0]['indexHeapMB'], g['selfFound']))

    print('\n### QPS a igual recall (primera ronda; entre paréntesis, la segunda)\n')
    for th in (1, 8):
        print('**%d thread%s**\n' % (th, '' if th == 1 else 's'))
        print('| celda | QPS@0.90 | QPS@0.95 | contra el ancla | QPS@0.99 | contra el ancla | ef para 0.99 | recall máximo (ef 1024) |')
        print('|---|---|---|---|---|---|---|---|')
        base = {t: at(curve(anchor, th), t) for t in TARGETS}
        for c, name in CELLS:
            a, b = (curve(x, th) for x in docs[c])
            q = {t: (at(a, t), at(b, t)) for t in TARGETS}
            print('| %s | %.0f (%.0f) | %.0f (%.0f) | %+.1f %% (%+.1f %%) | %.0f (%.0f) | %+.1f %% (%+.1f %%) | %s | %.4f |' % (
                name, q[0.90][0], q[0.90][1], q[0.95][0], q[0.95][1], pct(q[0.95][0], base[0.95]), pct(q[0.95][1], base[0.95]),
                q[0.99][0], q[0.99][1], pct(q[0.99][0], base[0.99]), pct(q[0.99][1], base[0.99]),
                first_ef(a, 0.99), a[-1]['recall']))
        print()

    print('### Trabajo por query a igual recall (1 thread, primera ronda)\n')
    print('| celda | recall | ef | endpoints | escaneados | distancias | cargas | rondas | µs por query | endpoints, µs | scan, µs | centroides, µs |')
    print('|---|---|---|---|---|---|---|---|---|---|---|---|')
    for t in (0.95, 0.99):
        for c, name in CELLS[:3]:
            a = curve(docs[c][0], 1)
            print('| %s | %.2f | %.0f | %.0f | %.0f | %.0f | %.1f | %.1f | %.0f | %.0f | %.0f | %.0f |' % (
                name, t, at(a, t, 'ef'), at(a, t, 'endpointsMean'), at(a, t, 'scannedMean'), at(a, t, 'distances'),
                at(a, t, 'loadsMean'), at(a, t, 'roundsMean'), at(a, t, 'totalUs'), at(a, t, 'expandUs'),
                at(a, t, 'scanUs'), at(a, t, 'centroidUs')))

    print('\n### A igual ef (1 thread, primera ronda)\n')
    print('| ef | recall sin cap / cap 10 / cap 8 | QPS sin cap / cap 10 / cap 8 | endpoints sin cap / cap 10 / cap 8 |')
    print('|---|---|---|---|')
    rows = [curve(docs[c][0], 1) for c in ('cap0', 'cap10', 'cap8')]
    for i, p in enumerate(rows[0]):
        print('| %d | %s | %s | %s |' % (p['ef'], ' / '.join('%.4f' % r[i]['recall'] for r in rows),
                                        ' / '.join('%.0f' % r[i]['qps'] for r in rows),
                                        ' / '.join('%.0f' % r[i]['stats']['endpointsMean'] for r in rows)))

    print('\n### Deriva entre las dos anclas\n')
    print('| threads | mediana del ancla del final contra la del principio, por punto | mínimo | máximo |')
    print('|---|---|---|---|')
    for th in (1, 8):
        r = [pct(b['qps'], a['qps']) for a, b in zip(curve(anchor, th), curve(anchor2, th))]
        print('| %d | %+.1f %% | %+.1f %% | %+.1f %% |' % (th, statistics.median(r), min(r), max(r)))
    allr = [pct(b['qps'], a['qps']) for th in (1, 8) for a, b in zip(curve(anchor, th), curve(anchor2, th))]
    drift = abs(statistics.median(allr)) > 100 * DRIFT
    print('\nTodos los puntos: %+.1f %% de mediana, %s.' % (statistics.median(allr), 'deriva' if drift else 'sin deriva según el umbral del 2 %'))

    print('\n### Veredicto\n')
    for c, name in CELLS[1:3]:
        g = docs[c][0]['diagnostics']
        if g['edgeRecall'] < FULL_EDGE_RECALL - STOP:
            print('- **%s:** stop, recall de arista %.4f.' % (name, g['edgeRecall']))
            continue
        gains = {}
        for th in (1, 8):
            gains[th] = [pct(at(curve(docs[c][0], th), 0.95), at(curve(ref, th), 0.95)) for ref in (anchor, anchor2)]
        reach = first_ef(curve(docs[c][0], 1), 0.99) is not None and first_ef(curve(docs[c][0], 8), 0.99) is not None
        wins = {th: gains[th][0] >= 100 * WIN for th in (1, 8)}
        verdict = wins[8] and reach  # 8 threads decides a split
        print('- **%s:** QPS@0.95 %+.1f %% con 1 thread y %+.1f %% con 8 contra el ancla del principio (%+.1f %% y %+.1f %% contra la del final); 0.99 %salcanzable con rescoreLimit 200. %s.' % (
            name, gains[1][0], gains[8][0], gains[1][1], gains[8][1], '' if reach else 'no ',
            'Gana' if verdict else 'No gana'))


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else '.')
