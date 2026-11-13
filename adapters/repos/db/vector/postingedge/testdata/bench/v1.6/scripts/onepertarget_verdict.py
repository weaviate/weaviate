"""One edge per target posting at 10M (v1.6) read against its pre-registration.

usage: onepertarget_verdict.py <dir with the v1.6 jsons> [prefix]

Reads <prefix>-{full,onepertarget,full-2}.json and their -repeat twins, in the
order they ran, and <prefix>-onepertarget-untimed.json, the untimed check that
comes before any of them. The prefix is wikidpr-10m-onepertarget unless given.
QPS and work at a recall are read on each cell's ef curve as the report's
tables read them (statAtRecall), as cap_verdict.py of v1.5 does. Prints
Markdown.

Pre-registration (bench-report.md, v1.6, "Una arista por posting de destino"):
- it wins if, with 8 threads and against the first anchor, QPS@0.95 is up 5 %
  or more, QPS@0.99 is down no more than 2 %, and 0.99 is still reached by
  raising only ef with rescoreLimit 200; 8 threads decides, as in v1.5;
- reported too, not criteria: endpoints, loads and rounds at equal recall, the
  edge memory saved, and 1 thread;
- stop: if the filtered edges reach 0.99 at no ef up to 1024, untimed, it is
  reported and nothing is timed;
- drift: if the two anchors differ more than 2 % in median, it is said, and
  the verdict is given against both.
"""
import json
import math
import os
import statistics
import sys

CELLS = [('full', 'sin filtro (ancla)'), ('onepertarget', 'una arista por posting'), ('full-2', 'sin filtro (ancla del final)')]
TARGETS = (0.95, 0.99)
GAIN, LOSS, DRIFT, FLOOR = 0.05, 0.02, 0.02, 0.99


def load(d, prefix, cell, repeat=False):
    path = os.path.join(d, '%s-%s%s.json' % (prefix, cell, '-repeat' if repeat else ''))
    if not os.path.exists(path):
        return None
    return json.load(open(path))


def curve(doc, threads):
    pts = [p for p in doc['points'] if p['threads'] == threads and p.get('qps', 0) > 0]
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
    if a is None or b is None:
        return float('nan')
    return 100 * (a / b - 1)


def num(x, fmt='%.0f'):
    return '—' if x is None else fmt % x


def edges_table(docs):
    print('### Aristas de cada celda\n')
    print('| celda | aristas por vector | postings de destino por vector | aristas quitadas | recall de arista contra todas | aristas, MB | heap del índice, MB | self found |')
    print('|---|---|---|---|---|---|---|---|')
    for c, name in CELLS:
        doc = docs[c][0]
        if doc is None:
            continue
        g = doc['diagnostics']
        print('| %s | %.2f | %.2f | %.1f %% | %.4f | %.1f | %.1f | %.4f |' % (
            name, g['edgesPerVector'], g['edgeTargetsPerVector'], 100 * g.get('edgesDropped', 0),
            g['edgeRecallOfAllEdges'], doc['componentsMB']['edges'], doc['indexHeapMB'], g['selfFound']))
    print()


def untimed_table(check):
    print('### La comprobación sin cronometrar, antes de ninguna celda\n')
    print('| ef | recall | cargas | rondas | endpoints | escaneados |')
    print('|---|---|---|---|---|---|')
    for p in sorted(check['points'], key=lambda p: p['ef']):
        s = p['stats']
        print('| %d | %.4f | %.1f | %.1f | %.0f | %.0f |' % (
            p['ef'], p['recall'], s['loadsMean'], s['roundsMean'], s['endpointsMean'], s['scannedMean']))
    print()


def main(d, prefix):
    check = load(d, prefix, 'onepertarget-untimed')
    docs = {c: (load(d, prefix, c), load(d, prefix, c, True)) for c, _ in CELLS}
    if check is None:
        print('No hay comprobación sin cronometrar (%s-onepertarget-untimed.json): el paso no corrió.' % prefix)
        return
    best = max(p['recall'] for p in check['points'])
    max_ef = max(p['ef'] for p in check['points'])
    if docs['onepertarget'][0] is None:
        edges_table({c: (check if c == 'onepertarget' else None, None) for c, _ in CELLS})
        untimed_table(check)
        print('### Veredicto\n')
        stop = best < FLOOR
        print('- **%s:** recall máximo %.4f con ef hasta %d, %s de %.2f.%s' % (
            'Stop' if stop else 'Sin celdas cronometradas', best, max_ef, 'por debajo' if stop else 'por encima', FLOOR,
            ' Se reporta sin cronometrar nada.' if stop else ' El paso no llegó a cronometrar: ver su log.'))
        return

    anchor, anchor2 = docs['full'][0], docs['full-2'][0]
    threads = sorted({p['threads'] for p in anchor['points'] if p.get('qps', 0) > 0})
    # 8 threads decides; a run with other thread counts (a smoke) is read
    # with its largest.
    decide = 8 if 8 in threads else threads[-1]
    edges_table(docs)
    untimed_table(check)

    print('### QPS a igual recall (primera ronda; entre paréntesis, la segunda)\n')
    for th in threads:
        print('**%d thread%s**\n' % (th, '' if th == 1 else 's'))
        print('| celda | QPS@0.95 | contra el ancla | QPS@0.99 | contra el ancla | ef para 0.99 | recall máximo (ef 1024) |')
        print('|---|---|---|---|---|---|---|')
        base = {t: at(curve(anchor, th), t) for t in TARGETS}
        for c, name in CELLS:
            first, second = docs[c]
            if first is None:
                continue
            a = curve(first, th)
            b = curve(second, th) if second is not None else []
            q = {t: (at(a, t), at(b, t)) for t in TARGETS}
            if not a:
                continue
            print('| %s | %s (%s) | %+.1f %% (%+.1f %%) | %s (%s) | %+.1f %% (%+.1f %%) | %s | %.4f |' % (
                name, num(q[0.95][0]), num(q[0.95][1]), pct(q[0.95][0], base[0.95]), pct(q[0.95][1], base[0.95]),
                num(q[0.99][0]), num(q[0.99][1]), pct(q[0.99][0], base[0.99]), pct(q[0.99][1], base[0.99]),
                num(first_ef(a, 0.99), '%d'), a[-1]['recall']))
        print()

    print('### Trabajo por query a igual recall (1 thread, primera ronda)\n')
    print('| celda | recall | ef | endpoints | escaneados | distancias | cargas | rondas | µs por query | endpoints, µs | scan, µs | centroides, µs |')
    print('|---|---|---|---|---|---|---|---|---|---|---|---|')
    for t in TARGETS:
        for c, name in CELLS:
            if docs[c][0] is None:
                continue
            a = curve(docs[c][0], 1)
            if at(a, t) is None:
                print('| %s | %.2f | no llega | | | | | | | | | |' % (name, t))
                continue
            print('| %s | %.2f | %.0f | %.0f | %.0f | %.0f | %.1f | %.1f | %.0f | %.0f | %.0f | %.0f |' % (
                name, t, at(a, t, 'ef'), at(a, t, 'endpointsMean'), at(a, t, 'scannedMean'), at(a, t, 'distances'),
                at(a, t, 'loadsMean'), at(a, t, 'roundsMean'), at(a, t, 'totalUs'), at(a, t, 'expandUs'),
                at(a, t, 'scanUs'), at(a, t, 'centroidUs')))

    print('\n### A igual ef (1 thread, primera ronda)\n')
    print('| ef | recall sin filtro / con filtro | QPS sin filtro / con filtro | endpoints sin filtro / con filtro | cargas sin filtro / con filtro |')
    print('|---|---|---|---|---|')
    rows = [curve(docs[c][0], 1) for c in ('full', 'onepertarget')]
    for p, q in zip(*rows):
        print('| %d | %.4f / %.4f | %.0f / %.0f | %.0f / %.0f | %.1f / %.1f |' % (
            p['ef'], p['recall'], q['recall'], p['qps'], q['qps'], p['stats']['endpointsMean'], q['stats']['endpointsMean'],
            p['stats']['loadsMean'], q['stats']['loadsMean']))

    drift = False
    if anchor2 is not None:
        print('\n### Deriva entre las dos anclas\n')
        print('| threads | mediana del ancla del final contra la del principio, por punto | mínimo | máximo |')
        print('|---|---|---|---|')
        for th in threads:
            r = [pct(b['qps'], a['qps']) for a, b in zip(curve(anchor, th), curve(anchor2, th))]
            print('| %d | %+.1f %% | %+.1f %% | %+.1f %% |' % (th, statistics.median(r), min(r), max(r)))
        allr = [pct(b['qps'], a['qps']) for th in threads for a, b in zip(curve(anchor, th), curve(anchor2, th))]
        drift = abs(statistics.median(allr)) > 100 * DRIFT
        print('\nTodos los puntos: %+.1f %% de mediana, %s.' % (
            statistics.median(allr), 'deriva: el veredicto va contra las dos anclas' if drift else 'sin deriva según el umbral del 2 %'))

    night = os.path.join(d, 'wikidpr-10m-pe.json')
    if os.path.exists(night):
        pe = json.load(open(night))
        print('\n### El ancla del paso frente al brazo de posting-edge de la noche (sin ser criterio)\n')
        print('| threads | QPS@0.95 ancla / noche | QPS@0.99 ancla / noche |')
        print('|---|---|---|')
        for th in threads:
            a, n = curve(anchor, th), curve(pe, th)
            print('| %d | %s / %s (%+.1f %%) | %s / %s (%+.1f %%) |' % (
                th, num(at(a, 0.95)), num(at(n, 0.95)), pct(at(a, 0.95), at(n, 0.95)),
                num(at(a, 0.99)), num(at(n, 0.99)), pct(at(a, 0.99), at(n, 0.99))))

    print('\n### Veredicto\n')
    one = docs['onepertarget'][0]
    refs = [('el ancla del principio', anchor)] + ([('la del final', anchor2)] if anchor2 is not None else [])
    for th in threads:
        parts = []
        for label, ref in refs:
            g95 = pct(at(curve(one, th), 0.95), at(curve(ref, th), 0.95))
            g99 = pct(at(curve(one, th), 0.99), at(curve(ref, th), 0.99))
            parts.append('%+.1f %% a 0.95 y %+.1f %% a 0.99 contra %s' % (g95, g99, label))
        print('- **%d thread%s:** %s.' % (th, '' if th == 1 else 's', '; '.join(parts)))
    reach = first_ef(curve(one, decide), 0.99) is not None

    def wins(ref):
        g95 = pct(at(curve(one, decide), 0.95), at(curve(ref, decide), 0.95))
        g99 = pct(at(curve(one, decide), 0.99), at(curve(ref, decide), 0.99))
        # nan, a level not reached, never passes
        return g95 >= 100 * GAIN and g99 >= -100 * LOSS and reach

    first = wins(anchor)
    line = '- **Con %d threads, que decide%s:** 0.99 %salcanzable con rescoreLimit 200. %s contra el ancla del principio' % (
        decide, '' if decide == 8 else ' (no hay 8 en esta corrida)', '' if reach else 'no ', 'Gana' if first else 'No gana')
    if drift and anchor2 is not None:
        line += ', y %s contra la del final' % ('gana' if wins(anchor2) else 'no gana')
    print(line + '.')


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else '.', sys.argv[2] if len(sys.argv) > 2 else 'wikidpr-10m-onepertarget')
