#!/usr/bin/env python3
"""The verdict of v2.1, the insert ef sweep over H1 (bench-report.md, "v2.1: el
ef de inserción, sobre H1"), from the files of run_v21_insertef.sh.

usage: insertef_verdict.py <v2.1 dir> <v2.0 dir> > insertef-verdict.md

Every cell is the H1 cell of v2.0 with another ef for the edge search of an
insert. A cell passes when, at 100 % of the corpus and against the batch
reference of v2.0, its recall at every search ef of the grid is no more than
0.005 below, its loads at recall 0.95 no more than 15 % above and its loads at
0.99 no more than 10 % above. The chosen ef is the lowest that passes; the
anchor, ef 320, is the H1 cell of v2.0 and its repeat.
"""
import json
import math
import os
import sys

EFS = (160, 80, 40, 20)
ANCHOR = 320
MAX_DEFICIT, MAX_LOADS_95, MAX_LOADS_99, MIN_EDGE_RECALL = 0.005, 0.15, 0.10, 0.90
SEARCH = ('searchLoads', 'searchRounds', 'searchScanned', 'searchEndpoints', 'searchRescored')


def load(path):
    return json.load(open(path)) if os.path.exists(path) else None


def curve(points):
    return sorted((p for p in points if p['threads'] == 1), key=lambda p: p['ef'])


def value(p, field):
    return p[field] if field in p else p['stats'][field]


def at(pts, target, field):
    """The field at the recall target, interpolated between the two points around it."""
    prev = None
    for p in pts:
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


def fmt(x, spec='%.1f'):
    return '—' if x is None else spec % x


def pct(x):
    return '—' if x is None else '%+.1f %%' % (100 * x)


def quantile(hist, q):
    """The smallest value with at least the share q of the inserts at or below it."""
    total, seen = sum(hist), 0
    for v, count in enumerate(hist):
        seen += count
        if seen >= q * total:
            return v
    return len(hist) - 1


def recall_of(c):
    return c['captured'] / max(c['wanted'], 1), c['postingsCaptured'] / max(c['postingsWanted'], 1)


def stretches(doc):
    """(percent, cumulative stats, atInsert) of every stop of a cell after the batch half, in order."""
    stops = [(g['percent'], g['stats'], g['atInsert']) for g in doc.get('guards', [])]
    stops += [(cp['percent'], cp['stats'], cp['atInsert']) for cp in doc['checkpoints'] if cp['percent'] > 50]
    return sorted(stops, key=lambda s: s[0])


class Cell:
    def __init__(self, label, ef, doc):
        self.label, self.ef, self.doc = label, ef, doc
        self.stopped = doc.get('stopped') if doc else None
        cps = doc['checkpoints'] if doc else []
        self.final = cps[-1] if cps and cps[-1]['percent'] == 100 else None
        self.pts = curve(self.final['points']) if self.final else None
        self.first = curve(cps[0]['points']) if cps else None

    def latest(self):
        """The last stop of the cell, a checkpoint or a guard: what it had done when it ended."""
        stops = (self.doc.get('guards', []) + self.doc['checkpoints']) if self.doc else []
        return max(stops, key=lambda x: x['percent']) if stops else None

    def state(self):
        if self.doc is None:
            return 'sin datos'
        if self.stopped:
            return 'cortada: ' + self.stopped
        return 'completa' if self.final else 'sin acabar'


def main(d, v20):
    ref = load(os.path.join(v20, 'dbpedia-1m-stream-principal-reference.json'))
    if not ref:
        print('Sin la referencia en lote de v2.0: sin veredicto.')
        return
    refpts = curve(ref['points'])
    ref_at = {p['ef']: p['recall'] for p in refpts}
    ref95, ref99 = at(refpts, 0.95, 'loadsMean'), at(refpts, 0.99, 'loadsMean')
    anchors = [Cell('%d, ancla' % ANCHOR, ANCHOR, load(os.path.join(v20, 'dbpedia-1m-stream-h1.json'))),
               Cell('%d, ancla repetida' % ANCHOR, ANCHOR, load(os.path.join(v20, 'dbpedia-1m-stream-h1-writes.json')))]
    cells = [Cell('%d' % ef, ef, load(os.path.join(d, 'dbpedia-1m-stream-h1-ef%d.json' % ef))) for ef in EFS]
    rows = [c for c in anchors + cells if c.doc]

    # The noise floor at 100 %: the two runs of H1 at ef 320.
    floor_recall, floor95, floor99 = {}, None, None
    a, b = anchors
    if a.pts and b.pts:
        other = {p['ef']: p['recall'] for p in b.pts}
        floor_recall = {p['ef']: abs(p['recall'] - other[p['ef']]) for p in a.pts if p['ef'] in other}
        la, lb = at(a.pts, 0.95, 'loadsMean'), at(b.pts, 0.95, 'loadsMean')
        floor95 = abs(lb / la - 1)
        la, lb = at(a.pts, 0.99, 'loadsMean'), at(b.pts, 0.99, 'loadsMean')
        floor99 = abs(lb / la - 1)
    floor = max(floor_recall.values(), default=0)

    print('### Estado de las celdas\n')
    print('| ef de inserción | estado | splits (fallidos) | tamaño medio (p50 / p95 / máx) | cobertura | no-nearest | µs por insert, indicativo |')
    print('|---|---|---|---|---|---|---|')
    for c in rows:
        cp, last = c.final, c.latest()
        if not last:
            print('| %s | %s | — | — | — | — | — |' % (c.label, c.state()))
            continue
        st = last['stats']
        sizes = '%.1f (%d / %d / %d)' % (cp['sizeMean'], cp['sizeP50'], cp['sizeP95'], cp['sizeMax']) if cp else '—'
        print('| %s | %s | %d (%d) | %s | %s | %.4f | %s |' % (
            c.label, c.state(), st['splits'], st['splitsFailed'], sizes,
            last['coverage'], st['nonNearest'] / max(st['inserted'], 1),
            fmt(last['insertSec'] * 1e6 / st['inserted'] if st['inserted'] else None, '%.0f')))

    done = [c for c in rows if c.pts]
    print('\n### Recall a igual ef de búsqueda al 100 %, diferencia frente al lote\n')
    print('| ef de búsqueda | lote | ' + ' | '.join(c.label for c in done) + ' |')
    print('|---|---|' + '---|' * len(done))
    worst = {c.label: math.inf for c in done}
    for p in refpts:
        line = []
        for c in done:
            mine = next((q['recall'] for q in c.pts if q['ef'] == p['ef']), None)
            if mine is None:
                line.append('—')
                continue
            diff = mine - p['recall']
            worst[c.label] = min(worst[c.label], diff)
            line.append('%+.4f' % diff)
        print('| %d | %.4f | %s |' % (p['ef'], p['recall'], ' | '.join(line)))
    print('| peor | | ' + ' | '.join('%+.4f' % worst[c.label] for c in done) + ' |')

    def verdict(passes, margin, noise):
        word = 'cumple' if passes else 'no cumple'
        return word + (', dentro del ruido' if noise is not None and abs(margin) < noise else '')

    print('\n### Criterio\n')
    print('| ef de inserción | peor diferencia de recall (umbral −%.3f) | cargas a 0.95 (umbral +%d %%) | cargas a 0.99 (umbral +%d %%) '
          '| guardia: peor tramo (umbral %.2f) | cumple |' % (MAX_DEFICIT, 100 * MAX_LOADS_95, 100 * MAX_LOADS_99, MIN_EDGE_RECALL))
    print('|---|---|---|---|---|---|')
    passing = {}
    for c in rows:
        guard = [recall_of(ai)[0] for _, _, ai in stretches(c.doc) if ai.get('vectors')]
        guard_txt = fmt(min(guard), '%.4f') if guard else '—'
        if not c.pts:
            passing[c.label] = False
            print('| %s | — | — | — | %s | **no:** %s |' % (c.label, guard_txt, c.state()))
            continue
        l95, l99 = at(c.pts, 0.95, 'loadsMean'), at(c.pts, 0.99, 'loadsMean')
        d95 = l95 / ref95 - 1 if l95 else None
        d99 = l99 / ref99 - 1 if l99 else None
        w = worst[c.label]
        ok_r, ok_95, ok_99 = w >= -MAX_DEFICIT, d95 is not None and d95 <= MAX_LOADS_95, d99 is not None and d99 <= MAX_LOADS_99
        ok_g = not guard or min(guard) >= MIN_EDGE_RECALL
        passing[c.label] = ok_r and ok_95 and ok_99 and ok_g
        print('| %s | %+.4f: %s | %s (%s): %s | %s (%s): %s | %s | **%s** |' % (
            c.label, w, verdict(ok_r, w + MAX_DEFICIT, floor),
            fmt(l95), pct(d95), verdict(ok_95, MAX_LOADS_95 - d95, floor95) if d95 is not None else 'no llega a 0.95',
            fmt(l99), pct(d99), verdict(ok_99, MAX_LOADS_99 - d99, floor99) if d99 is not None else 'no llega a 0.99',
            guard_txt, 'sí' if passing[c.label] else 'no'))

    print()
    ok = [c.ef for c in cells if passing.get(c.label)]
    missing = [c.label for c in cells if not c.doc or (not c.final and not c.stopped)]
    if missing:
        print('- **Sin veredicto todavía:** faltan las celdas de ef %s.' % ', '.join(missing))
    elif not ok:
        print('- **Elegido: ef %d.** Ninguna de las cuatro celdas cumple, y el ancla sí.' % ANCHOR)
    else:
        chosen = min(ok)
        print('- **Elegido: ef %d,** el menor que cumple.%s' % (
            chosen, ' No hay medida por debajo.' if chosen == min(EFS) else ''))
        above = [c.ef for c in cells if c.ef > chosen and not passing.get(c.label)]
        if above:
            print('- **No es monótono:** no cumple un ef mayor que el elegido (%s). La regla se aplica como está escrita.' % (
                ', '.join(str(e) for e in sorted(above))))
    if floor_recall:
        print('- **Suelo de ruido, de las dos corridas del ancla:** hasta %.4f de recall (%s), un %.1f %% en cargas a 0.95 y un %.1f %% a 0.99.' % (
            floor, ', '.join('ef %d: %.4f' % (e, v) for e, v in sorted(floor_recall.items())), 100 * floor95, 100 * floor99))

    print('\n### La búsqueda de un insert: lo que lee en disco\n')
    print('Conteo directo de la búsqueda de cada insert, por insert, hasta donde llegó la celda. La aproximación es el trabajo de las queries de un checkpoint '
          'a un ef de búsqueda igual al de inserción, que es lo único que hay para el ancla.\n')
    print('| ef de inserción | cargas: media | p50 | p95 | máximo | rondas | vectores escaneados | endpoints puntuados | rescoreados '
          '| aproximación, cargas al 50 % | al 100 % | conteo / media de la aproximación |')
    print('|---|---|---|---|---|---|---|---|---|---|---|---|')
    for c in rows:
        cp = c.latest()
        st = cp['stats'] if cp else {}
        n = st.get('inserted', 0)
        hist = st.get('searchLoadsHist')
        direct = [st[k] / n for k in SEARCH] if hist and n else None
        proxy = [next((q['stats']['loadsMean'] for q in pts if q['ef'] == c.ef), None) if pts else None for pts in (c.first, c.pts)]
        ratio = direct[0] / ((proxy[0] + proxy[1]) / 2) if direct and None not in proxy else None
        print('| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |' % (
            c.label, fmt(direct[0] if direct else None), fmt(quantile(hist, 0.5) if hist else None, '%d'),
            fmt(quantile(hist, 0.95) if hist else None, '%d'), fmt(len(hist) - 1 if hist else None, '%d'),
            fmt(direct[1] if direct else None), fmt(direct[2] if direct else None, '%.0f'),
            fmt(direct[3] if direct else None, '%.0f'), fmt(direct[4] if direct else None),
            fmt(proxy[0]), fmt(proxy[1]), fmt(ratio, '%.3f')))

    print('\n**Por tramo:** cargas de posting por insert, y entre paréntesis las escrituras de aristas inversas por insert.\n')
    marks = sorted({s[0] for c in rows for s in stretches(c.doc)})
    print('| ef de inserción | ' + ' | '.join('hasta el %d %%' % m for m in marks) + ' |')
    print('|---|' + '---|' * len(marks))
    for c in rows:
        prev, cellsOut = {'inserted': 0}, {}
        for percent, st, _ in stretches(c.doc):
            n = st['inserted'] - prev.get('inserted', 0)
            if n > 0:
                loads = (st['searchLoads'] - prev.get('searchLoads', 0)) / n if 'searchLoadsHist' in st else None
                inv = (st['inverseEdges'] - prev.get('inverseEdges', 0)) / n
                cellsOut[percent] = '%s (%.2f)' % (fmt(loads), inv)
            prev = st
        print('| %s | %s |' % (c.label, ' | '.join(cellsOut.get(m, '—') for m in marks)))

    print('\n### Recall de arista al insertar, por tramo\n')
    print('A nivel de vector y, entre paréntesis, de posting. Es la guardia del constructor: por debajo de %.2f la celda se corta.\n' % MIN_EDGE_RECALL)
    print('| ef de inserción | ' + ' | '.join('hasta el %d %%' % m for m in marks) + ' |')
    print('|---|' + '---|' * len(marks))
    for c in rows:
        by = {percent: '%.4f (%.4f)' % recall_of(ai) for percent, _, ai in stretches(c.doc) if ai.get('vectors')}
        print('| %s | %s |' % (c.label, ' | '.join(by.get(m, '—') for m in marks)))

    print('\n### Grado y recall de arista contra el corpus actual, al 100 %\n')
    print('| ef de inserción | grado medio: insertados | lote | insertados ahora: arista (posting) | lote ahora: arista (posting) |')
    print('|---|---|---|---|---|')
    for c in rows:
        if not c.final:
            continue
        i, bt = c.final['inserted'], c.final['batch']
        print('| %s | %.2f | %.2f | %.4f (%.4f) | %.4f (%.4f) |' % (
            (c.label, i['edges'] / i['vectors'], bt['edges'] / bt['vectors']) + recall_of(i) + recall_of(bt)))

    print('\n### Escrituras de aristas inversas por insert\n')
    print('| ef de inserción | listas leídas | escrituras: media | p50 | p95 | máximo | inserts sin ninguna '
          '| postings distintos: media | p50 | p95 | máximo |')
    print('|---|---|---|---|---|---|---|---|---|---|---|')
    for c in rows:
        cp = c.latest()
        st = cp['stats'] if cp else {}
        n = st.get('inserted', 0)
        if not n:
            continue
        w, po = st.get('inverseWrites'), st.get('inversePostings')
        if not w:
            print('| %s | — | %.2f | — | — | — | — | — | — | — | — |' % (c.label, st['inverseEdges'] / n))
            continue
        top = lambda h: max((v for v, k in enumerate(h) if k), default=0)
        print('| %s | %.2f | %.2f | %d | %d | %d | %.1f %% | %.2f | %d | %d | %d |' % (
            c.label, st['inverseChecks'] / n, st['inverseEdges'] / n, quantile(w, 0.5), quantile(w, 0.95), top(w),
            100 * w[0] / n, sum(v * k for v, k in enumerate(po)) / n, quantile(po, 0.5), quantile(po, 0.95), top(po)))

    print('\n### Trabajo de una query a igual recall, al 100 %\n')
    print('| ef de inserción | cargas a 0.95 | endpoints a 0.95 | cargas a 0.99 | endpoints a 0.99 '
          '| frente al lote: cargas a 0.95 / a 0.99 | frente al ancla: cargas a 0.95 / a 0.99 | frente al ancla: endpoints a 0.95 / a 0.99 |')
    print('|---|---|---|---|---|---|---|---|')
    print('| lote | %s | %s | %s | %s | | | |' % (
        fmt(ref95), fmt(at(refpts, 0.95, 'endpointsMean'), '%.0f'), fmt(ref99), fmt(at(refpts, 0.99, 'endpointsMean'), '%.0f')))
    base = None
    for c in done:
        v = [at(c.pts, t, f) for t in (0.95, 0.99) for f in ('loadsMean', 'endpointsMean')]
        if base is None:
            base = v
        rel = lambda x, y: pct(x / y - 1) if x and y else '—'
        print('| %s | %s | %s | %s | %s | %s / %s | %s / %s | %s / %s |' % (
            c.label, fmt(v[0]), fmt(v[1], '%.0f'), fmt(v[2]), fmt(v[3], '%.0f'),
            rel(v[0], ref95), rel(v[2], ref99), rel(v[0], base[0]), rel(v[2], base[2]), rel(v[1], base[1]), rel(v[3], base[3])))

    print('\n### Fases de un insert, µs\n')
    print('Indicativo: las celdas de v2.1 corrieron a la vez; el ancla, en paralelo con otras dos celdas, y su repetición, sola.\n')
    print('| ef de inserción | asignación | centroide exacto | búsqueda de aristas | escritura | aristas inversas | split | total |')
    print('|---|---|---|---|---|---|---|---|')
    for c in rows:
        cp = c.latest()
        st = cp['stats'] if cp else {}
        n = st.get('inserted', 0)
        if not n:
            continue
        print('| %s | %s | %.0f |' % (c.label, ' | '.join(
            '%.0f' % (st.get(k, 0) / n / 1000) for k in ('assignNs', 'nearestCheckNs', 'edgeSearchNs', 'writeNs', 'inverseNs', 'splitNs')),
            cp['insertSec'] * 1e6 / n))


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
