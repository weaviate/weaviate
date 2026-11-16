"""The v2.0 streaming experiment read against its pre-registration.

usage: stream_verdict.py <the v2.0 directory>

Reads dbpedia-1m-stream-{calibration,principal,h1,h2,natural,both}.json, the
checkpoint grids -cp<percent>.json next to each cell, the batch reference
dbpedia-1m-stream-principal-reference.json and the timed frontiers
dbpedia-1m-stream-principal-timed-{reference,reference-docid,cell}.json with
their -repeat twins. Whatever is missing is left out. Values at a recall are
read on the ef curve as statAtRecall reads them. Prints Markdown.

Pre-registration (bench-report.md, "v2.0"):
- H1 (no inverse edges needed) and H2 (no edge repair needed) hold if, at
  100 %, the principal cell is within 0.005 of the batch reference's recall at
  every ef of the grid (the worst ef decides), and its loads at 0.95 are at
  most 15 % above the reference's;
- stop-and-report: a coverage failure, or the inserted vectors' edge recall
  below 0.90, at any checkpoint;
- the threshold keeps the final mean posting size within 15 % of 236.
"""
import json
import math
import os
import statistics
import sys

CELLS = [('principal', 'principal'), ('h1', 'control H1 (aristas inversas)'),
         ('h2', 'control H2 (reparación)'), ('both', 'las dos'), ('natural', 'principal, orden natural')]
MAX_DEFICIT, MAX_LOADS, MIN_EDGE_RECALL, S, BAND = 0.005, 0.15, 0.90, 236, 0.15


def load(d, name):
    path = os.path.join(d, name)
    return json.load(open(path)) if os.path.exists(path) else None


def curve(points, threads=1, timed=None):
    pts = [p for p in points if p['threads'] == threads and (timed is None or (p.get('qps', 0) > 0) == timed)]
    return sorted(pts, key=lambda p: p['ef'])


def value(p, field):
    if field in p:
        return p[field]
    return p['stats'][field]


def at(pts, target, field):
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


def main(d):
    calib = load(d, 'dbpedia-1m-stream-calibration.json')
    if calib:
        print('### Calibración del umbral\n')
        print('| umbral | media del lote al 50 % | postings | tamaño medio final | p50 | p95 | máximo | splits (fallidos) | no-nearest | s |')
        print('|---|---|---|---|---|---|---|---|---|---|')
        for c in calib:
            inside = abs(c['sizeMean'] / S - 1) <= BAND
            print('| %d | %.1f | %d | %.1f%s | %d | %d | %d | %d (%d) | %.4f | %.0f |' % (
                c['threshold'], c['batchMean'], c['postings'], c['sizeMean'], '' if inside else ' (fuera)',
                c['sizeP50'], c['sizeP95'], c['sizeMax'], c['splits'], c['splitsFailed'], c['nonNearest'], c['seconds']))
        print()

    ref = load(d, 'dbpedia-1m-stream-principal-reference.json')
    refpts = curve(ref['points']) if ref else []
    refAt = {p['ef']: p for p in refpts}

    cells = {}
    for key, name in CELLS:
        doc = load(d, 'dbpedia-1m-stream-%s.json' % key)
        if doc:
            cells[key] = (name, doc)

    for key, (name, doc) in cells.items():
        print('### %s (umbral %d, ef de inserción %d, orden %s)\n' % (name, doc['threshold'], doc['insertEF'], doc['order']))
        if doc.get('stopped'):
            print('**Stop-and-report:** %s\n' % doc['stopped'])
        print('| checkpoint | vivos | postings | tamaño medio (p50 / p95 / máx) | recall de arista insertados (grado) | lote (grado) | cobertura | cargas a 0.95 | a 0.99 | ef para 0.99 | no-nearest | splits | µs por insert |')
        print('|---|---|---|---|---|---|---|---|---|---|---|---|---|')
        for cp in doc['checkpoints']:
            pts = curve(cp['points'])
            ef99 = next((p['ef'] for p in pts if p['recall'] >= 0.99), None)
            st = cp['stats']
            ins = st['inserted']
            print('| %d %% | %d | %d | %.1f (%d / %d / %d) | %s (%s) | %.4f (%.2f) | %s | %s | %s | %s | %s | %d | %s |' % (
                cp['percent'], cp['live'], cp['postings'], cp['sizeMean'], cp['sizeP50'], cp['sizeP95'], cp['sizeMax'],
                fmt(cp.get('insertedEdgeRecall') or None, '%.4f'), fmt(cp.get('insertedDegree') or None, '%.2f'),
                cp['batchEdgeRecall'], cp['batchDegree'], cp['coverage'],
                fmt(at(pts, 0.95, 'loadsMean')), fmt(at(pts, 0.99, 'loadsMean')), fmt(ef99, '%d'),
                fmt(st['nonNearest'] / ins if ins else None, '%.4f'), st['splits'],
                fmt(cp['insertSec'] * 1e6 / ins if ins else None, '%.0f')))
        last = doc['checkpoints'][-1] if doc['checkpoints'] else None
        if last and last['percent'] == 100 and refAt:
            st, ins = last['stats'], last['stats']['inserted']
            print('\nFases por insert, µs (indicativo): asignación %.0f, centroide exacto %.0f, búsqueda de aristas %.0f, escritura %.0f, inversas %.0f, split %.0f, reparación %.0f.\n' % tuple(
                st[f] / 1e3 / ins for f in ('assignNs', 'nearestCheckNs', 'edgeSearchNs', 'writeNs', 'inverseNs', 'splitNs', 'repairNs')))
            pts = curve(last['points'])
            print('Al 100 %%, frente a la referencia en lote (recall a igual ef):\n')
            print('| ef | referencia | %s | diferencia |' % key)
            print('|---|---|---|---|')
            worst = 0
            for p in pts:
                r = refAt.get(p['ef'])
                if r is None:
                    continue
                diff = p['recall'] - r['recall']
                worst = min(worst, diff)
                print('| %d | %.4f | %.4f | %+.4f |' % (p['ef'], r['recall'], p['recall'], diff))
            l95, r95 = at(pts, 0.95, 'loadsMean'), at(refpts, 0.95, 'loadsMean')
            l99, r99 = at(pts, 0.99, 'loadsMean'), at(refpts, 0.99, 'loadsMean')
            print('\nPeor diferencia: %+.4f. Cargas a 0.95: %s frente a %s (%s); a 0.99: %s frente a %s (%s).\n' % (
                worst, fmt(l95), fmt(r95), fmt(100 * (l95 / r95 - 1) if l95 and r95 else None, '%+.1f %%'),
                fmt(l99), fmt(r99), fmt(100 * (l99 / r99 - 1) if l99 and r99 else None, '%+.1f %%')))
            cells[key] = (name, doc, worst, l95 / r95 - 1 if l95 and r95 else None)
        print()

    timed = {n: (load(d, 'dbpedia-1m-stream-principal-timed-%s.json' % n), load(d, 'dbpedia-1m-stream-principal-timed-%s-repeat.json' % n))
             for n in ('reference', 'reference-docid', 'cell')}
    if all(v[0] for v in timed.values()):
        print('### Frontera cronometrada, mismo proceso (QPS a igual recall, primera ronda; entre paréntesis, la segunda)\n')
        print('| recall | threads | referencia | referencia por docID | principal | principal / referencia | por docID / referencia |')
        print('|---|---|---|---|---|---|---|')
        for t in (0.95, 0.99):
            for th in (1, 8):
                q = {n: (at(curve(a['points'], th, True), t, 'qps'), at(curve(b['points'], th, True), t, 'qps') if b else None)
                     for n, (a, b) in timed.items()}
                r, k, c = q['reference'], q['reference-docid'], q['cell']
                print('| %.2f | %d | %s (%s) | %s (%s) | %s (%s) | %s | %s |' % (
                    t, th, fmt(r[0], '%.0f'), fmt(r[1], '%.0f'), fmt(k[0], '%.0f'), fmt(k[1], '%.0f'), fmt(c[0], '%.0f'), fmt(c[1], '%.0f'),
                    fmt(c[0] / r[0] if c[0] and r[0] else None, '%.3fx'), fmt(k[0] / r[0] if k[0] and r[0] else None, '%.3fx')))
        print('\n| curva | threads | deriva entre rondas: mediana | mínimo | máximo |')
        print('|---|---|---|---|---|')
        for n, (a, b) in timed.items():
            if not b:
                continue
            for th in (1, 8):
                pa = {p['ef']: p['qps'] for p in curve(a['points'], th, True)}
                r = [100 * (p['qps'] / pa[p['ef']] - 1) for p in curve(b['points'], th, True) if p['ef'] in pa]
                if r:
                    print('| %s | %d | %+.1f %% | %+.1f %% | %+.1f %% |' % (n, th, statistics.median(r), min(r), max(r)))
        print()

    print('### Veredicto\n')
    principal = cells.get('principal')
    if not principal or len(principal) < 4:
        print('- Sin la celda principal al 100 %: sin veredicto.')
        return
    _, doc, worst, loads = principal
    stops = [k for k, v in cells.items() if v[1].get('stopped')]
    edge_low = [(k, cp['percent'], cp['insertedEdgeRecall']) for k, v in cells.items() for cp in v[1]['checkpoints']
                if cp.get('insertedEdgeRecall') and cp['insertedEdgeRecall'] < MIN_EDGE_RECALL]
    holds = worst >= -MAX_DEFICIT and loads is not None and loads <= MAX_LOADS
    print('- **Celda principal al 100 %%:** peor diferencia de recall a igual ef %+.4f (umbral −%.3f); cargas a 0.95 %+.1f %% (umbral +%d %%).' % (
        worst, MAX_DEFICIT, 100 * loads, 100 * MAX_LOADS))
    print('- **H1 y H2 %s.**' % ('se sostienen' if holds else 'no se sostienen'))
    if stops:
        print('- **Stop-and-report en:** %s.' % ', '.join(stops))
    if edge_low:
        print('- **Recall de arista de los insertados bajo 0.90:** %s.' % ', '.join('%s al %d %% (%.4f)' % e for e in edge_low))
    for key in ('h1', 'h2', 'both'):
        if key in cells and len(cells[key]) == 4:
            _, _, w, l = cells[key]
            print('- **%s:** peor diferencia %+.4f (principal %+.4f), cargas a 0.95 %+.1f %% (principal %+.1f %%).' % (
                cells[key][0], w, worst, 100 * l, 100 * loads))


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else '.')
