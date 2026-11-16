"""The v2.0 streaming experiment read against its pre-registration.

usage: stream_verdict.py <the v2.0 directory>

Reads dbpedia-1m-stream-{calibration,principal,h1,h2,natural,both,siblings,
inverse-siblings,h1-writes}.json, the checkpoint grids -cp<percent>.json next
to each cell, the batch reference dbpedia-1m-stream-principal-reference.json
and the timed frontiers
dbpedia-1m-stream-principal-timed-{reference,reference-docid,cell}.json with
their -repeat twins. Whatever is missing is left out. Values at a recall are
read on the ef curve as statAtRecall reads them. Prints Markdown.

Pre-registration (bench-report.md, "v2.0"):
- H1 (no inverse edges needed) and H2 (no edge repair needed) hold if, at
  100 %, the principal cell is within 0.005 of the batch reference's recall at
  every ef of the grid (the worst ef decides), and its loads at 0.95 are at
  most 15 % above the reference's;
- stop-and-report: a coverage failure at any checkpoint, or, by the
  amendment, the edge recall at insertion below 0.90 in any stretch (the
  guard of the edge builder); the edge recall against the current corpus and
  at posting level are reported, not stops;
- the threshold keeps the final mean posting size within 15 % of 236;
- cell 5 (sibling repair) matches H2 if its recall is at most 0.001 below
  H2's at every ef and its loads at 0.95 at most 2 % above;
- cell 6 (inverse edges with sibling repair) adds to H1 if its recall is more
  than 0.001 above H1's at some ef, or its loads at 0.95 more than 2 % below;
- the noise between runs is the spread of the cells at the 50 % checkpoint,
  the same batch build, and at 100 % the difference between the two runs of
  H1 (h1 and h1-writes): a verdict that rests on less is marked.
"""
import json
import math
import os
import statistics
import sys

CELLS = [('principal', 'principal'), ('h1', 'control H1 (aristas inversas)'),
         ('h2', 'control H2 (reparación)'), ('both', 'las dos'), ('natural', 'principal, orden natural'),
         ('siblings', 'celda 5 (reparación por hermanos)'),
         ('inverse-siblings', 'celda 6 (aristas inversas y reparación por hermanos)'),
         ('h1-writes', 'H1, repetición con el histograma de escrituras')]
# the cells that start from the same batch build: the shuffled half
SAME_BATCH = ('principal', 'h1', 'h2', 'both', 'siblings', 'inverse-siblings', 'h1-writes')
# cell 5 against H2 (pre-registration of cell 5): it matches if its recall is
# at most this much below H2's at every ef, and its loads at 0.95 at most
# this share above H2's
MATCH_RECALL, MATCH_LOADS = 0.001, 0.02
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


def counts(c, degree):
    """Edge recall / posting-level recall of edgeCounts, and the mean degree."""
    if not c or not c.get('vectors'):
        return '—'
    out = '%.4f / %.4f' % (c['captured'] / max(c['wanted'], 1), c['postingsCaptured'] / max(c['postingsWanted'], 1))
    return out + (' (%.2f)' % (c['edges'] / c['vectors']) if degree else '')


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
        print('| checkpoint | vivos | postings | tamaño medio (p50 / p95 / máx) | al insertar: arista / posting | insertados ahora: arista / posting (grado) | lote ahora: arista / posting (grado) | cobertura | cargas a 0.95 | a 0.99 | ef para 0.99 | no-nearest | splits | µs por insert |')
        print('|---|---|---|---|---|---|---|---|---|---|---|---|---|---|')
        for cp in doc['checkpoints']:
            pts = curve(cp['points'])
            ef99 = next((p['ef'] for p in pts if p['recall'] >= 0.99), None)
            st = cp['stats']
            ins = st['inserted']
            print('| %d %% | %d | %d | %.1f (%d / %d / %d) | %s | %s | %s | %s | %s | %s | %s | %s | %d | %s |' % (
                cp['percent'], cp['live'], cp['postings'], cp['sizeMean'], cp['sizeP50'], cp['sizeP95'], cp['sizeMax'],
                counts(cp.get('atInsert'), False), counts(cp.get('inserted'), True), counts(cp.get('batch'), True), cp['coverage'],
                fmt(at(pts, 0.95, 'loadsMean')), fmt(at(pts, 0.99, 'loadsMean')), fmt(ef99, '%d'),
                fmt(st['nonNearest'] / ins if ins else None, '%.4f'), st['splits'],
                fmt(cp['insertSec'] * 1e6 / ins if ins else None, '%.0f')))
        last = doc['checkpoints'][-1] if doc['checkpoints'] else None
        if last and last['percent'] == 100 and refAt:
            st, ins = last['stats'], last['stats']['inserted']
            print('\nFases por insert, µs (indicativo): asignación %.0f, centroide exacto %.0f, búsqueda de aristas %.0f, escritura %.0f, inversas %.0f, split %.0f, reparación %.0f.\n' % tuple(
                st[f] / 1e3 / ins for f in ('assignNs', 'nearestCheckNs', 'edgeSearchNs', 'writeNs', 'inverseNs', 'splitNs', 'repairNs')))
            pts = curve(last['points'])
            print('Al 100 %, frente a la referencia en lote (recall a igual ef):\n')
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
    edge_low = [(k, cp['percent'], cp['atInsert']['captured'] / max(cp['atInsert']['wanted'], 1))
                for k, v in cells.items() for cp in v[1]['checkpoints']
                if cp.get('atInsert', {}).get('vectors') and cp['atInsert']['captured'] / max(cp['atInsert']['wanted'], 1) < MIN_EDGE_RECALL]
    holds = worst >= -MAX_DEFICIT and loads is not None and loads <= MAX_LOADS
    print('- **Celda principal al 100 %%:** peor diferencia de recall a igual ef %+.4f (umbral −%.3f); cargas a 0.95 %+.1f %% (umbral +%d %%).' % (
        worst, MAX_DEFICIT, 100 * loads, 100 * MAX_LOADS))
    print('- **H1 y H2 %s.**' % ('se sostienen' if holds else 'no se sostienen'))
    if stops:
        print('- **Stop-and-report en:** %s.' % ', '.join(stops))
    if edge_low:
        print('- **Guardia del constructor, recall de arista al insertar bajo 0.90:** %s.' % ', '.join('%s en el tramo al %d %% (%.4f)' % e for e in edge_low))
    for key in ('h1', 'h2', 'both', 'siblings', 'inverse-siblings'):
        if key in cells and len(cells[key]) == 4:
            _, _, w, l = cells[key]
            print('- **%s:** peor diferencia %+.4f (principal %+.4f), cargas a 0.95 %+.1f %% (principal %+.1f %%).' % (
                cells[key][0], w, worst, 100 * l, 100 * loads))
    if 'both' not in cells:
        print('- **La celda 4, «las dos», no corrió.**')
    floor = noise(cells)
    work(cells, refpts)
    inverse_writes(cells)
    siblings_vs_h2(cells, floor)
    cell6_vs_h1(cells, floor)


def final(doc):
    """The points of the 100 % checkpoint of a cell, None before it."""
    last = doc['checkpoints'][-1] if doc['checkpoints'] else None
    return curve(last['points']) if last and last['percent'] == 100 else None


def noise(cells):
    """The noise between runs, by ef: the spread of the cells at the 50 %
    checkpoint, and the difference between the two runs of H1 at 100 %."""
    at50 = {}
    for key in SAME_BATCH:
        if key in cells and cells[key][1]['checkpoints'] and cells[key][1]['checkpoints'][0]['percent'] == 50:
            for p in curve(cells[key][1]['checkpoints'][0]['points']):
                at50.setdefault(p['ef'], []).append((key, p))
    runs = sorted({k for v in at50.values() for k, _ in v}, key=SAME_BATCH.index)
    a = final(cells['h1'][1]) if 'h1' in cells else None
    b = final(cells['h1-writes'][1]) if 'h1-writes' in cells else None
    at100 = {}
    if a and b:
        pb = {p['ef']: p for p in b}
        at100 = {p['ef']: abs(p['recall'] - pb[p['ef']]['recall']) for p in a if p['ef'] in pb}
    if len(runs) < 2:
        return {}
    print('\n### Suelo de ruido entre corridas\n')
    print('El checkpoint del 50 %% es el mismo build en lote en %d corridas (%s).\n' % (len(runs), ', '.join(runs)))
    print('| ef | recall al 50 %: mínimo | máximo | diferencia | cargas: diferencia relativa | H1 frente a su repetición al 100 % |')
    print('|---|---|---|---|---|---|')
    floor = {}
    for ef in sorted(at50):
        r = [p['recall'] for _, p in at50[ef]]
        l = [p['stats']['loadsMean'] for _, p in at50[ef]]
        floor[ef] = max(max(r) - min(r), at100.get(ef, 0))
        print('| %d | %.4f | %.4f | %.4f | %.2f %% | %s |' % (
            ef, min(r), max(r), max(r) - min(r), 100 * (max(l) / min(l) - 1), fmt(at100.get(ef), '%.4f')))
    if a and b:
        la, lb = at(a, 0.95, 'loadsMean'), at(b, 0.95, 'loadsMean')
        if la and lb:
            floor['loads'] = abs(la / lb - 1)
            print('\nCargas a 0.95 de H1 al 100 %%: %.2f y %.2f en las dos corridas (%.1f %% de diferencia).' % (la, lb, 100 * floor['loads']))
    return floor


def work(cells, refpts):
    """Loads and endpoints scored at equal recall, and the mean degree, at 100 %."""
    rows = [('referencia en lote', refpts, None)] if refpts else []
    for key, v in cells.items():
        pts = final(v[1])
        if pts:
            rows.append((v[0], pts, v[1]['checkpoints'][-1]))
    if not rows:
        return
    print('\n### Trabajo a igual recall y grado medio, al 100 %\n')
    print('| celda | cargas a 0.95 | endpoints puntuados a 0.95 | cargas a 0.99 | endpoints puntuados a 0.99 | grado medio: insertados | lote |')
    print('|---|---|---|---|---|---|---|')
    for name, pts, cp in rows:
        deg = lambda c: fmt(c['edges'] / c['vectors'] if c and c.get('vectors') else None, '%.2f')
        print('| %s | %s | %s | %s | %s | %s | %s |' % (
            name, fmt(at(pts, 0.95, 'loadsMean')), fmt(at(pts, 0.95, 'endpointsMean'), '%.0f'),
            fmt(at(pts, 0.99, 'loadsMean')), fmt(at(pts, 0.99, 'endpointsMean'), '%.0f'),
            deg(cp.get('inserted')) if cp else '—', deg(cp.get('batch')) if cp else '—'))


def quantile(hist, q):
    """The smallest value with at least the share q of the inserts at or below it."""
    total, seen = sum(hist), 0
    for value, count in enumerate(hist):
        seen += count
        if seen >= q * total:
            return value
    return len(hist) - 1


def hist_line(hist):
    total = max(sum(hist), 1)
    mean = sum(v * c for v, c in enumerate(hist)) / total
    top = max((v for v, c in enumerate(hist) if c), default=0)
    return '%.2f | %d | %d | %d | %.1f %%' % (mean, quantile(hist, 0.5), quantile(hist, 0.95), top, 100 * hist[0] / total)


def inverse_writes(cells):
    """What the inverse edges of an insert read and write, in counts."""
    rows = [(k, v) for k, v in cells.items() if v[1]['checkpoints'] and v[1]['checkpoints'][-1]['stats'].get('inverseEdges')]
    if not rows:
        return
    print('\n### Escrituras de aristas inversas por insert\n')
    print('Cada arista inversa es una escritura a la lista de otro vector. «Postings» cuenta los postings distintos donde viven esas listas: lo que escribe un insert si las aristas se guardan con su posting.\n')
    print('| celda | tramo | inserts | listas con aristas leídas por insert | distancias exactas por insert | escrituras: media | p50 | p95 | máximo | inserts sin ninguna | postings distintos: media | p50 | p95 | máximo | inserts sin ninguno |')
    print('|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|')
    for key, v in rows:
        cps = v[1]['checkpoints']
        stretches = [('hasta el %d %%' % cps[-1]['percent'], None, cps[-1]['stats'])]
        if len(cps) > 2:
            stretches += [('%d a %d %%' % (a['percent'], b['percent']), a['stats'], b['stats']) for a, b in zip(cps, cps[1:])]
        for label, prev, st in stretches:
            def d(f):
                return st.get(f, 0) - (prev.get(f, 0) if prev else 0)
            ins = max(d('inserted'), 1)
            if 'inverseWrites' not in st:
                print('| %s | %s | %d | — | — | %.2f | — | — | — | — | — | — | — | — | — |' % (v[0], label, ins, d('inverseEdges') / ins))
                continue
            def h(f):
                before = prev.get(f) if prev else None
                return [c - (before[i] if before else 0) for i, c in enumerate(st[f])]
            print('| %s | %s | %d | %.2f | %.1f | %s | %s |' % (
                v[0], label, ins, d('inverseChecks') / ins, d('inverseDistances') / ins,
                hist_line(h('inverseWrites')), hist_line(h('inversePostings'))))
    a, b = cells.get('h1'), cells.get('h1-writes')
    if a and b and final(a[1]) and final(b[1]):
        sa, sb = a[1]['checkpoints'][-1]['stats'], b[1]['checkpoints'][-1]['stats']
        print('\nLa repetición de H1 frente a la corrida original, al 100 %%: %.3f escrituras por insert frente a %.3f, %d splits frente a %d.' % (
            sb['inverseEdges'] / sb['inserted'], sa['inverseEdges'] / sa['inserted'], sb['splits'], sa['splits']))

def per_split(doc, fields):
    st = doc['checkpoints'][-1]['stats']
    return sum(st[f] for f in fields) / 1e3 / max(st['splits'], 1), st


def floor_line(diffs, floor, threshold):
    """Says whether the differences past the threshold are past the noise too."""
    past = [(ef, d) for ef, d in diffs if abs(d) > threshold]
    if not past or not floor:
        return
    inside = [(ef, d) for ef, d in past if abs(d) <= floor.get(ef, 0)]
    print('- **Frente al suelo de ruido:** %s' % ('; '.join(
        'ef %d: %+.4f, con un suelo de %.4f%s' % (ef, d, floor.get(ef, 0), ' (dentro)' if abs(d) <= floor.get(ef, 0) else '') for ef, d in past)) + '.')
    if len(inside) == len(past):
        print('- **Todas las diferencias que pasan el umbral están dentro del ruido entre corridas: el veredicto de recall no se distingue de él.**')


def siblings_vs_h2(cells, floor):
    if 'siblings' not in cells or 'h2' not in cells or len(cells['siblings']) < 4 or len(cells['h2']) < 4:
        return
    sib, h2 = cells['siblings'][1], cells['h2'][1]
    ps = curve(sib['checkpoints'][-1]['points'])
    ph = {p['ef']: p for p in curve(h2['checkpoints'][-1]['points'])}
    print('\n### Celda 5 frente a H2, al 100 %\n')
    print('| ef | H2 | celda 5 | diferencia |')
    print('|---|---|---|---|')
    worst, diffs = 0, []
    for p in ps:
        if p['ef'] in ph:
            d = p['recall'] - ph[p['ef']]['recall']
            worst = min(worst, d)
            diffs.append((p['ef'], d))
            print('| %d | %.4f | %.4f | %+.4f |' % (p['ef'], ph[p['ef']]['recall'], p['recall'], d))
    l5, lh = at(ps, 0.95, 'loadsMean'), at(curve(h2['checkpoints'][-1]['points']), 0.95, 'loadsMean')
    us5, st5 = per_split(sib, ('splitNs', 'siblingNs'))
    ush, sth = per_split(h2, ('splitNs', 'repairNs'))
    matches = worst >= -MATCH_RECALL and l5 is not None and lh and l5 <= lh * (1 + MATCH_LOADS)
    print('\n- **Recall:** peor diferencia frente a H2 %+.4f (umbral −%.3f). **Cargas a 0.95:** %s frente a %s (%s; umbral +%d %%).' % (
        worst, MATCH_RECALL, fmt(l5), fmt(lh), fmt(100 * (l5 / lh - 1) if l5 and lh else None, '%+.1f %%'), 100 * MATCH_LOADS))
    print('- **Coste por split, µs (indicativo; H2 corrió en paralelo con otras dos celdas, la 5 sola):** celda 5 %.0f, H2 %.0f.' % (us5, ush))
    print('- **Trabajo por split, sin depender de la máquina:** celda 5, %.0f distancias exactas; H2, %.1f búsquedas.' % (
        st5['siblingDistances'] / max(st5['splits'], 1), sth['repaired'] / max(sth['splits'], 1)))
    print('- **Celda 5 %s a H2 sin búsqueda: %s.**' % ('iguala o supera' if matches else 'no iguala',
          'candidata a mecanismo de split para disco' if matches else 'no es la candidata'))
    floor_line([(ef, d) for ef, d in diffs if d < 0], floor, MATCH_RECALL)


def cell6_vs_h1(cells, floor):
    if 'inverse-siblings' not in cells or 'h1' not in cells:
        return
    six, h1 = cells['inverse-siblings'][1], cells['h1'][1]
    p6, p1 = final(six), final(h1)
    if not p6 or not p1:
        return
    ph = {p['ef']: p for p in p1}
    rep = final(cells['h1-writes'][1]) if 'h1-writes' in cells else None
    pr = {p['ef']: p for p in rep} if rep else {}
    print('\n### Celda 6 frente a H1, al 100 %\n')
    print('| ef | H1 | H1, repetición | celda 6 | diferencia frente a H1 | frente a la repetición | suelo de ruido |')
    print('|---|---|---|---|---|---|---|')
    diffs = []
    for p in p6:
        if p['ef'] not in ph:
            continue
        d = p['recall'] - ph[p['ef']]['recall']
        diffs.append((p['ef'], d))
        r = pr.get(p['ef'])
        print('| %d | %.4f | %s | %.4f | %+.4f | %s | %s |' % (
            p['ef'], ph[p['ef']]['recall'], fmt(r['recall'] if r else None, '%.4f'), p['recall'], d,
            fmt(p['recall'] - r['recall'] if r else None, '%+.4f'), fmt(floor.get(p['ef']) if floor else None, '%.4f')))
    best, worst = max(d for _, d in diffs), min(d for _, d in diffs)
    l6, l1 = at(p6, 0.95, 'loadsMean'), at(p1, 0.95, 'loadsMean')
    loads = l6 / l1 - 1 if l6 and l1 else None
    adds = best > MATCH_RECALL or (loads is not None and loads < -MATCH_LOADS)
    loses = worst < -MATCH_RECALL or (loads is not None and loads > MATCH_LOADS)
    print('\n- **Recall:** mejor diferencia frente a H1 %+.4f, peor %+.4f (umbral ±%.3f). **Cargas a 0.95:** %s frente a %s (%s; umbral ∓%d %%).' % (
        best, worst, MATCH_RECALL, fmt(l6), fmt(l1), fmt(100 * loads if loads is not None else None, '%+.1f %%'), 100 * MATCH_LOADS))
    if adds and loses:
        verdict = 'aporta por un lado y pierde por otro: veredicto mixto'
    elif adds:
        verdict = 'los hermanos aportan sobre las aristas inversas'
    else:
        verdict = 'los hermanos no hacen falta cuando hay aristas inversas'
    print('- **Celda 6 frente a H1: %s.**' % verdict)
    floor_line(diffs, floor, MATCH_RECALL)
    if floor and 'loads' in floor and loads is not None and abs(loads) > MATCH_LOADS:
        print('- **Cargas frente al suelo de ruido:** %+.1f %%, con %.1f %% entre las dos corridas de H1%s.' % (
            100 * loads, 100 * floor['loads'], ' (dentro)' if abs(loads) <= floor['loads'] else ''))
    st6, st1 = six['checkpoints'][-1]['stats'], h1['checkpoints'][-1]['stats']
    splits, ins = max(st6['splits'], 1), max(st6['inserted'], 1)
    print('- **Trabajo por split, sin depender de la máquina:** %.0f distancias exactas, %.1f listas de aristas que cambian y %.1f aristas hacia la mitad hermana. Repartido entre los inserts: %.2f listas cambiadas por insert.' % (
        st6['siblingDistances'] / splits, st6.get('siblingLists', 0) / splits, st6['siblingEdges'] / splits,
        st6.get('siblingLists', 0) / ins))
    print('- **Trabajo por insert, sin depender de la máquina:** una búsqueda (k=16, ef=%d); %.2f listas con aristas leídas y %.2f escrituras de aristas inversas (H1: %.2f escrituras).' % (
        six['insertEF'], st6.get('inverseChecks', 0) / ins, st6['inverseEdges'] / ins, st1['inverseEdges'] / max(st1['inserted'], 1)))
    us = lambda st, fields: sum(st[f] for f in fields) / 1e3
    print('- **µs, indicativos (H1 corrió en paralelo con otras dos celdas, la 6 sola):** por insert, celda 6 %.0f y H1 %.0f; por split con su reparación, celda 6 %.0f.' % (
        six['checkpoints'][-1]['insertSec'] * 1e6 / ins, h1['checkpoints'][-1]['insertSec'] * 1e6 / max(st1['inserted'], 1),
        us(st6, ('splitNs', 'siblingNs')) / splits))


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else '.')
