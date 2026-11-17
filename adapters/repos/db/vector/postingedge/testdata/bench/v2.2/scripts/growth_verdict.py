#!/usr/bin/env python3
"""The verdict of v2.2, growing from 10 % over H1 (bench-report.md, "v2.2: crecer
desde el 10 %, sobre H1"), from the files of run_v22_growth.sh.

usage: growth_verdict.py <v2.2 dir> <v2.0 dir> <v2.1 dir> [suffix] > growth-verdict.md

A cell is the H1 mechanism of v2.0, inverse edges without split repair, over a
batch build of the first 10 % of the train set, with the other 90 % inserted.
It passes when, at 100 % of the corpus and against the batch reference of
v2.0, its recall at every search ef of the grid is no more than 0.005 below,
its loads at recall 0.95 no more than 15 % above and its loads at 0.99 no more
than 10 % above. Its final mean posting size has to stay within 15 % of the
batch's. The cells of v2.1 with the same insert ef, which grew from 50 %, tell
whether the drift grows with the inserted stretch. suffix is that of the
cells of a recalibrated run, "-t<threshold>".
"""
import json
import math
import os
import sys

EFS = (160, 80)
MAX_DEFICIT, MAX_LOADS_95, MAX_LOADS_99, MIN_EDGE_RECALL, SIZE_BAND = 0.005, 0.15, 0.10, 0.90, 0.15


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


def mill(x):
    """A count, in millions."""
    return '—' if x is None else ('%.1f M' % (x / 1e6) if x < 1e10 else '%.0f M' % (x / 1e6))


def quantile(hist, q):
    """The smallest value with at least the share q of the inserts at or below it."""
    total, seen = sum(hist), 0
    for v, count in enumerate(hist):
        seen += count
        if seen >= q * total:
            return v
    return len(hist) - 1


def minus(a, b):
    """The histogram a without the counts of b, an earlier state of it."""
    b = b or []
    return [x - (b[i] if i < len(b) else 0) for i, x in enumerate(a)]


def recall_of(c):
    return c['captured'] / max(c['wanted'], 1), c['postingsCaptured'] / max(c['postingsWanted'], 1)


def degree_of(c):
    return c['edges'] / max(c['vectors'], 1)


def stops_of(doc):
    """(percent, cumulative stats, atInsert) of every stop of a cell after its batch part, in order."""
    start = doc.get('start', 50)
    stops = [(g['percent'], g['stats'], g['atInsert']) for g in doc.get('guards', [])]
    stops += [(cp['percent'], cp['stats'], cp['atInsert']) for cp in doc['checkpoints'] if cp['percent'] > start]
    return sorted(stops, key=lambda s: s[0])


class Cell:
    def __init__(self, label, ef, doc):
        self.label, self.ef, self.doc = label, ef, doc
        self.stopped = doc.get('stopped') if doc else None
        self.start = doc.get('start', 50) if doc else None
        cps = doc['checkpoints'] if doc else []
        self.final = cps[-1] if cps and cps[-1]['percent'] == 100 else None
        self.pts = curve(self.final['points']) if self.final else None

    def latest(self):
        stops = (self.doc.get('guards', []) + self.doc['checkpoints']) if self.doc else []
        return max(stops, key=lambda x: x['percent']) if stops else None

    def state(self):
        if self.doc is None:
            return 'sin datos'
        if self.stopped:
            return 'cortada: ' + self.stopped
        return 'completa' if self.final else 'sin acabar'

    def stat_at(self, percent):
        """The cumulative stats at a stop, {} at the start."""
        for p, st, _ in stops_of(self.doc):
            if p == percent:
                return st
        return {} if percent == self.start else None


def main(d, v20, v21, suffix=''):
    ref = load(os.path.join(v20, 'dbpedia-1m-stream-principal-reference.json'))
    if not ref:
        print('Sin la referencia en lote de v2.0: sin veredicto.')
        return
    refpts = curve(ref['points'])
    ref95, ref99 = at(refpts, 0.95, 'loadsMean'), at(refpts, 0.99, 'loadsMean')
    work = load(os.path.join(d, 'dbpedia-1m-batch-buildwork.json'))
    cells = [Cell('%d, desde el 10 %%' % ef, ef, load(os.path.join(d, 'dbpedia-1m-grow10-h1-ef%d%s.json' % (ef, suffix)))) for ef in EFS]
    halves = [Cell('%d, desde el 50 %% (v2.1)' % ef, ef, load(os.path.join(v21, 'dbpedia-1m-stream-h1-ef%d.json' % ef))) for ef in EFS]
    half_of = {c.ef: c for c in halves if c.doc}
    rows = [c for c in cells if c.doc]

    # The noise floor at 100 %: the two runs of H1 at ef 320 of v2.0.
    a = Cell('', 320, load(os.path.join(v20, 'dbpedia-1m-stream-h1.json')))
    b = Cell('', 320, load(os.path.join(v20, 'dbpedia-1m-stream-h1-writes.json')))
    floor, floor95, floor99 = None, None, None
    if a.pts and b.pts:
        other = {p['ef']: p['recall'] for p in b.pts}
        floor = max(abs(p['recall'] - other[p['ef']]) for p in a.pts if p['ef'] in other)
        floor95 = abs(at(b.pts, 0.95, 'loadsMean') / at(a.pts, 0.95, 'loadsMean') - 1)
        floor99 = abs(at(b.pts, 0.99, 'loadsMean') / at(a.pts, 0.99, 'loadsMean') - 1)

    batch_mean = work['sizeMean'] if work else None
    print('### Estado de las celdas\n')
    if work:
        print('El lote del 100 %%: %d postings de %.1f vectores de media (p50 %d, p95 %d, máximo %d). La banda de ±%d %% va de %.1f a %.1f.\n' % (
            work['postings'], batch_mean, work['sizeP50'], work['sizeP95'], work['sizeMax'], 100 * SIZE_BAND,
            batch_mean * (1 - SIZE_BAND), batch_mean * (1 + SIZE_BAND)))
    else:
        print('Sin el build de conteos del lote: sin banda de tamaño.\n')
    print('| celda | estado | umbral | splits (fallidos) | tamaño medio final (p50 / p95 / máx) | frente al lote | cobertura | no-nearest | µs por insert, indicativo |')
    print('|---|---|---|---|---|---|---|---|---|')
    in_band = {}
    for c in rows:
        cp, last = c.final, c.latest()
        st = last['stats']
        sizes, band = '—', '—'
        if cp:
            sizes = '%.1f (%d / %d / %d)' % (cp['sizeMean'], cp['sizeP50'], cp['sizeP95'], cp['sizeMax'])
            if batch_mean:
                rel = cp['sizeMean'] / batch_mean - 1
                in_band[c.label] = abs(rel) <= SIZE_BAND
                band = '%s: %s' % (pct(rel), 'dentro de la banda' if in_band[c.label] else '**fuera de la banda**')
        print('| %s | %s | %d | %d (%d) | %s | %s | %s | %.4f | %s |' % (
            c.label, c.state(), c.doc['threshold'], st['splits'], st['splitsFailed'], sizes, band, last['coverage'],
            st['nonNearest'] / max(st['inserted'], 1),
            fmt(last['insertSec'] * 1e6 / st['inserted'] if st['inserted'] else None, '%.0f')))

    print('\n### Checkpoints\n')
    print('El recall y las cargas de los checkpoints intermedios son informativos: no hay lote de ese tamaño con el que compararlos.\n')
    for c in rows:
        print('**ef de inserción %d:**\n' % c.ef)
        print('| checkpoint | vivos | postings | tamaño medio (p50 / p95 / máx) | al insertar, en el tramo: arista / posting | insertados ahora: arista / posting (grado) '
              '| lote ahora: arista / posting (grado) | cobertura | recall a ef 10 | cargas a 0.95 | a 0.99 | truth por fuerza bruta |')
        print('|---|---|---|---|---|---|---|---|---|---|---|---|')
        for cp in c.doc['checkpoints']:
            pts = curve(cp['points']) if cp.get('points') else None
            edge = lambda e: '%.4f / %.4f (%.2f)' % (recall_of(e) + (degree_of(e),)) if e.get('vectors') else '—'
            print('| %d %% | %d | %s | %s | %s | %s | %s | %s | %s | %s | %s | %d |' % (
                cp['percent'], cp['live'], fmt(cp.get('postings'), '%d') if cp.get('postings') else '—',
                '%.1f (%d / %d / %d)' % (cp['sizeMean'], cp['sizeP50'], cp['sizeP95'], cp['sizeMax']) if cp.get('postings') else '—',
                '%.4f / %.4f' % recall_of(cp['atInsert']) if cp['atInsert'].get('vectors') else '—',
                edge(cp['inserted']), edge(cp['batch']), cp['coverage'],
                fmt(next((p['recall'] for p in pts if p['ef'] == 10), None), '%.4f') if pts else '—',
                fmt(at(pts, 0.95, 'loadsMean')) if pts else '—', fmt(at(pts, 0.99, 'loadsMean')) if pts else '—',
                cp.get('truthFallbacks', 0)))
        print()

    done = [c for c in rows if c.pts]
    shown = []
    for c in done:
        shown.append(c)
        if c.ef in half_of and half_of[c.ef].pts:
            shown.append(half_of[c.ef])
    print('### Recall a igual ef de búsqueda al 100 %, diferencia frente al lote\n')
    print('| ef de búsqueda | lote | ' + ' | '.join(c.label for c in shown) + ' |')
    print('|---|---|' + '---|' * len(shown))
    worst = {c.label: math.inf for c in shown}
    for p in refpts:
        line = []
        for c in shown:
            mine = next((q['recall'] for q in c.pts if q['ef'] == p['ef']), None)
            if mine is None:
                line.append('—')
                continue
            diff = mine - p['recall']
            worst[c.label] = min(worst[c.label], diff)
            line.append('%+.4f' % diff)
        print('| %d | %.4f | %s |' % (p['ef'], p['recall'], ' | '.join(line)))
    print('| peor | | ' + ' | '.join('%+.4f' % worst[c.label] for c in shown) + ' |')

    def verdict(passes, margin, noise):
        word = 'cumple' if passes else 'no cumple'
        return word + (', dentro del ruido' if noise is not None and abs(margin) < noise else '')

    print('\n### Criterio\n')
    print('| celda | peor diferencia de recall (umbral −%.3f) | cargas a 0.95 (umbral +%d %%) | cargas a 0.99 (umbral +%d %%) '
          '| guardia: peor tramo (umbral %.2f) | cumple |' % (MAX_DEFICIT, 100 * MAX_LOADS_95, 100 * MAX_LOADS_99, MIN_EDGE_RECALL))
    print('|---|---|---|---|---|---|')
    for c in rows + [half_of[c.ef] for c in rows if c.ef in half_of]:
        guard = [recall_of(ai)[0] for _, _, ai in stops_of(c.doc) if ai.get('vectors')]
        guard_txt = fmt(min(guard), '%.4f') if guard else '—'
        if not c.pts:
            print('| %s | — | — | — | %s | **no:** %s |' % (c.label, guard_txt, c.state()))
            continue
        l95, l99 = at(c.pts, 0.95, 'loadsMean'), at(c.pts, 0.99, 'loadsMean')
        d95 = l95 / ref95 - 1 if l95 else None
        d99 = l99 / ref99 - 1 if l99 else None
        w = worst[c.label]
        ok_r, ok_95, ok_99 = w >= -MAX_DEFICIT, d95 is not None and d95 <= MAX_LOADS_95, d99 is not None and d99 <= MAX_LOADS_99
        ok_g = not guard or min(guard) >= MIN_EDGE_RECALL
        print('| %s | %+.4f: %s | %s (%s): %s | %s (%s): %s | %s | **%s** |' % (
            c.label, w, verdict(ok_r, w + MAX_DEFICIT, floor),
            fmt(l95), pct(d95), verdict(ok_95, MAX_LOADS_95 - d95, floor95) if d95 is not None else 'no llega a 0.95',
            fmt(l99), pct(d99), verdict(ok_99, MAX_LOADS_99 - d99, floor99) if d99 is not None else 'no llega a 0.99',
            guard_txt, 'sí' if ok_r and ok_95 and ok_99 and ok_g else 'no'))
    print()
    out = [c.label for c in rows if in_band.get(c.label) is False]
    if out:
        print('- **El tamaño medio final cae fuera de la banda en: %s.** La regla pide recalibrar el umbral.' % '; '.join(out))
    elif in_band:
        print('- **El tamaño medio final queda dentro de la banda** en las celdas que acabaron. El umbral no se recalibra.')
    if floor is not None:
        print('- **Suelo de ruido, de las dos corridas de H1 con ef 320 de v2.0:** hasta %.4f de recall, un %.1f %% en cargas a 0.95 y un %.1f %% a 0.99.' % (
            floor, 100 * floor95, 100 * floor99))

    print('\n### Por tramo\n')
    print('Lo que hicieron los inserts de cada tramo. Las cargas son las de la búsqueda de aristas de cada insert; el grado al insertar y el recall de arista, '
          'de la muestra de la guardia.\n')
    for c in rows:
        print('**ef de inserción %d:**\n' % c.ef)
        print('| tramo | inserts | splits | no-nearest | cargas por insert: media (p50 / p95) | rondas | vectores escaneados | endpoints '
              '| recall de arista al insertar (posting) | grado al insertar | escrituras inversas: media (p95) | postings distintos: media (p95) | listas leídas |')
        print('|---|---|---|---|---|---|---|---|---|---|---|---|---|')
        prev, prev_percent = {}, c.start
        for percent, st, ai in stops_of(c.doc):
            n = st['inserted'] - prev.get('inserted', 0)
            if n <= 0:
                continue
            dif = lambda k: (st.get(k, 0) - prev.get(k, 0)) / n
            hist = minus(st['searchLoadsHist'], prev.get('searchLoadsHist'))
            w = minus(st['inverseWrites'], prev.get('inverseWrites'))
            po = minus(st['inversePostings'], prev.get('inversePostings'))
            print('| %d → %d %% | %d | %d | %.4f | %.1f (%d / %d) | %.1f | %.0f | %.0f | %s | %s | %.2f (%d) | %.2f (%d) | %.2f |' % (
                prev_percent, percent, n, st['splits'] - prev.get('splits', 0), dif('nonNearest'),
                dif('searchLoads'), quantile(hist, 0.5), quantile(hist, 0.95), dif('searchRounds'), dif('searchScanned'), dif('searchEndpoints'),
                '%.4f (%.4f)' % recall_of(ai) if ai.get('vectors') else '—', fmt(degree_of(ai), '%.2f') if ai.get('vectors') else '—',
                dif('inverseEdges'), quantile(w, 0.95), sum(v * k for v, k in enumerate(po)) / n, quantile(po, 0.95), dif('inverseChecks')))
            prev, prev_percent = st, percent
        last = c.latest()['stats']
        if last.get('inserted'):
            n = last['inserted']
            w, po = last['inverseWrites'], last['inversePostings']
            print('| todo | %d | %d | %.4f | %.1f (%d / %d) | %.1f | %.0f | %.0f | | | %.2f (%d) | %.2f (%d) | %.2f |' % (
                n, last['splits'], last['nonNearest'] / n, last['searchLoads'] / n, quantile(last['searchLoadsHist'], 0.5),
                quantile(last['searchLoadsHist'], 0.95), last['searchRounds'] / n, last['searchScanned'] / n, last['searchEndpoints'] / n,
                last['inverseEdges'] / n, quantile(w, 0.95), sum(v * k for v, k in enumerate(po)) / n, quantile(po, 0.95),
                last['inverseChecks'] / n))
        print()

    print('### Grado por cohorte\n')
    print('El grado medio de todos los vectores de cada cohorte, sin muestra, en cada checkpoint: los del lote y los inserts de cada tramo.\n')
    for c in rows:
        cohorts = c.doc['checkpoints'][-1].get('cohorts') or []
        n = c.doc['vectors']
        names = ['lote' if co['from'] == 0 else '%d → %d %%' % (round(100 * co['from'] / n), round(100 * co['to'] / n)) for co in cohorts]
        print('**ef de inserción %d:**\n' % c.ef)
        print('| checkpoint | ' + ' | '.join(names) + ' | todos |')
        print('|---|' + '---|' * (len(names) + 1))
        for cp in c.doc['checkpoints']:
            cs = cp.get('cohorts') or []
            line = ['%.2f' % (co['edges'] / co['vectors']) if co['vectors'] else '—' for co in cs]
            line += [''] * (len(names) - len(line))
            total_v, total_e = sum(co['vectors'] for co in cs), sum(co['edges'] for co in cs)
            print('| %d %% | %s | %s |' % (cp['percent'], ' | '.join(line), fmt(total_e / total_v if total_v else None, '%.2f')))
        print()

    print('### Frente a la H1 de 50 → 100 %, al mismo ef de inserción\n')
    print('Al 100 %. La deriva crece con el tramo insertado si la celda que crece desde el 10 % queda peor que la que crece desde el 50 % por más que el suelo de ruido.\n')
    for c in done:
        h = half_of.get(c.ef)
        if not h or not h.pts:
            print('**ef de inserción %d:** sin la celda de v2.1.\n' % c.ef)
            continue
        print('**ef de inserción %d:**\n' % c.ef)
        print('| al 100 % | desde el 50 % (v2.1) | desde el 10 % (v2.2) | diferencia | suelo de ruido |')
        print('|---|---|---|---|---|')
        hr = {p['ef']: p['recall'] for p in h.pts}
        for p in c.pts:
            if p['ef'] in hr:
                diff = p['recall'] - hr[p['ef']]
                print('| recall a ef %d | %.4f | %.4f | %+.4f%s | %s |' % (
                    p['ef'], hr[p['ef']], p['recall'], diff, '' if floor is None or abs(diff) > floor else ', dentro del ruido', fmt(floor, '%.4f')))
        for target, fl in ((0.95, floor95), (0.99, floor99)):
            for field, name in (('loadsMean', 'cargas'), ('roundsMean', 'rondas'), ('endpointsMean', 'endpoints')):
                x, y = at(h.pts, target, field), at(c.pts, target, field)
                if x and y:
                    rel = y / x - 1
                    noise = fl if field == 'loadsMean' else None
                    print('| %s a %.2f | %s | %s | %s%s | %s |' % (
                        name, target, fmt(x, '%.2f' if field == 'roundsMean' else '%.1f'), fmt(y, '%.2f' if field == 'roundsMean' else '%.1f'), pct(rel),
                        '' if noise is None or abs(rel) > noise else ', dentro del ruido', '%.1f %%' % (100 * noise) if noise is not None else ''))
        fh, fc = h.final, c.final
        for key, name in (('inserted', 'insertados ahora'), ('batch', 'lote ahora')):
            rh, rc = recall_of(fh[key]), recall_of(fc[key])
            print('| recall de arista, %s | %.4f | %.4f | %+.4f | |' % (name, rh[0], rc[0], rc[0] - rh[0]))
            print('| recall de posting, %s | %.4f | %.4f | %+.4f | |' % (name, rh[1], rc[1], rc[1] - rh[1]))
            print('| grado, %s (muestra) | %.2f | %.2f | %+.2f | |' % (name, degree_of(fh[key]), degree_of(fc[key]), degree_of(fc[key]) - degree_of(fh[key])))
        sh, sc = fh['stats'], fc['stats']
        for name, f in (('escrituras inversas por insert', lambda s: s['inverseEdges'] / s['inserted']),
                        ('postings distintos por insert', lambda s: sum(v * k for v, k in enumerate(s['inversePostings'])) / s['inserted']),
                        ('no-nearest', lambda s: s['nonNearest'] / s['inserted'])):
            x, y = f(sh), f(sc)
            print('| %s, acumulado | %.4f | %.4f | %+.4f | |' % (name, x, y, y - x))
        # The last fifth of the corpus, which both cells insert into an index of the same size.
        h80, c80 = h.stat_at(80), c.stat_at(80)
        if h80 and c80:
            for key, name in (('searchLoads', 'cargas por insert'), ('searchRounds', 'rondas por insert'), ('inverseEdges', 'escrituras inversas por insert'),
                              ('nonNearest', 'no-nearest')):
                x = (sh[key] - h80[key]) / (sh['inserted'] - h80['inserted'])
                y = (sc[key] - c80[key]) / (sc['inserted'] - c80['inserted'])
                print('| %s, en el tramo del 80 al 100 %% | %.4f | %.4f | %s | |' % (name, x, y, pct(y / x - 1) if x else '—'))
        print('| postings al final | %d | %d | %s | |' % (fh['postings'], fc['postings'], pct(fc['postings'] / fh['postings'] - 1)))
        print('| tamaño medio de posting | %.1f | %.1f | %s | |' % (fh['sizeMean'], fc['sizeMean'], pct(fc['sizeMean'] / fh['sizeMean'] - 1)))
        print()

    print('### El trabajo del build, en conteos\n')
    if not work:
        print('Sin el build de conteos del lote.\n')
    builds = [c for c in rows if c.doc.get('batchBuild')]
    if work and builds:
        print('En millones. El incremental es el lote del 10 % más los inserts hasta donde llegó la celda. Las cargas del lote son visitas a postings, '
              'una cota superior de lo que leería.\n')

        def batch_parts(w):
            return [('cargas de posting', w['postingsProbed']), ('distancias por código', w['candidatesScored']),
                    ('exactas: 2-means de la partición', w['partitionDistances']), ('exactas: asignación', w['assignDistances']),
                    ('exactas: rescore de aristas', w['candidatesRescored']), ('exactas: construir el grafo de centroides', w['centroidGraphDistances'])]

        def insert_parts(st):
            return [('cargas de posting', st['searchLoads']), ('distancias por código', st['searchScanned'] + st['searchEndpoints']),
                    ('exactas: asignación por el grafo', st['assignDistances']),
                    ('exactas: entrada de la búsqueda, por el grafo', st.get('searchCentroidDistances', 0)),
                    ('exactas: rescore de la búsqueda', st['searchRescored']), ('exactas: comprobar las aristas inversas', st['inverseDistances']),
                    ('exactas: 2-means y medias de los splits', st['splitDistances']), ('exactas: altas y bajas de centroides en el grafo', st['splitGraphDistances']),
                    ('exactas: limpieza de lápidas', st['cleanDistances'])]

        def totals(parts):
            loads = sum(v for k, v in parts if k.startswith('cargas'))
            code = sum(v for k, v in parts if k.startswith('distancias por código'))
            exact = sum(v for k, v in parts if k.startswith('exactas'))
            return loads, code, exact

        full = totals(batch_parts(work))
        print('| | lote del 100 % | ' + ' | '.join('ef %d: lote del 10 %% | ef %d: inserts | ef %d: total | ef %d: frente al lote' % ((c.ef,) * 4) for c in builds) + ' |')
        print('|---|---|' + '---|---|---|---|' * len(builds))
        names = ['cargas de posting', 'distancias por código', 'distancias exactas']
        per = []
        for c in builds:
            bb, st = c.doc['batchBuild'], c.latest()['stats']
            per.append((totals(batch_parts(bb)), totals(insert_parts(st))))
        for i, name in enumerate(names):
            line = []
            for tb, ti in per:
                line.append('%s | %s | %s | %.2fx' % (mill(tb[i]), mill(ti[i]), mill(tb[i] + ti[i]), (tb[i] + ti[i]) / full[i]))
            print('| %s | %s | %s |' % (name, mill(full[i]), ' | '.join(line)))

        print('\n**El detalle del lote del 100 %%** (%d vectores, %d postings):\n' % (work['vectors'], work['postings']))
        print('| parte | millones | por vector |')
        print('|---|---|---|')
        for k, v in batch_parts(work):
            print('| %s | %s | %.1f |' % (k, mill(v), v / work['vectors']))

        for c in builds:
            bb, st = c.doc['batchBuild'], c.latest()['stats']
            n = max(st['inserted'], 1)
            print('\n**El detalle de la celda de ef %d** (lote de %d vectores en %d postings, y %d inserts):\n' % (c.ef, bb['vectors'], bb['postings'], st['inserted']))
            print('| parte | millones | por vector del lote, o por insert |')
            print('|---|---|---|')
            for k, v in batch_parts(bb):
                print('| lote del 10 %%: %s | %s | %.1f |' % (k, mill(v), v / bb['vectors']))
            for k, v in insert_parts(st):
                print('| inserts: %s | %s | %.1f |' % (k, mill(v), v / n))
        print()

    print('### Fases de un insert, µs\n')
    print('Indicativo: las dos celdas y el build de conteos corrieron a la vez.\n')
    print('| celda | asignación | centroide exacto | búsqueda de aristas | escritura | aristas inversas | split | total |')
    print('|---|---|---|---|---|---|---|---|')
    for c in rows:
        cp = c.latest()
        st = cp['stats']
        n = st.get('inserted', 0)
        if not n:
            continue
        print('| %s | %s | %.0f |' % (c.label, ' | '.join(
            '%.0f' % (st.get(k, 0) / n / 1000) for k in ('assignNs', 'nearestCheckNs', 'edgeSearchNs', 'writeNs', 'inverseNs', 'splitNs')),
            cp['insertSec'] * 1e6 / n))


if __name__ == '__main__':
    main(*sys.argv[1:5])
