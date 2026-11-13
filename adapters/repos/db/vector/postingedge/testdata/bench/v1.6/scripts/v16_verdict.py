"""The v1.6 night of 2026-11-13 to 14 read against its pre-registration.

usage: v16_verdict.py <the testdata/bench directory>

Reads, under that directory:
- v1.6/wikidpr-10m-{hnsw,pe}.json and their -repeat twins: the two arms with
  the kernel of #13880;
- v1.6/wikidpr-10m-pe-centroidef{64,48,32,16,8}.json: the untimed points of the
  centroid-ef sweep, and the timed plan of the last value that kept recall,
  with its -repeat twin;
- v1.5/wikidpr-10m-degree-cap0.json and cap0-2.json: the posting-edge anchors
  of the night before, the same build without the kernel;
- v1.4/wikidpr-10m-{hnsw,hnsw-redo}.json: the two HNSW builds without the
  kernel (afternoon and morning), and v1.4/wikidpr-10m-full.json, posting-edge
  without the kernel, the "PE today" of the estimate.

QPS and work at a recall are read on each ef curve as statAtRecall in
bench_cells_report_test.go reads them. Prints Markdown.

Pre-registration (bench-report.md, "v1.6"):
- gate: the HNSW / posting-edge ratio at 0.95 with 1 thread falls in
  1.87x-1.91x +-5 %, from 1.7765x to 2.0055x (written 1.78x to 2.01x); the
  ratio with 8 threads is given against the same band;
- the ratio is read on the first round; if the two rounds of an arm differ by
  more than 2 % in median, it is said and the verdict is given with both;
- outside the band, the four contrasts: the scan in cache and the saving per
  endpoint, the speedup at 8 threads, the HNSW distances, the HNSW build;
- centroid ef: "keeps recall" is losing at most 0.0005 at ef 20 and at ef 160
  against 64; the last value that keeps it is timed and compared at equal recall
  with the main curve; a gain above the drift (2 %) goes as a proposal, the
  default does not change.
"""
import json
import math
import os
import statistics
import sys

LOW, HIGH = 1.87 * 0.95, 1.91 * 1.05
DRIFT, KEEPS = 2.0, 0.0005
RESCORE_HNSW, RESCORE_PE = 512, 200
# the restated estimate (v1.6/kernel-pr-estimate.md), afternoon and morning HNSW builds of v1.4
ESTIMATE = {(0.95, 1): (1.95, 1.94), (0.95, 8): (2.00, 1.93), (0.99, 1): (1.48, 1.18), (0.99, 8): (1.57, 1.16)}
EST_GAIN_PE = {0.95: 1.16, 0.99: 1.22}
EST_GAIN_HNSW = {0.95: (1.06, 1.07), 0.99: (1.05, 1.06)}
# savings per code of the kernel of the PR, from the interleaved rounds (ns)
SAVE_ENDPOINT, SAVE_SCANNED = 16.4, 15.2
# the v1.5 anchor at 0.95, the reference of the scan contrast (ns)
V15_SCANNED_NS, V15_ENDPOINT_NS = 50.6, 72.2
# distances to codes per query of HNSW at 10M, TestSearchWork (v1.6/kernel-pr-estimate.md)
HNSW_DISTANCES = {10: 696, 16: 876, 24: 1109, 32: 1338, 48: 1780, 64: 2209, 96: 3052, 128: 3874,
                  192: 5462, 256: 7000, 384: 9969, 512: 12827, 768: 18275, 1024: 23477}
CENTROID_EFS = (64, 48, 32, 16, 8)


def load(d, name):
    path = os.path.join(d, name)
    return json.load(open(path)) if os.path.exists(path) else None


def curve(doc, threads):
    """The timed ef curve of an arm in the configuration of the gate: HNSW with
    rescoreLimit 512 and posting-edge with 200. The v1.4 files hold other
    rescoreLimits beside them."""
    rescore = RESCORE_HNSW if doc['arm'] == 'hnsw' else RESCORE_PE
    pts = [p for p in doc['points'] if p['threads'] == threads and p['qps'] > 0 and p.get('rescoreLimit') == rescore]
    return sorted(pts, key=lambda p: p['ef'])


def value(p, field):
    if field in p:
        return p[field]
    if field == 'distances':
        return p['stats']['endpointsMean'] + p['stats']['scannedMean']
    return p['stats'][field]


def at(pts, target, field='qps'):
    """The value at a recall level, linearly in recall and in the logarithm of
    the value, as statAtRecall reads it."""
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


def first_ef(pts, target):
    for p in pts:
        if p['recall'] >= target:
            return p['ef']
    return None


def distances(ef):
    """HNSW distances at an ef, linear between the efs TestSearchWork counted."""
    efs = sorted(HNSW_DISTANCES)
    for a, b in zip(efs, efs[1:]):
        if a <= ef <= b:
            return HNSW_DISTANCES[a] + (ef - a) / (b - a) * (HNSW_DISTANCES[b] - HNSW_DISTANCES[a])
    return None


def pct(a, b):
    return 100 * (a / b - 1)


def drift(a, b, th):
    pa = {p['ef']: p['qps'] for p in curve(a, th)}
    r = [pct(p['qps'], pa[p['ef']]) for p in curve(b, th) if p['ef'] in pa]
    return statistics.median(r), min(r), max(r)


def fmt(x, spec='%.0f'):
    return '—' if x is None else spec % x


def gate(ratio):
    return 'dentro' if LOW <= ratio <= HIGH else ('fuera, por encima' if ratio > HIGH else 'fuera, por debajo')


def main(d):
    v16, v15, v14 = (os.path.join(d, v) for v in ('v1.6', 'v1.5', 'v1.4'))
    hnsw, hnsw2 = load(v16, 'wikidpr-10m-hnsw.json'), load(v16, 'wikidpr-10m-hnsw-repeat.json')
    pe, pe2 = load(v16, 'wikidpr-10m-pe.json'), load(v16, 'wikidpr-10m-pe-repeat.json')
    anchor, anchor2 = load(v15, 'wikidpr-10m-degree-cap0.json'), load(v15, 'wikidpr-10m-degree-cap0-2.json')
    tarde, manana, full14 = (load(v14, 'wikidpr-10m-%s.json' % n) for n in ('hnsw', 'hnsw-redo', 'full'))

    print('### Deriva entre las dos rondas\n')
    print('| brazo | threads | mediana de la 2ª ronda contra la 1ª, por punto | mínimo | máximo | |')
    print('|---|---|---|---|---|---|')
    drifted = {}
    for name, a, b in (('HNSW', hnsw, hnsw2), ('posting-edge', pe, pe2)):
        for th in (1, 8):
            med, lo, hi = drift(a, b, th)
            drifted[(name, th)] = abs(med) > DRIFT
            print('| %s | %d | %+.1f %% | %+.1f %% | %+.1f %% | %s |' % (
                name, th, med, lo, hi, '**deriva**' if abs(med) > DRIFT else 'sin deriva'))

    print('\n### El gate: HNSW / posting-edge a igual recall\n')
    print('Banda: de %.4fx a %.4fx (1.87x–1.91x ±5 %%, escrita de 1.78x a 2.01x). '
          'HNSW con rescoreLimit 512, posting-edge con 200.\n' % (LOW, HIGH))
    print('| recall | threads | HNSW, 1ª / 2ª ronda | posting-edge, 1ª / 2ª | ratio, 1ª ronda | ratio, 2ª ronda | estimación rehecha (tarde / mañana) | v1.4, sin kernel (tarde / mañana) | gate |')
    print('|---|---|---|---|---|---|---|---|---|')
    ratios = {}
    for t in (0.95, 0.99):
        for th in (1, 8):
            h = (at(curve(hnsw, th), t), at(curve(hnsw2, th), t))
            p = (at(curve(pe, th), t), at(curve(pe2, th), t))
            r = (h[0] / p[0], h[1] / p[1])
            old = tuple(at(curve(x, th), t) / at(curve(full14, th), t) for x in (tarde, manana))
            ratios[(t, th)] = r
            g = '%s / %s' % (gate(r[0]), gate(r[1])) if t == 0.95 else '—'
            print('| %.2f | %d | %.0f / %.0f | %.0f / %.0f | %.3fx | %.3fx | %.2fx / %.2fx | %.2fx / %.2fx | %s |' % (
                t, th, h[0], h[1], p[0], p[1], r[0], r[1], ESTIMATE[(t, th)][0], ESTIMATE[(t, th)][1], old[0], old[1], g))

    print('\n### Contraste 1: el scan en caché y el ahorro por endpoint\n')
    print('Pase instrumentado de posting-edge, 1 thread, a igual recall. El build es el del ancla de v1.5.\n')
    print('| recall | corrida | ef | escaneados | scan, µs | ns por escaneado | endpoints | endpoints, µs | ns por endpoint | centroides, µs | total, µs |')
    print('|---|---|---|---|---|---|---|---|---|---|---|')
    per = {}
    for t in (0.95, 0.99):
        for name, doc in (('v1.5, sin kernel', anchor), ('esta noche', pe)):
            a = curve(doc, 1)
            sc, scu, en, enu = (at(a, t, f) for f in ('scannedMean', 'scanUs', 'endpointsMean', 'expandUs'))
            per[(t, name)] = (1000 * scu / sc, 1000 * enu / en)
            print('| %.2f | %s | %.0f | %.0f | %.0f | %.1f | %.0f | %.0f | %.1f | %.0f | %.0f |' % (
                t, name, at(a, t, 'ef'), sc, scu, 1000 * scu / sc, en, enu, 1000 * enu / en,
                at(a, t, 'centroidUs'), at(a, t, 'totalUs')))
    print()
    for t in (0.95, 0.99):
        s = [per[(t, 'v1.5, sin kernel')][i] - per[(t, 'esta noche')][i] for i in (0, 1)]
        print('- **A %.2f:** ahorro de %.1f ns por vector escaneado (microbenchmark: %.1f) y %.1f ns por endpoint (microbenchmark: %.1f).' % (
            t, s[0], SAVE_SCANNED, s[1], SAVE_ENDPOINT))

    print('\n### Contraste 2: la ganancia de cada brazo y la aceleración a 8 threads\n')
    print('Posting-edge contra el ancla de v1.5 (el mismo build, la noche anterior) y contra el de v1.4 (el «hoy» de la estimación). HNSW contra los dos builds de v1.4. Primera ronda; entre paréntesis, la segunda.\n')
    print('| recall | threads | PE / ancla v1.5 | PE / v1.4 | estimado | HNSW / tarde | HNSW / mañana | estimado (tarde / mañana) |')
    print('|---|---|---|---|---|---|---|---|')
    for t in (0.95, 0.99):
        for th in (1, 8):
            p1, p2 = at(curve(pe, th), t), at(curve(pe2, th), t)
            h1, h2 = at(curve(hnsw, th), t), at(curve(hnsw2, th), t)
            a, f = at(curve(anchor, th), t), at(curve(full14, th), t)
            ht, hm = at(curve(tarde, th), t), at(curve(manana, th), t)
            print('| %.2f | %d | %.3fx (%.3fx) | %.3fx (%.3fx) | %.2fx | %.3fx (%.3fx) | %.3fx (%.3fx) | %.2fx / %.2fx |' % (
                t, th, p1 / a, p2 / a, p1 / f, p2 / f, EST_GAIN_PE[t], h1 / ht, h2 / ht, h1 / hm, h2 / hm,
                EST_GAIN_HNSW[t][0], EST_GAIN_HNSW[t][1]))
    print('\nAceleración de 1 a 8 threads, QPS con 8 / QPS con 1 a igual recall, primera ronda (segunda):\n')
    print('| recall | HNSW esta noche | HNSW tarde | HNSW mañana | PE esta noche | PE ancla v1.5 | PE v1.4 |')
    print('|---|---|---|---|---|---|---|')
    for t in (0.95, 0.99):
        def sp(doc):
            return at(curve(doc, 8), t) / at(curve(doc, 1), t)
        print('| %.2f | %.2fx (%.2fx) | %.2fx | %.2fx | %.2fx (%.2fx) | %.2fx | %.2fx |' % (
            t, sp(hnsw), sp(hnsw2), sp(tarde), sp(manana), sp(pe), sp(pe2), sp(anchor), sp(full14)))

    print('\n### Contrastes 3 y 4: las distancias de HNSW y su build\n')
    print('ef al que cada build llega a cada recall: el primero de la malla y el interpolado (lineal en recall). '
          'Distancias por query a ese ef, de las cuentas de TestSearchWork a 10M (un tercer build). '
          'QPS sin el kernel, deducido: 1 / (1/QPS + distancias × %.1f ns), 1 thread.\n' % SAVE_ENDPOINT)
    print('| recall | build | ef (malla) | ef interpolado | distancias | QPS, 1 thread | QPS sin el kernel |')
    print('|---|---|---|---|---|---|---|')
    for t in (0.95, 0.99):
        rows = [('esta noche, 1ª ronda', hnsw, True), ('esta noche, 2ª ronda', hnsw2, True),
                ('v1.4 tarde', tarde, False), ('v1.4 mañana', manana, False)]
        for name, doc, kernel in rows:
            a = curve(doc, 1)
            ef, q = at(a, t, 'ef'), at(a, t)
            dist = distances(ef)
            nok = 1 / (1 / q + dist * SAVE_ENDPOINT * 1e-9) if kernel else q
            print('| %.2f | %s | %s | %.1f | %.0f | %.0f | %.0f%s |' % (
                t, name, first_ef(a, t), ef, dist, q, nok, '' if kernel else ' (medido)'))
    print('\nA igual ef, 1 thread: recall y QPS de cada build, primera ronda.\n')
    print('| ef | recall esta noche / tarde / mañana | QPS esta noche (2ª) / tarde / mañana | esta noche / tarde, 1ª (2ª) |')
    print('|---|---|---|---|')
    c1, c2, ct, cm = ({p['ef']: p for p in curve(x, 1)} for x in (hnsw, hnsw2, tarde, manana))
    for ef in sorted(c1):
        if ef in ct and ef in cm:
            print('| %d | %.4f / %.4f / %.4f | %.0f (%.0f) / %.0f / %.0f | %.3fx (%.3fx) |' % (
                ef, c1[ef]['recall'], ct[ef]['recall'], cm[ef]['recall'], c1[ef]['qps'], c2[ef]['qps'],
                ct[ef]['qps'], cm[ef]['qps'], c1[ef]['qps'] / ct[ef]['qps'], c2[ef]['qps'] / ct[ef]['qps']))

    print('\n### El ef de centroides, sin cronometrar\n')
    print('| ef de centroides | recall a ef=20 | a ef=160 | pérdida frente a 64 | cargas (20 / 160) | endpoints (20 / 160) | entrada = centroide más cercano | centroides, µs (20 / 160) | total, µs (20 / 160) |')
    print('|---|---|---|---|---|---|---|---|---|')
    sweep = {e: load(v16, 'wikidpr-10m-pe-centroidef%d.json' % e) for e in CENTROID_EFS}
    base = {p['ef']: p for p in sweep[64]['points'] if p['qps'] == 0}
    last = None
    for e in CENTROID_EFS:
        if sweep[e] is None:
            continue
        pts = {p['ef']: p for p in sweep[e]['points'] if p['qps'] == 0}
        loss = max(base[ef]['recall'] - pts[ef]['recall'] for ef in (20, 160))
        if loss <= KEEPS:
            last = e
        print('| %d | %.4f | %.4f | %.4f | %.1f / %.1f | %.0f / %.0f | %.3f | %.0f / %.0f | %.0f / %.0f |' % (
            e, pts[20]['recall'], pts[160]['recall'], loss, pts[20]['stats']['loadsMean'], pts[160]['stats']['loadsMean'],
            pts[20]['stats']['endpointsMean'], pts[160]['stats']['endpointsMean'],
            sweep[e]['diagnostics'].get('entryIsNearestCentroid', float('nan')),
            pts[20]['stats']['centroidUs'], pts[160]['stats']['centroidUs'],
            pts[20]['stats']['totalUs'], pts[160]['stats']['totalUs']))

    timed, timed2 = (load(v16, 'wikidpr-10m-pe-centroidef%d%s.json' % (last, s)) for s in ('', '-repeat'))
    if timed is None or not curve(timed, 1):
        print('\nNingún valor cronometrado.')
        return
    print('\n### El ef de centroides %d, cronometrado, contra la curva principal (ef 64) del mismo proceso\n' % last)
    print('| recall | threads | ef 64, 1ª / 2ª | ef %d, 1ª / 2ª | ganancia, 1ª ronda | 2ª ronda | HNSW / PE con ef %d, 1ª ronda (informativo) |' % (last, last))
    print('|---|---|---|---|---|---|---|')
    for t in (0.90, 0.95, 0.99):
        for th in (1, 8):
            m1, m2 = at(curve(pe, th), t), at(curve(pe2, th), t)
            c1_, c2_ = at(curve(timed, th), t), at(curve(timed2, th), t)
            print('| %.2f | %d | %.0f / %.0f | %.0f / %.0f | %+.1f %% | %+.1f %% | %.2fx |' % (
                t, th, m1, m2, c1_, c2_, pct(c1_, m1), pct(c2_, m2), at(curve(hnsw, th), t) / c1_))
    print('\n| threads | deriva del ef %d, 2ª ronda contra la 1ª: mediana | mínimo | máximo |' % last)
    print('|---|---|---|---|')
    for th in (1, 8):
        med, lo, hi = drift(timed, timed2, th)
        print('| %d | %+.1f %% | %+.1f %% | %+.1f %% |' % (th, med, lo, hi))
    print('\nTrabajo a igual recall, 1 thread, del pase instrumentado:\n')
    print('| recall | ef de centroides | ef | cargas | rondas | endpoints | escaneados | centroides, µs | total, µs |')
    print('|---|---|---|---|---|---|---|---|---|')
    for t in (0.95, 0.99):
        for name, doc in (('64', pe), (str(last), timed)):
            a = curve(doc, 1)
            print('| %.2f | %s | %.1f | %.1f | %.1f | %.0f | %.0f | %.0f | %.0f |' % (
                t, name, at(a, t, 'ef'), at(a, t, 'loadsMean'), at(a, t, 'roundsMean'), at(a, t, 'endpointsMean'),
                at(a, t, 'scannedMean'), at(a, t, 'centroidUs'), at(a, t, 'totalUs')))


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else '.')
