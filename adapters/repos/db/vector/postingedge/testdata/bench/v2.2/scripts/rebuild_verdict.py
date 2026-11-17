#!/usr/bin/env python3
"""The verdict of the diagnostic of v2.2, the edges or the partition
(bench-report.md, "Diagnóstico: aristas o partición"), from the file of
run_v22_rebuild.sh.

usage: rebuild_verdict.py <dir of the run> <v2.0 dir> <v2.2 dir> [insert ef] > rebuild-verdict.md

The cell of insert ef 80 of v2.2 ran once more, and at 100 % of the corpus the
edges of every vector were rebuilt in batch over the partition as it was. The
user's reading, on the loads at recall 0.99 after the rebuild against the
batch reference of v2.0: at +10 % or less the deficit was in the edges; where
they were, within the noise of the +17.9 % of the first run, it is in the
partition; in between, the part the rebuild recovers is reported. The noise
floor is that of v2.0, 3.0 % of the loads at 0.99. The part recovered is
(before - after) / (before - batch), with before the 100 % checkpoint of the
same run, which has the same partition.
"""
import json
import math
import os
import sys

MAX_LOADS_99, NOISE_99, NOISE_95, NOISE_RECALL = 0.10, 0.030, 0.005, 0.0009


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


def rel(x, base):
    return None if x is None or not base else x / base - 1


def recovered(before, after, batch):
    """The share of the gap to the batch that the rebuild closes."""
    if None in (before, after, batch) or before == batch:
        return None
    return (before - after) / (before - batch)


def share(x):
    return '—' if x is None else '%.0f %%' % (100 * x)


def recall_of(c):
    return c['captured'] / max(c['wanted'], 1), c['postingsCaptured'] / max(c['postingsWanted'], 1)


def main(d, v20, v22, ef):
    ref = load(os.path.join(v20, 'dbpedia-1m-stream-principal-reference.json'))
    first = load(os.path.join(v22, 'dbpedia-1m-grow10-h1-ef%s.json' % ef))
    work = load(os.path.join(v22, 'dbpedia-1m-batch-buildwork.json'))
    run = load(os.path.join(d, 'dbpedia-1m-grow10-h1-ef%s-run2.json' % ef))
    if run is None:
        print('no run')
        return
    batch = curve(ref['points'])
    cps = {c['percent']: c for c in run['checkpoints']}
    states = [('lote del 100 %', batch, None)]
    if first is not None and first['checkpoints'][-1]['percent'] == 100:
        states.append(('primera corrida, al 100 %', curve(first['checkpoints'][-1]['points']), first['checkpoints'][-1]))
    before = cps.get(100)
    rebuilt = run.get('rebuilt') or {}
    if before is not None:
        states.append(('esta corrida, al 100 %: antes', curve(before['points']), before))
    for key, name in (('edges', 'aristas reconstruidas'), ('graph', 'aristas y grafo de centroides reconstruidos')):
        if rebuilt.get(key):
            states.append((name, curve(rebuilt[key]['points']), rebuilt[key]))
    by_name = {name: (pts, cp) for name, pts, cp in states}
    names = [name for name, _, _ in states]

    print('### Estado')
    print()
    if run.get('stopped'):
        print('**La corrida se cortó:** %s.' % run['stopped'])
        print()
    print('| estado | postings | tamaño medio (p50 / p95 / máx) | cobertura | recall a ef 10 | cargas a 0.95 | '
          'frente al lote | cargas a 0.99 | frente al lote | rondas a 0.99 | endpoints a 0.99 |')
    print('|---|---|---|---|---|---|---|---|---|---|---|')
    l95, l99 = at(batch, 0.95, 'loadsMean'), at(batch, 0.99, 'loadsMean')
    for name, pts, cp in states:
        a95, a99 = at(pts, 0.95, 'loadsMean'), at(pts, 0.99, 'loadsMean')
        size = '—'
        postings, coverage = '—', '—'
        if cp is not None:
            size = '%.1f (%d / %d / %d)' % (cp['sizeMean'], cp['sizeP50'], cp['sizeP95'], cp['sizeMax'])
            postings, coverage = str(cp['postings']), cp['coverage']
        elif work is not None:
            size = '%.1f (%d / %d / %d)' % (work['sizeMean'], work['sizeP50'], work['sizeP95'], work['sizeMax'])
            postings = str(work['postings'])
        print('| %s | %s | %s | %s | %.4f | %s | %s | %s | %s | %s | %s |' % (
            name, postings, size, coverage, pts[0]['recall'], fmt(a95, '%.2f'), pct(rel(a95, l95)),
            fmt(a99, '%.2f'), pct(rel(a99, l99)), fmt(at(pts, 0.99, 'roundsMean'), '%.2f'),
            fmt(at(pts, 0.99, 'endpointsMean'), '%.0f')))
    print()

    print('### La regla')
    print()
    b_name, e_name, g_name = 'esta corrida, al 100 %: antes', 'aristas reconstruidas', 'aristas y grafo de centroides reconstruidos'
    if b_name not in by_name or e_name not in by_name:
        print('La corrida no llegó a reconstruir las aristas: no hay veredicto.')
        print()
    else:
        b99 = at(by_name[b_name][0], 0.99, 'loadsMean')
        e99 = at(by_name[e_name][0], 0.99, 'loadsMean')
        limit = l99 * (1 + MAX_LOADS_99)
        f99 = at(by_name['primera corrida, al 100 %'][0], 0.99, 'loadsMean') if 'primera corrida, al 100 %' in by_name else None
        print('- **El lote carga %.2f postings a 0.99.** +10 %% son %.2f cargas.' % (l99, limit))
        lo = hi = None
        if f99 is not None:
            lo, hi = f99 * (1 - NOISE_99), f99 * (1 + NOISE_99)
            print('- **La primera corrida cargaba %.2f (%s).** Su banda de ruido, de ±3.0 %%, va de %.2f a %.2f cargas.'
                  % (f99, pct(rel(f99, l99)), lo, hi))
            gap = b99 / f99 - 1
            print('- **Esta corrida, antes de reconstruir, carga %.2f (%s frente al lote):** %s frente a la primera, %s.'
                  % (b99, pct(rel(b99, l99)), pct(gap),
                     'dentro del suelo de ruido' if abs(gap) <= NOISE_99 else 'más que el suelo de ruido de v2.0'))
        print('- **Con las aristas reconstruidas carga %.2f (%s frente al lote).**' % (e99, pct(rel(e99, l99))))
        if e99 <= limit:
            reading = 'a +10 % o menos del lote: **el déficit es de las aristas**'
        elif lo is not None and e99 >= lo:
            reading = 'donde estaba, dentro del ruido del de la primera corrida%s: **es la partición**' % (
                '' if e99 <= hi else ' (por encima de su banda)')
        elif lo is None:
            reading = 'por encima de +10 %, y sin primera corrida con la que fijar la banda'
        else:
            reading = 'entre medias: **se reporta qué parte recupera**'
        print('- **Lectura por la regla:** %s.' % reading)
        print('- **Parte recuperada de las cargas a 0.99:** %s, de %.2f a %.2f con el lote en %.2f.'
              % (share(recovered(b99, e99, l99)), b99, e99, l99))
        if g_name in by_name:
            g99 = at(by_name[g_name][0], 0.99, 'loadsMean')
            print('- **Con el grafo de centroides reconstruido además, informativo:** %.2f (%s frente al lote); '
                  'recuperado desde antes, %s.' % (g99, pct(rel(g99, l99)), share(recovered(b99, g99, l99))))
        print()

    print('### Recall a igual ef de búsqueda, diferencia frente al lote')
    print()
    others = names[1:]
    print('| ef de búsqueda | lote | ' + ' | '.join(others) + ' |')
    print('|---|---|' + '---|' * len(others))
    worst = {name: None for name in others}
    for i, p in enumerate(batch):
        row = ['%d' % p['ef'], '%.4f' % p['recall']]
        for name in others:
            pts = by_name[name][0]
            diff = pts[i]['recall'] - p['recall']
            worst[name] = diff if worst[name] is None else min(worst[name], diff)
            row.append('%+.4f' % diff)
        print('| ' + ' | '.join(row) + ' |')
    print('| peor | | ' + ' | '.join('%+.4f' % worst[name] for name in others) + ' |')
    print()

    print('### Cargas de posting a igual ef de búsqueda, frente al lote')
    print()
    print('| ef de búsqueda | lote | ' + ' | '.join(others) + ' |')
    print('|---|---|' + '---|' * len(others))
    for i, p in enumerate(batch):
        row = ['%d' % p['ef'], '%.2f' % p['stats']['loadsMean']]
        for name in others:
            x = by_name[name][0][i]['stats']['loadsMean']
            row.append('%.2f (%s)' % (x, pct(rel(x, p['stats']['loadsMean']))))
        print('| ' + ' | '.join(row) + ' |')
    print()

    if b_name in by_name:
        print('### Parte recuperada, de antes a después')
        print()
        after = [name for name in (e_name, g_name) if name in by_name]
        print('La parte de la distancia al lote que cierra la reconstrucción: (antes − después) / (antes − lote). '
              'Por encima del 100 % queda mejor que el lote; en negativo, peor que antes.')
        print()
        print('| | lote | antes | ' + ' | '.join('%s | recuperado' % name for name in after) + ' |')
        print('|---|---|---|' + '---|---|' * len(after))
        bpts = by_name[b_name][0]

        def line(label, base, was, values, spec):
            row = [label, spec % base, spec % was]
            for v in values:
                row += [spec % v, share(recovered(was, v, base))]
            print('| ' + ' | '.join(row) + ' |')
        for target in (0.95, 0.99):
            for field, label in (('loadsMean', 'cargas'), ('roundsMean', 'rondas'), ('endpointsMean', 'endpoints')):
                line('%s a %.2f' % (label, target), at(batch, target, field), at(bpts, target, field),
                     [at(by_name[name][0], target, field) for name in after], '%.2f')
        for i, p in enumerate(batch):
            line('recall a ef %d' % p['ef'], p['recall'], bpts[i]['recall'],
                 [by_name[name][0][i]['recall'] for name in after], '%.4f')
        for i, p in enumerate(batch):
            line('cargas a ef %d' % p['ef'], p['stats']['loadsMean'], bpts[i]['stats']['loadsMean'],
                 [by_name[name][0][i]['stats']['loadsMean'] for name in after], '%.2f')
        print()

    print('### Grado por cohorte')
    print()
    with_cohorts = [(name, cp) for name, _, cp in states if cp is not None and cp.get('cohorts') and name != 'primera corrida, al 100 %']
    if with_cohorts:
        cohorts = with_cohorts[0][1]['cohorts']
        n = cohorts[-1]['to']
        heads = ['lote' if c['from'] == 0 else '%d → %d %%' % (round(100 * c['from'] / n), round(100 * c['to'] / n)) for c in cohorts]
        print('| estado | ' + ' | '.join(heads) + ' | todos |')
        print('|---|' + '---|' * (len(heads) + 1))
        for name, cp in with_cohorts:
            row = ['%.2f' % (c['edges'] / max(c['vectors'], 1)) for c in cp['cohorts']]
            row.append('%.2f' % (sum(c['edges'] for c in cp['cohorts']) / max(sum(c['vectors'] for c in cp['cohorts']), 1)))
            print('| %s | %s |' % (name, ' | '.join(row)))
        if work is not None:
            print('| lote del 100 %% | %s | %.2f |' % (' | '.join('' for _ in heads), work['edges'] / work['vectors']))
        print()

    print('### Recall de arista contra el corpus actual')
    print()
    print('De la muestra de cada grupo, a nivel de vector y, entre paréntesis, de posting, con su grado.')
    print()
    print('| estado | insertados: arista (posting) | grado | vectores del lote: arista (posting) | grado |')
    print('|---|---|---|---|---|')
    for name, _, cp in states:
        if cp is None:
            continue
        cells = []
        for group in ('inserted', 'batch'):
            c = cp[group]
            if not c['vectors']:
                cells += ['—', '—']
                continue
            v, p = recall_of(c)
            cells += ['%.4f (%.4f)' % (v, p), '%.2f' % (c['edges'] / c['vectors'])]
        print('| %s | %s |' % (name, ' | '.join(cells)))
    print()

    w = rebuilt.get('work')
    if w:
        print('### La partición al final, y el trabajo de la reconstrucción')
        print()
        print('- **Vectores que no están en el posting de su centroide más cercano:** %d de %d, el %.2f %%. '
              'Fuera de sus %d centroides más cercanos: %d.'
              % (w['notNearest'], w['vectors'], 100 * w['notNearest'] / w['vectors'],
                 run['config']['buildProbe'], w['outsideProbes']))
        print('- **Aristas:** %d, %.2f por vector.' % (w['edges'], w['edges'] / w['vectors']))
        print('- **Trabajo, en millones:** %.1f postings sondeados, %.0f candidatos ordenados por código, %.1f rescoreados '
              'y %.1f distancias exactas de la asignación. En %.0f s, indicativo.'
              % (w['postingsProbed'] / 1e6, w['candidatesScored'] / 1e6, w['candidatesRescored'] / 1e6,
                 w['assignDistances'] / 1e6, rebuilt.get('seconds', 0)))
        if work is not None:
            print('- **El lote del 100 %%, en millones:** %.1f postings sondeados, %.0f candidatos por código, %.1f rescoreados '
                  'y %.1f distancias de la asignación.'
                  % (work['postingsProbed'] / 1e6, work['candidatesScored'] / 1e6, work['candidatesRescored'] / 1e6,
                     work['assignDistances'] / 1e6))
        print()

    if before is not None and first is not None:
        print('### La celda repetida frente a la primera corrida, al 100 %')
        print()
        f = first['checkpoints'][-1]
        fp, bp = curve(f['points']), curve(before['points'])
        print('| | primera corrida | esta corrida | diferencia | suelo de ruido de v2.0 |')
        print('|---|---|---|---|---|')
        for i, p in enumerate(fp):
            diff = bp[i]['recall'] - p['recall']
            print('| recall a ef %d | %.4f | %.4f | %+.4f%s | %.4f |' % (
                p['ef'], p['recall'], bp[i]['recall'], diff, ', dentro' if abs(diff) <= NOISE_RECALL else '', NOISE_RECALL))
        for target, noise in ((0.95, NOISE_95), (0.99, NOISE_99)):
            a, b = at(fp, target, 'loadsMean'), at(bp, target, 'loadsMean')
            gap = rel(b, a)
            print('| cargas a %.2f | %.2f | %.2f | %s%s | %.1f %% |' % (
                target, a, b, pct(gap), ', dentro' if gap is not None and abs(gap) <= noise else '', 100 * noise))
        for label, get in (
            ('postings', lambda c: c['postings']),
            ('tamaño medio', lambda c: c['sizeMean']),
            ('splits', lambda c: c['stats']['splits']),
            ('no-nearest, acumulado', lambda c: c['stats']['nonNearest'] / max(c['stats']['inserted'], 1)),
            ('cargas por insert', lambda c: c['stats']['searchLoads'] / max(c['stats']['inserted'], 1)),
            ('escrituras inversas por insert', lambda c: c['stats']['inverseEdges'] / max(c['stats']['inserted'], 1)),
            ('grado, todo el índice', lambda c: sum(x['edges'] for x in c['cohorts']) / max(sum(x['vectors'] for x in c['cohorts']), 1)),
        ):
            a, b = get(f), get(before)
            print('| %s | %.4g | %.4g | %s | |' % (label, a, b, pct(rel(b, a))))
        print()


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] if len(sys.argv) > 4 else '80')
