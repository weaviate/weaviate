#!/usr/bin/env python3
"""The verdict of v2.3, the edge refresh by the insert's own search
(bench-report.md, "v2.3: refresco de aristas con la búsqueda del insert"),
from the file of run_v23_refresh.sh.

usage: refresh_verdict.py <dir of the run> <v2.0 dir> [insert ef] > refresh-verdict.md

One growth cell from 10 % of the corpus with insert ef 80 and, at 100 %, the
edges of the vectors refreshed with the search of an insert, the oldest
first, in three cumulative stages, each measured within the same run. The
user's criterion: the refresh works if, with everything refreshed, the loads
at recall 0.99 are at +10 % or less of the batch reference of v2.0. The part
of the recovery of a stage is what the loads go down in it over what they go
down from before the refresh to after the last stage. The user's noise rule:
nothing at 0.99 is compared between different runs, so every state here is
of this run, and the batch is the one reference outside it.
"""
import json
import math
import os
import sys

MAX_LOADS_99 = 0.10


def load(path):
    return json.load(open(path)) if os.path.exists(path) else None


def curve(points):
    return sorted((p for p in points if p['threads'] == 1), key=lambda p: p['ef'])


def value(p, field):
    return p[field] if field in p else p['stats'][field]


def crossing(pts, target):
    """The two points the curve crosses the recall target between."""
    prev = None
    for p in pts:
        if p['recall'] >= target:
            return prev, p
        prev = p
    return prev, None


def at(pts, target, field):
    """The field at the recall target, interpolated between the two points around it."""
    prev, p = crossing(pts, target)
    if p is None:
        return None
    if prev is None:
        return value(p, field)
    f = (target - prev['recall']) / (p['recall'] - prev['recall'])
    a, b = value(prev, field), value(p, field)
    if a <= 0 or b <= 0:
        return a + f * (b - a)
    return math.exp(math.log(a) + f * (math.log(b) - math.log(a)))


def fmt(x, spec='%.2f'):
    return '—' if x is None else spec % x


def pct(x):
    return '—' if x is None else '%+.1f %%' % (100 * x)


def rel(x, base):
    return None if x is None or not base else x / base - 1


def share(x):
    return '—' if x is None else '%.0f %%' % (100 * x)


def ratio(a, b):
    return None if a is None or b is None or b == 0 else a / b


def recall_of(c):
    return c['captured'] / max(c['wanted'], 1), c['postingsCaptured'] / max(c['postingsWanted'], 1)


def hist_p(hist, q):
    total = sum(hist)
    seen = 0
    for v, count in enumerate(hist):
        seen += count
        if total and seen >= q * total:
            return v
    return 0


def hist_mean(hist):
    total = sum(hist)
    return sum(v * count for v, count in enumerate(hist)) / total if total else 0.0


def degree(cp):
    cohorts = cp.get('cohorts') or []
    vectors = sum(c['vectors'] for c in cohorts)
    return sum(c['edges'] for c in cohorts) / vectors if vectors else None


def main(d, v20, ef):
    ref = load(os.path.join(v20, 'dbpedia-1m-stream-principal-reference.json'))
    run = load(os.path.join(d, 'dbpedia-1m-grow10-h1-ef%s-refresh.json' % ef))
    if run is None:
        print('no run')
        return
    batch = curve(ref['points'])
    cps = {c['percent']: c for c in run['checkpoints']}
    before = cps.get(100)
    stages = [s for s in run.get('refreshed') or [] if s.get('after')]
    rebuilt = (run.get('rebuilt') or {}).get('edges')

    # (name, short name, curve, checkpoint)
    states = [('lote del 100 %', 'lote', batch, None)]
    if before is not None:
        states.append(('antes del refresco', 'antes', curve(before['points']), before))
    for i, s in enumerate(stages):
        states.append(('etapa %d: refrescados los que entraron hasta el %d %%' % (i + 1, s['percent']), 'etapa %d' % (i + 1),
                       curve(s['after']['points']), s['after']))
    if rebuilt:
        states.append(('informativa: aristas reconstruidas en lote', 'reconstruidas en lote', curve(rebuilt['points']), rebuilt))
    l95, l99 = at(batch, 0.95, 'loadsMean'), at(batch, 0.99, 'loadsMean')

    print('### Estado')
    print()
    if run.get('stopped'):
        print('**La corrida se cortó:** %s.' % run['stopped'])
        print()
    print('| estado | cargas a 0.95 | frente al lote | cargas a 0.99 | frente al lote | cruza 0.99 entre | recall en esos ef | '
          'rondas a 0.99 | endpoints a 0.99 | aristas por vector | recall de arista: insertados / lote del 10 % | cobertura | '
          'listas con una arista repetida |')
    print('|---|---|---|---|---|---|---|---|---|---|---|---|---|')
    for name, _, pts, cp in states:
        a95, a99 = at(pts, 0.95, 'loadsMean'), at(pts, 0.99, 'loadsMean')
        prev, p = crossing(pts, 0.99)
        between, recalls = '—', '—'
        if p is not None and prev is not None:
            between, recalls = 'ef %d y %d' % (prev['ef'], p['ef']), '%.4f y %.4f' % (prev['recall'], p['recall'])
        elif p is not None:
            between, recalls = 'por debajo de ef %d' % p['ef'], '%.4f' % p['recall']
        deg, edge, coverage, repeated = '—', '—', '—', '—'
        if cp is not None:
            deg = fmt(degree(cp))
            edge = '%.4f / %.4f' % (recall_of(cp['inserted'])[0], recall_of(cp['batch'])[0])
            coverage, repeated = cp['coverage'], str(cp.get('repeatedEdges', '—'))
        print('| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |' % (
            name, fmt(a95), pct(rel(a95, l95)), fmt(a99), pct(rel(a99, l99)), between, recalls,
            fmt(at(pts, 0.99, 'roundsMean')), fmt(at(pts, 0.99, 'endpointsMean'), '%.0f'), deg, edge, coverage, repeated))
    print()

    print('### El criterio')
    print()
    if before is None or len(stages) == 0:
        print('La corrida no llegó a refrescar: no hay veredicto.')
        print()
        return
    complete = stages[-1]['to'] == run['vectors']
    b99 = at(curve(before['points']), 0.99, 'loadsMean')
    last = curve(stages[-1]['after']['points'])
    f99 = at(last, 0.99, 'loadsMean')
    print('- **El lote carga %.2f postings a 0.99.** +10 %% son %.2f cargas.' % (l99, l99 * (1 + MAX_LOADS_99)))
    print('- **Antes del refresco, esta corrida carga %s (%s frente al lote).**' % (fmt(b99), pct(rel(b99, l99))))
    if not complete:
        print('- **El refresco no llegó a todos los vectores:** la última etapa acaba en %d de %d. No hay veredicto.'
              % (stages[-1]['to'], run['vectors']))
    else:
        verdict = 'no hay medida a 0.99'
        if f99 is not None:
            verdict = '**el refresco sirve**' if f99 <= l99 * (1 + MAX_LOADS_99) else '**el refresco no sirve**'
        print('- **Con todo refrescado carga %s (%s frente al lote):** %s, por el criterio.' % (fmt(f99), pct(rel(f99, l99)), verdict))
        prev, p = crossing(last, 0.99)
        if prev is not None and p is not None:
            print('  - Cruza 0.99 entre ef %d y ef %d, con recall %.4f y %.4f. El lote, entre ef %d y ef %d.' % (
                prev['ef'], p['ef'], prev['recall'], p['recall'], crossing(batch, 0.99)[0]['ef'], crossing(batch, 0.99)[1]['ef']))
    if rebuilt:
        r99 = at(curve(rebuilt['points']), 0.99, 'loadsMean')
        print('- **Informativa, con las aristas reconstruidas en lote después, en la misma corrida:** %s (%s frente al lote).'
              % (fmt(r99), pct(rel(r99, l99))))
    print()

    print('### La recuperación, por etapa')
    print()
    print('La parte de una etapa es lo que baja la medida en ella sobre lo que baja de antes del refresco a después de la '
          'última etapa. La distancia al lote cerrada es (antes − después) / (antes − lote), acumulada.')
    print()
    stage_pts = [curve(s['after']['points']) for s in stages]
    counts = [s['stats']['refreshed'] for s in stages]
    header = '| | antes | ' + ' | '.join('etapa %d' % (i + 1) for i in range(len(stages))) + (' | reconstruidas en lote |' if rebuilt else ' |')
    rule = '|---' * (2 + len(stages) + (1 if rebuilt else 0)) + '|'
    for label, target, field, spec in (('cargas a 0.99', 0.99, 'loadsMean', '%.2f'), ('cargas a 0.95', 0.95, 'loadsMean', '%.2f'),
                                       ('rondas a 0.99', 0.99, 'roundsMean', '%.2f'), ('endpoints a 0.99', 0.99, 'endpointsMean', '%.0f'),
                                       ('endpoints a 0.95', 0.95, 'endpointsMean', '%.0f')):
        b = at(curve(before['points']), target, field)
        lot = at(batch, target, field)
        vals = [at(pts, target, field) for pts in stage_pts]
        extra = at(curve(rebuilt['points']), target, field) if rebuilt else None
        print('**%s** (lote: %s)' % (label[0].upper() + label[1:], fmt(lot, spec)))
        print()
        print(header)
        print(rule)
        tail = (' | %s |' % fmt(extra, spec)) if rebuilt else ' |'
        print('| %s | %s | %s%s' % (label, fmt(b, spec), ' | '.join(fmt(v, spec) for v in vals), tail))
        if None not in vals and b is not None:
            total = b - vals[-1]
            steps = [prev - v for prev, v in zip([b] + vals[:-1], vals)]
            tail = ' | |' if rebuilt else ' |'
            print('| lo que baja en la etapa | | %s%s' % (' | '.join(fmt(s, spec) for s in steps), tail))
            if total > 0:
                print('| parte de la recuperación | | %s%s' % (' | '.join(share(s / total) for s in steps), tail))
            else:
                print('| parte de la recuperación | | %s%s' % (' | '.join('—' for _ in steps), tail))
            print('| por 100 000 vectores refrescados | | %s%s' % (
                ' | '.join(fmt(s * 100000 / c, '%.3f' if spec == '%.2f' else '%.1f') for s, c in zip(steps, counts)), tail))
            closed = [ratio(b - v, b - lot) for v in vals]
            tail = (' | %s |' % share(ratio(b - extra, b - lot))) if rebuilt and extra is not None else tail
            print('| distancia al lote cerrada, acumulada | | %s%s' % (' | '.join(share(c) for c in closed), tail))
        print()

    names = [short for _, short, _, _ in states[1:]]
    print('### Recall a igual ef de búsqueda, diferencia frente al lote')
    print()
    print('| ef de búsqueda | lote | ' + ' | '.join(names) + ' |')
    print('|---|---' + '|---' * len(names) + '|')
    by_ef = {short: {p['ef']: p for p in pts} for _, short, pts, _ in states}
    worst = {short: None for short in names}
    for p in batch:
        cells = []
        for short in names:
            q = by_ef[short].get(p['ef'])
            if q is None:
                cells.append('—')
                continue
            diff = q['recall'] - p['recall']
            worst[short] = diff if worst[short] is None else min(worst[short], diff)
            cells.append('%+.4f' % diff)
        print('| %d | %.4f | %s |' % (p['ef'], p['recall'], ' | '.join(cells)))
    print('| peor | | %s |' % ' | '.join('—' if worst[s] is None else '%+.4f' % worst[s] for s in names))
    print()

    print('### Recall a igual ef: la parte de cada etapa')
    print()
    print('| ef de búsqueda | antes | con todo refrescado | lo que sube | ' + ' | '.join('etapa %d' % (i + 1) for i in range(len(stages))) + ' |')
    print('|---|---|---|---' + '|---' * len(stages) + '|')
    for p in batch:
        b = by_ef['antes'].get(p['ef'])
        vals = [by_ef['etapa %d' % (i + 1)].get(p['ef']) for i in range(len(stages))]
        if b is None or None in vals:
            continue
        recalls = [b['recall']] + [v['recall'] for v in vals]
        total = recalls[-1] - recalls[0]
        steps = [y - x for x, y in zip(recalls, recalls[1:])]
        parts = ' | '.join(share(s / total) if total > 0 else '%+.4f' % s for s in steps)
        print('| %d | %.4f | %.4f | %+.4f | %s |' % (p['ef'], recalls[0], recalls[-1], total, parts))
    print()

    print('### Cargas de posting a igual ef de búsqueda, frente al lote')
    print()
    print('| ef de búsqueda | lote | ' + ' | '.join(names) + ' |')
    print('|---|---' + '|---' * len(names) + '|')
    for p in batch:
        base = value(p, 'loadsMean')
        cells = []
        for short in names:
            q = by_ef[short].get(p['ef'])
            cells.append('—' if q is None else '%.2f (%s)' % (value(q, 'loadsMean'), pct(rel(value(q, 'loadsMean'), base))))
        print('| %d | %.2f | %s |' % (p['ef'], base, ' | '.join(cells)))
    print()

    print('### Grado por cohorte')
    print()
    cohorts = before.get('cohorts') or []
    n = run['vectors']
    print('| estado | ' + ' | '.join('%d → %d %%' % (round(100 * c['from'] / n), round(100 * c['to'] / n)) for c in cohorts) + ' | todos |')
    print('|---' + '|---' * len(cohorts) + '|---|')
    for name, _, _, cp in states[1:]:
        if cp is None or not cp.get('cohorts'):
            continue
        print('| %s | %s | %s |' % (name, ' | '.join('%.2f' % (c['edges'] / max(c['vectors'], 1)) for c in cp['cohorts']), fmt(degree(cp))))
    print()

    print('### Recall de arista contra el corpus actual')
    print()
    print('De la muestra de cada grupo, a nivel de vector y, entre paréntesis, de posting, con su grado.')
    print()
    print('| estado | insertados: arista (posting) | grado | vectores del lote del 10 %: arista (posting) | grado |')
    print('|---|---|---|---|---|')
    for name, _, _, cp in states[1:]:
        if cp is None:
            continue
        cells = []
        for key in ('inserted', 'batch'):
            c = cp[key]
            cells += ['%.4f (%.4f)' % recall_of(c), '%.2f' % (c['edges'] / max(c['vectors'], 1))]
        print('| %s | %s |' % (name, ' | '.join(cells)))
    print()

    print('### El coste del refresco, por etapa')
    print()
    print('Por vector refrescado, salvo donde dice otra cosa.')
    print()
    cols = ['etapa %d' % (i + 1) for i in range(len(stages))] + ['las tres']
    print('| | ' + ' | '.join(cols) + ' |')
    print('|---' + '|---' * len(cols) + '|')

    def summed(key, sub=None):
        vals = [(s['stats'][sub][key] if sub else s['stats'][key]) for s in stages]
        return vals + [sum(vals)]

    refreshed = summed('refreshed')

    def row(label, vals, spec='%.2f', per=True):
        cells = [spec % (v / r if per else v) for v, r in zip(vals, refreshed)]
        print('| %s | %s |' % (label, ' | '.join(cells)))

    row('vectores refrescados', refreshed, '%d', per=False)
    row('cargas de posting', summed('searchLoads', 'work'), '%.1f')
    row('rondas', summed('searchRounds', 'work'), '%.1f')
    scanned, endpoints = summed('searchScanned', 'work'), summed('searchEndpoints', 'work')
    row('distancias por código: scan', scanned, '%.0f')
    row('distancias por código: endpoints', endpoints, '%.0f')
    row('distancias por código, todas', [a + b for a, b in zip(scanned, endpoints)], '%.0f')
    row('exactas: rescore de la búsqueda', summed('searchRescored', 'work'), '%.1f')
    row('exactas: comprobar las aristas inversas', summed('inverseDistances', 'work'), '%.1f')
    row('listas de vecinos leídas para las inversas', summed('inverseChecks', 'work'))
    row('vecinos que ya tenían al vector', [s['stats']['work'].get('inverseAlready', 0) for s in stages]
        + [sum(s['stats']['work'].get('inverseAlready', 0) for s in stages)])
    row('aristas inversas escritas', summed('inverseEdges', 'work'))
    changed = summed('listsChanged')
    print('| listas propias que cambian | %s |' % ' | '.join('%.1f %%' % (100 * c / r) for c, r in zip(changed, refreshed)))
    row('aristas propias: antes', summed('edgesBefore'))
    row('aristas propias: después', summed('edgesAfter'))
    row('aristas propias: conservadas', summed('edgesKept'))
    hists = [s['stats']['work'].get('inversePostings') or [] for s in stages]
    total_hist = [sum(h[i] for h in hists if i < len(h)) for i in range(max((len(h) for h in hists), default=0))]
    print('| postings distintos con una lista de vecino escrita: media (p50 / p95) | %s |' % ' | '.join(
        '%.2f (%d / %d)' % (hist_mean(h), hist_p(h, 0.5), hist_p(h, 0.95)) for h in hists + [total_hist]))
    missing = summed('selfMissing')
    row('vectores que su búsqueda no encontró', missing, '%d', per=False)
    seconds = [s['stats']['ns'] / 1e9 for s in stages]
    print('| segundos, indicativos | %s |' % ' | '.join('%.0f' % v for v in seconds + [sum(seconds)]))
    print()

    print('### Un refresco junto a un insert de esta corrida y a la reconstrucción en lote')
    print()
    st = before['stats']
    inserted = st['inserted']
    cp80 = cps.get(80)
    print('| por vector | un refresco, las tres etapas | un insert, toda la corrida | un insert, del 80 al 100 % | la regla del lote |')
    print('|---|---|---|---|---|')
    work = (run.get('rebuilt') or {}).get('work')

    def ins(key):
        return st[key] / inserted

    def tail_ins(key):
        if cp80 is None:
            return None
        return (st[key] - cp80['stats'][key]) / (inserted - cp80['stats']['inserted'])

    def ref(key):
        return sum(s['stats']['work'][key] for s in stages) / refreshed[-1]

    rows = (
        ('cargas de posting', ref('searchLoads'), ins('searchLoads'), tail_ins('searchLoads'), None, '%.1f'),
        ('distancias por código', ref('searchScanned') + ref('searchEndpoints'), ins('searchScanned') + ins('searchEndpoints'),
         None if cp80 is None else tail_ins('searchScanned') + tail_ins('searchEndpoints'),
         None if work is None else work['candidatesScored'] / work['vectors'], '%.0f'),
        ('exactas: grafo de centroides o asignación', 0.0, ins('assignDistances'), tail_ins('assignDistances'),
         None if work is None else work['assignDistances'] / work['vectors'], '%.1f'),
        ('exactas: rescore', ref('searchRescored'), ins('searchRescored'), tail_ins('searchRescored'),
         None if work is None else work['candidatesRescored'] / work['vectors'], '%.1f'),
        ('exactas: comprobar las aristas inversas', ref('inverseDistances'), ins('inverseDistances'), tail_ins('inverseDistances'), None, '%.1f'),
        ('aristas inversas escritas', ref('inverseEdges'), ins('inverseEdges'), tail_ins('inverseEdges'), None, '%.2f'),
    )
    for label, a, b, c, e, spec in rows:
        print('| %s | %s | %s | %s | %s |' % (label, fmt(a, spec), fmt(b, spec), fmt(c, spec), fmt(e, spec)))
    print()
    code_refresh = sum(scanned[:-1]) + sum(endpoints[:-1])
    code_inserts = st['searchScanned'] + st['searchEndpoints']
    print('- **El refresco entero, en millones:** %.0f cargas de posting, %.0f distancias por código y %.1f exactas.' % (
        sum(s['stats']['work']['searchLoads'] for s in stages) / 1e6, code_refresh / 1e6,
        sum(s['stats']['work']['searchRescored'] + s['stats']['work']['inverseDistances'] for s in stages) / 1e6))
    print('- **Los %d inserts de la corrida, en millones:** %.0f cargas de posting y %.0f distancias por código en sus búsquedas.' % (
        inserted, st['searchLoads'] / 1e6, code_inserts / 1e6))
    if work is not None:
        print('- **La reconstrucción en lote, en millones:** %.0f distancias por código y %.0f exactas de la asignación. '
              'El refresco hace %.2f veces sus distancias por código.' % (
                  work['candidatesScored'] / 1e6, work['assignDistances'] / 1e6, code_refresh / work['candidatesScored']))
    print()

    print('### La regla de ruido')
    print()
    print('- **Ninguna comparación a 0.99 entre corridas distintas.** Todos los estados de arriba son de esta corrida, '
          'y el lote es la única referencia de fuera, porque el criterio está escrito contra él.')
    print('- **El «antes» de esta corrida no se compara a 0.99** con las celdas de v2.2 ni con la corrida del diagnóstico.')


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else '80')
