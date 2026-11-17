#!/usr/bin/env python3
"""The verdict of v2.4, the edge refresh on doubling during the growth
(bench-report.md, "v2.4: refresco al doblar, en línea"), from the file of
run_v24_doubling.sh.

usage: doubling_verdict.py <dir of the run> <v2.0 dir> <v2.3 dir> [insert ef] > doubling-verdict.md

One growth cell from 10 % of the corpus with insert ef 80. When the index
reaches each size of a round, every vector whose edges were last built with
half that many vectors or fewer has them refreshed, with a measurement before
and one after. The user's criterion: at 100 %, the loads at recall 0.99 are at
+10 % or less of the batch reference of v2.0. The edges rebuilt in batch at
the end are the reference inside the run. The user's noise rule: nothing at
0.99 is compared between different runs, so every state here is of this run,
the batch is the one reference outside it, and v2.3 is read for its counts.

Informative, after all of that: the three hypotheses about why the edges of
the batch rule search better than those of a refresh, each read as the
pre-registration says.
"""
import json
import math
import os
import sys

MAX_LOADS_99 = 0.10
# The readings of the informative part, as pre-registered.
MIN_GAP = 0.0005
DECISIVE_EFS = (160, 320)
H3_REFRESH_SHARE = 0.30
H3_BATCH_SHARE = 0.25
H3_REFUTED_SHARE = 0.25
H3_REFUTED_GAP = 0.03


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


def share1(x):
    return '—' if x is None else '%.1f %%' % (100 * x)


def ratio(a, b):
    return None if a is None or b is None or b == 0 else a / b


def more(x, base):
    """How much more x is than base, in words."""
    r = rel(x, base)
    if r is None:
        return '—'
    return 'un %.1f %% %s' % (100 * abs(r), 'más' if r >= 0 else 'menos')


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


def between(pts):
    prev, p = crossing(pts, 0.99)
    if p is not None and prev is not None:
        return 'ef %d y %d' % (prev['ef'], p['ef']), '%.4f y %.4f' % (prev['recall'], p['recall'])
    if p is not None:
        return 'por debajo de ef %d' % p['ef'], '%.4f' % p['recall']
    return 'no llega', '—'


def refresh_cost(stats):
    """The counts of some refreshes, as a dict of totals."""
    w = stats['work']
    return {
        'refreshed': stats['refreshed'], 'loads': w['searchLoads'], 'rounds': w['searchRounds'],
        'scan': w['searchScanned'], 'endpoints': w['searchEndpoints'], 'code': w['searchScanned'] + w['searchEndpoints'],
        'rescored': w['searchRescored'], 'inverseDistances': w['inverseDistances'], 'inverseChecks': w['inverseChecks'],
        'inverseAlready': w.get('inverseAlready', 0), 'inverseEdges': w['inverseEdges'], 'changed': stats['listsChanged'],
        'before': stats['edgesBefore'], 'after': stats['edgesAfter'], 'kept': stats['edgesKept'],
        'selfMissing': stats['selfMissing'], 'seconds': stats['ns'] / 1e9,
        'postings': w.get('inversePostings') or [],
    }


def add_costs(costs):
    total = {}
    for c in costs:
        for k, v in c.items():
            if k == 'postings':
                old = total.get(k, [])
                total[k] = [(old[i] if i < len(old) else 0) + (v[i] if i < len(v) else 0) for i in range(max(len(old), len(v)))]
            else:
                total[k] = total.get(k, 0) + v
    return total


def insert_cost(st):
    exact = sum(st.get(k, 0) for k in ('assignDistances', 'searchRescored', 'inverseDistances', 'splitDistances',
                                       'splitGraphDistances', 'cleanDistances'))
    return {
        'inserted': st['inserted'], 'loads': st['searchLoads'], 'code': st['searchScanned'] + st['searchEndpoints'],
        'exact': exact, 'inverseEdges': st['inverseEdges'],
    }


def main(d, v20, v23, ef):
    ref = load(os.path.join(v20, 'dbpedia-1m-stream-principal-reference.json'))
    run = load(os.path.join(d, 'dbpedia-1m-grow10-h1-ef%s-doubling.json' % ef))
    old = load(os.path.join(v23, 'dbpedia-1m-grow10-h1-ef%s-refresh.json' % ef))
    if run is None:
        print('no run')
        return
    n = run['vectors']
    batch = curve(ref['points']) if ref else []
    cps = {c['percent']: c for c in run['checkpoints']}
    final = cps.get(100)
    rounds = run.get('doubling') or []
    rebuilt = (run.get('rebuilt') or {}).get('edges')
    why = run.get('why') or {}
    l95, l99 = at(batch, 0.95, 'loadsMean'), at(batch, 0.99, 'loadsMean')
    whole = len(batch) > 0 and run['vectors'] == ref.get('vectors', run['vectors'])

    # (name, short name, checkpoint, measured over the whole corpus)
    states = []
    start = cps.get(run['start'])
    if start is not None:
        states.append(('lote del %d %%' % run['start'], 'lote del %d %%' % run['start'], start, False))
    for r in rounds:
        if r.get('before'):
            states.append(('corpus de %d, antes de su tanda' % r['at'], '%d, antes' % r['at'], r['before'], False))
        if r.get('after'):
            states.append(('corpus de %d, después de su tanda' % r['at'], '%d, después' % r['at'], r['after'], False))
    if final is not None:
        states.append(('al 100 %', 'al 100 %', final, True))
    if rebuilt:
        states.append(('referencia interna: aristas reconstruidas en lote', 'reconstruidas en lote', rebuilt, True))
    if why.get('refreshed'):
        states.append(('informativo: todo refrescado', 'todo refrescado', why['refreshed'], True))
    if why.get('own'):
        states.append(('informativo: cada lista como la dejó su refresco', 'sin las inversas', why['own'], True))

    print('### Estado')
    print()
    if run.get('stopped'):
        print('**La corrida se cortó:** %s.' % run['stopped'])
        print()
    print('| estado | vectores | cargas a 0.95 | cargas a 0.99 | cruza 0.99 entre | recall en esos ef | rondas a 0.99 | '
          'endpoints a 0.99 | aristas por vector | recall de arista: insertados / lote del 10 % | cobertura | '
          'listas con una arista repetida |')
    print('|---|---|---|---|---|---|---|---|---|---|---|---|')
    if batch:
        b, r = between(batch)
        print('| lote del 100 %% (v2.0) | %d | %s | %s | %s | %s | %s | %s | — | — | — | — |' % (
            ref.get('vectors', n), fmt(l95), fmt(l99), b, r, fmt(at(batch, 0.99, 'roundsMean')),
            fmt(at(batch, 0.99, 'endpointsMean'), '%.0f')))
    for name, _, cp, full in states:
        pts = curve(cp['points'])
        a95, a99 = at(pts, 0.95, 'loadsMean'), at(pts, 0.99, 'loadsMean')
        b, r = between(pts)
        versus95 = ' (%s)' % pct(rel(a95, l95)) if full and whole else ''
        versus99 = ' (%s)' % pct(rel(a99, l99)) if full and whole else ''
        inserted = '—' if not cp['inserted'].get('vectors') else '%.4f' % recall_of(cp['inserted'])[0]
        print('| %s | %d | %s%s | %s%s | %s | %s | %s | %s | %s | %s / %.4f | %s | %s |' % (
            name, cp['live'], fmt(a95), versus95, fmt(a99), versus99, b, r, fmt(at(pts, 0.99, 'roundsMean')),
            fmt(at(pts, 0.99, 'endpointsMean'), '%.0f'), fmt(degree(cp)), inserted, recall_of(cp['batch'])[0],
            cp['coverage'], cp.get('repeatedEdges', '—')))
    print()
    print('Entre paréntesis, frente al lote del 100 %. Los estados de un corpus menor no tienen lote con el que compararse.')
    print()

    print('### El criterio')
    print()
    if final is None:
        print('La corrida no llegó al 100 %: no hay veredicto.')
        print()
        return
    fpts = curve(final['points'])
    f99, f95 = at(fpts, 0.99, 'loadsMean'), at(fpts, 0.95, 'loadsMean')
    if not whole:
        print('- **No hay lote de este corpus con el que comparar:** la corrida es de %d vectores. Sin veredicto.' % n)
    else:
        print('- **El lote carga %.2f postings a 0.99.** +10 %% son %.2f cargas.' % (l99, l99 * (1 + MAX_LOADS_99)))
        verdict = 'no hay medida a 0.99'
        if f99 is not None:
            verdict = '**cumple**' if f99 <= l99 * (1 + MAX_LOADS_99) else '**no cumple**'
        print('- **Al 100 %%, con las %d tandas y sin refresco final, carga %s (%s frente al lote):** %s el criterio.' % (
            len(rounds), fmt(f99), pct(rel(f99, l99)), verdict))
        prev, p = crossing(fpts, 0.99)
        if prev is not None and p is not None:
            bprev, bp = crossing(batch, 0.99)
            print('  - Cruza 0.99 entre ef %d y ef %d, con recall %.4f y %.4f. El lote, entre ef %d y ef %d.' % (
                prev['ef'], p['ef'], prev['recall'], p['recall'], bprev['ef'], bp['ef']))
            per = (value(p, 'loadsMean') - value(prev, 'loadsMean')) / ((p['recall'] - prev['recall']) * 1000)
            print('  - En ese tramo, 0.001 de recall son %.1f cargas.' % per)
        print('  - A 0.95 carga %s (%s frente al lote).' % (fmt(f95), pct(rel(f95, l95))))
    if rebuilt:
        rpts = curve(rebuilt['points'])
        r99, r95 = at(rpts, 0.99, 'loadsMean'), at(rpts, 0.95, 'loadsMean')
        print('- **Referencia interna, con las aristas reconstruidas en lote sobre la misma partición:** %s a 0.99%s y %s a 0.95.' % (
            fmt(r99), ' (%s frente al lote)' % pct(rel(r99, l99)) if whole else '', fmt(r95)))
        print('  - El 100 %% de la corrida carga %s que su referencia interna a 0.99, y %s a 0.95.' % (more(f99, r99), more(f95, r95)))
    print()

    print('### Cada tanda, de su antes a su después')
    print()
    print('Sobre el mismo índice y contra los vecinos exactos del corpus de ese momento.')
    print()
    print('| tanda | corpus | vectores refrescados | cargas a 0.95: antes → después | cargas a 0.99: antes → después | '
          'cruza 0.99 entre: antes, después | recall a igual ef: lo que sube, del peor al mejor ef | aristas por vector: antes → después |')
    print('|---|---|---|---|---|---|---|---|')
    for i, r in enumerate(rounds):
        if not (r.get('before') and r.get('after')):
            continue
        bp, ap = curve(r['before']['points']), curve(r['after']['points'])
        after_by = {p['ef']: p for p in ap}
        diffs = [after_by[p['ef']]['recall'] - p['recall'] for p in bp if p['ef'] in after_by]
        b95, a95 = at(bp, 0.95, 'loadsMean'), at(ap, 0.95, 'loadsMean')
        b99, a99 = at(bp, 0.99, 'loadsMean'), at(ap, 0.99, 'loadsMean')
        print('| %d | %d | %d | %s → %s (%s) | %s → %s (%s) | %s, %s | de %+.4f a %+.4f | %s → %s |' % (
            i + 1, r['at'], r['stats']['refreshed'], fmt(b95), fmt(a95), pct(rel(a95, b95)), fmt(b99), fmt(a99),
            pct(rel(a99, b99)), between(bp)[0], between(ap)[0], min(diffs), max(diffs), fmt(degree(r['before'])),
            fmt(degree(r['after']))))
    print()
    for i, r in enumerate(rounds):
        if not (r.get('before') and r.get('after')):
            continue
        bp, ap = curve(r['before']['points']), curve(r['after']['points'])
        after_by = {p['ef']: p for p in ap}
        print('**Tanda %d, con %d vectores:** recall y cargas a igual ef.' % (i + 1, r['at']))
        print()
        print('| ef de búsqueda | recall antes | recall después | lo que sube | cargas antes | cargas después |')
        print('|---|---|---|---|---|---|')
        for p in bp:
            q = after_by.get(p['ef'])
            if q is None:
                continue
            print('| %d | %.4f | %.4f | %+.4f | %.2f | %.2f |' % (
                p['ef'], p['recall'], q['recall'], q['recall'] - p['recall'], value(p, 'loadsMean'), value(q, 'loadsMean')))
        print()

    full = [(short, curve(cp['points']), cp) for _, short, cp, whole_corpus in states if whole_corpus]
    names = [short for short, _, _ in full]
    by_ef = {short: {p['ef']: p for p in pts} for short, pts, _ in full}
    if batch and whole:
        print('### Recall a igual ef de búsqueda, diferencia frente al lote')
        print()
        print('| ef de búsqueda | lote | ' + ' | '.join(names) + ' |')
        print('|---|---' + '|---' * len(names) + '|')
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

    if rebuilt:
        print('### Al 100 %, frente a la referencia interna')
        print()
        rpts = curve(rebuilt['points'])
        rby = {p['ef']: p for p in rpts}
        print('| ef de búsqueda | recall al 100 % | recall de la referencia interna | diferencia | cargas al 100 % | cargas de la referencia |')
        print('|---|---|---|---|---|---|')
        for p in fpts:
            q = rby.get(p['ef'])
            if q is None:
                continue
            print('| %d | %.4f | %.4f | %+.4f | %.2f | %.2f |' % (
                p['ef'], p['recall'], q['recall'], p['recall'] - q['recall'], value(p, 'loadsMean'), value(q, 'loadsMean')))
        print()
        print('| | al 100 % | referencia interna | diferencia |')
        print('|---|---|---|---|')
        for label, target, field, spec in (('cargas a 0.95', 0.95, 'loadsMean', '%.2f'), ('cargas a 0.99', 0.99, 'loadsMean', '%.2f'),
                                           ('rondas a 0.99', 0.99, 'roundsMean', '%.2f'), ('endpoints a 0.99', 0.99, 'endpointsMean', '%.0f')):
            a, b = at(fpts, target, field), at(rpts, target, field)
            print('| %s | %s | %s | %s |' % (label, fmt(a, spec), fmt(b, spec), pct(rel(a, b))))
        print()

    print('### Grado por cohorte')
    print()
    cohorts = final.get('cohorts') or []
    print('| estado | ' + ' | '.join('%d → %d' % (c['from'], c['to']) for c in cohorts) + ' | todos |')
    print('|---' + '|---' * len(cohorts) + '|---|')
    for name, _, cp, _ in states:
        if not cp.get('cohorts'):
            continue
        cells = ['%.2f' % (c['edges'] / max(c['vectors'], 1)) for c in cp['cohorts']]
        cells += ['—'] * (len(cohorts) - len(cells))
        print('| %s | %s | %s |' % (name, ' | '.join(cells), fmt(degree(cp))))
    print()
    print('Cada cohorte, por el lugar en que entraron sus vectores. Al 100 %%, los que entraron antes de %s llevan aristas de la última tanda, '
          'y los demás, las de su insert.' % ('—' if len(rounds) == 0 else '%d' % (rounds[-1]['at'] // 2)))
    print()

    print('### Recall de arista contra el corpus actual')
    print()
    print('De la muestra de cada grupo, a nivel de vector y, entre paréntesis, de posting, con su grado.')
    print()
    print('| estado | insertados: arista (posting) | grado | vectores del lote del 10 %: arista (posting) | grado |')
    print('|---|---|---|---|---|')
    for name, _, cp, _ in states:
        cells = []
        for key in ('inserted', 'batch'):
            c = cp[key]
            if not c.get('vectors'):
                cells += ['—', '—']
                continue
            cells += ['%.4f (%.4f)' % recall_of(c), '%.2f' % (c['edges'] / max(c['vectors'], 1))]
        print('| %s | %s |' % (name, ' | '.join(cells)))
    print()

    print('### El coste de cada tanda')
    print()
    print('Por vector refrescado, salvo donde dice otra cosa.')
    print()
    costs = [refresh_cost(r['stats']) for r in rounds]
    cols = ['tanda %d, con %d' % (i + 1, r['at']) for i, r in enumerate(rounds)] + ['las %d' % len(rounds)]
    allc = costs + [add_costs(costs)] if costs else []
    if costs:
        print('| | ' + ' | '.join(cols) + ' |')
        print('|---' + '|---' * len(cols) + '|')

        def row(label, key, spec='%.2f', per=True):
            print('| %s | %s |' % (label, ' | '.join(spec % (c[key] / c['refreshed'] if per else c[key]) for c in allc)))

        row('vectores refrescados', 'refreshed', '%d', per=False)
        row('cargas de posting', 'loads', '%.1f')
        row('rondas', 'rounds', '%.1f')
        row('distancias por código: scan', 'scan', '%.0f')
        row('distancias por código: endpoints', 'endpoints', '%.0f')
        row('distancias por código, todas', 'code', '%.0f')
        row('exactas: rescore de la búsqueda', 'rescored', '%.1f')
        row('exactas: comprobar las aristas inversas', 'inverseDistances', '%.1f')
        row('vecinos que ya tenían al vector', 'inverseAlready')
        row('listas de vecinos leídas y comprobadas', 'inverseChecks')
        row('aristas inversas escritas', 'inverseEdges')
        print('| refrescos que no escriben ninguna | %s |' % ' | '.join(
            share1(c['postings'][0] / c['refreshed']) if c['postings'] else '—' for c in allc))
        print('| postings distintos con una lista de vecino escrita: media (p50 / p95) | %s |' % ' | '.join(
            '%.2f (%d / %d)' % (hist_mean(c['postings']), hist_p(c['postings'], 0.5), hist_p(c['postings'], 0.95)) for c in allc))
        print('| listas propias que cambian | %s |' % ' | '.join(share1(c['changed'] / c['refreshed']) for c in allc))
        row('aristas propias: antes', 'before')
        row('aristas propias: después', 'after')
        row('aristas propias: conservadas', 'kept')
        row('vectores que su búsqueda no encontró', 'selfMissing', '%d', per=False)
        row('segundos, indicativos', 'seconds', '%.0f', per=False)
        print()

    print('### El coste total, frente a v2.3')
    print()
    ins = insert_cost(final['stats'])
    mine = add_costs(costs) if costs else None
    if old is not None and mine is not None:
        ocps = {c['percent']: c for c in old['checkpoints']}
        oins = insert_cost(ocps[100]['stats'])
        orefresh = add_costs([refresh_cost(s['stats']) for s in old.get('refreshed') or []])
        print('En millones, salvo donde dice otra cosa. v2.3 refresca los %d vectores una vez, al final. v2.4 hace %d refrescos '
              'en %d tandas, durante el crecimiento.' % (orefresh['refreshed'], mine['refreshed'], len(rounds)))
        print()
        print('| | v2.3 | v2.4 | v2.4 frente a v2.3 |')
        print('|---|---|---|---|')

        def line(label, a, b, spec='%.1f', scale=1e6):
            print('| %s | %s | %s | %s |' % (label, spec % (a / scale), spec % (b / scale), pct(rel(b, a))))

        line('refrescos, en vectores', orefresh['refreshed'], mine['refreshed'], '%d', 1)
        line('inserts: cargas de posting de sus búsquedas', oins['loads'], ins['loads'])
        line('inserts: distancias por código', oins['code'], ins['code'], '%.0f')
        line('inserts: distancias exactas', oins['exact'], ins['exact'])
        line('inserts: aristas inversas escritas', oins['inverseEdges'], ins['inverseEdges'], '%.2f')
        line('refrescos: cargas de posting', orefresh['loads'], mine['loads'])
        line('refrescos: distancias por código', orefresh['code'], mine['code'], '%.0f')
        line('refrescos: distancias exactas', orefresh['rescored'] + orefresh['inverseDistances'],
             mine['rescored'] + mine['inverseDistances'])
        line('refrescos: listas propias escritas', orefresh['changed'], mine['changed'], '%.3f')
        line('refrescos: aristas inversas escritas', orefresh['inverseEdges'], mine['inverseEdges'], '%.3f')
        line('todo: cargas de posting', oins['loads'] + orefresh['loads'], ins['loads'] + mine['loads'])
        line('todo: distancias por código', oins['code'] + orefresh['code'], ins['code'] + mine['code'], '%.0f')
        line('todo: distancias exactas', oins['exact'] + orefresh['rescored'] + orefresh['inverseDistances'],
             ins['exact'] + mine['rescored'] + mine['inverseDistances'])
        print()
        print('| por refresco | v2.3 | v2.4 |')
        print('|---|---|---|')
        for label, key, spec in (('cargas de posting', 'loads', '%.1f'), ('distancias por código', 'code', '%.0f'),
                                 ('aristas inversas escritas', 'inverseEdges', '%.2f')):
            print('| %s | %s | %s |' % (label, spec % (orefresh[key] / orefresh['refreshed']), spec % (mine[key] / mine['refreshed'])))
        print('| listas propias que cambian | %s | %s |' % (
            share1(orefresh['changed'] / orefresh['refreshed']), share1(mine['changed'] / mine['refreshed'])))
        print()
        print('| por insert | v2.3 | v2.4 |')
        print('|---|---|---|')
        for label, key, spec in (('cargas de posting', 'loads', '%.2f'), ('distancias por código', 'code', '%.0f'),
                                 ('distancias exactas', 'exact', '%.1f'), ('aristas inversas escritas', 'inverseEdges', '%.2f')):
            print('| %s | %s | %s |' % (label, spec % (oins[key] / oins['inserted']), spec % (ins[key] / ins['inserted'])))
        print()
    elif mine is not None:
        print('- **Los %d refrescos, en millones:** %.1f cargas de posting, %.0f distancias por código y %.1f exactas.' % (
            mine['refreshed'], mine['loads'] / 1e6, mine['code'] / 1e6, (mine['rescored'] + mine['inverseDistances']) / 1e6))
        print('- **Los %d inserts, en millones:** %.1f cargas de posting y %.0f distancias por código en sus búsquedas.' % (
            ins['inserted'], ins['loads'] / 1e6, ins['code'] / 1e6))
        print()
    work = (run.get('rebuilt') or {}).get('work')
    if work is not None:
        print('- **La reconstrucción en lote de la referencia interna, en millones:** %.0f distancias por código y %.0f exactas de la asignación.' % (
            work['candidatesScored'] / 1e6, work['assignDistances'] / 1e6))
        print('  - %d vectores, el %.2f %%, no están en el posting de su centroide más cercano.' % (
            work['notNearest'], 100 * work['notNearest'] / work['vectors']))
        print()

    informative(run, why, rebuilt, final, by_ef)

    print('### La regla de ruido')
    print()
    print('- **Ninguna comparación a 0.99 entre corridas distintas.** Todos los estados de arriba son de esta corrida, '
          'y el lote es la única referencia de fuera, porque el criterio está escrito contra él.')
    print('- **De v2.3 solo se leen conteos.** Ni su índice refrescado ni su reconstrucción en lote se comparan a 0.99 con los de aquí.')


def informative(run, why, rebuilt, final, by_ef):
    print('### Informativo: por qué la regla del lote da más')
    print()
    if not why or not rebuilt:
        print('La corrida no hizo la parte informativa.')
        print()
        return
    if not (why.get('refreshed') and why.get('own')):
        print('La parte informativa no acabó sus dos medidas.')
        print()
        return
    st = why['refresh']
    c = refresh_cost(st)
    print('- **El instrumento:** con las aristas del 100 %% repuestas, un refresco de los %d vectores, como el de v2.3. '
          'Son %.1f cargas de posting y %.0f distancias por código por vector, %.2f aristas inversas escritas, y cambia el %s de las listas.' % (
              c['refreshed'], c['loads'] / c['refreshed'], c['code'] / c['refreshed'], c['inverseEdges'] / c['refreshed'],
              share1(c['changed'] / c['refreshed'])))
    print('- **No es parte del protocolo ni del criterio de v2.4.**')
    print()

    rb, rf, ow = by_ef['reconstruidas en lote'], by_ef['todo refrescado'], by_ef['sin las inversas']
    fn = by_ef['al 100 %']
    print('**Recall a igual ef, en la misma corrida:**')
    print()
    print('| ef de búsqueda | al 100 % | todo refrescado | sin las inversas | reconstruidas en lote | lote menos refrescado | '
          'sin las inversas menos refrescado | parte que cierra quitar las inversas |')
    print('|---|---|---|---|---|---|---|---|')
    closed = {}
    gaps = {}
    for ef in sorted(rb):
        if ef not in rf or ef not in ow:
            continue
        gap = rb[ef]['recall'] - rf[ef]['recall']
        gain = ow[ef]['recall'] - rf[ef]['recall']
        gaps[ef] = gap
        closed[ef] = gain / gap if gap > 0 else None
        print('| %d | %.4f | %.4f | %.4f | %.4f | %+.4f | %+.4f | %s |' % (
            ef, fn[ef]['recall'] if ef in fn else float('nan'), rf[ef]['recall'], ow[ef]['recall'], rb[ef]['recall'], gap, gain,
            share(closed[ef])))
    print()
    print('| | al 100 % | todo refrescado | sin las inversas | reconstruidas en lote |')
    print('|---|---|---|---|---|')
    curves = [curve(final['points']), curve(why['refreshed']['points']), curve(why['own']['points']), curve(rebuilt['points'])]
    for label, target in (('cargas a 0.95', 0.95), ('cargas a 0.99', 0.99)):
        print('| %s | %s |' % (label, ' | '.join(fmt(at(pts, target, 'loadsMean')) for pts in curves)))
    print('| aristas por vector | %s |' % ' | '.join(fmt(degree(cp)) for cp in (final, why['refreshed'], why['own'], rebuilt)))
    print('| recall de arista: insertados / lote del 10 %% | %s |' % ' | '.join(
        '%.4f / %.4f' % (recall_of(cp['inserted'])[0], recall_of(cp['batch'])[0]) for cp in (final, why['refreshed'], why['own'], rebuilt)))
    print()

    present = [ef for ef in DECISIVE_EFS if ef in gaps]
    repeats = any(gaps[ef] >= MIN_GAP for ef in present)
    print('**Comprobación previa.** La regla del lote da %s más de recall que el índice refrescado a ef %s.' % (
        ' y '.join('%+.4f' % gaps[ef] for ef in present), ' y '.join(str(ef) for ef in present)))
    if repeats:
        print('La diferencia de v2.3 se repite en esta corrida (el umbral era %.4f en alguno de los dos).' % MIN_GAP)
    else:
        print('**No llega a %.4f en ninguno de los dos: en esta corrida no hay diferencia que explicar.** '
              'Lo de abajo se da como quedó, sin lectura.' % MIN_GAP)
    print()

    a = why.get('analysis')
    if not a:
        print('La corrida no llegó al análisis de la muestra.')
        print()
        return
    wanted = a['wanted']
    rm, bm = a['refresh'], a['batch']
    print('**La muestra:** %d vectores, con %d vecinos exactos fuera de su posting, que son las aristas que pide la regla.' % (
        a['sample'], wanted))
    print()
    print('| | el refresco | la regla del lote |')
    print('|---|---|---|')
    print('| aristas que pide la regla | %d | %d |' % (wanted, wanted))
    print('| las que no pone | %d (%s) | %d (%s) |' % (rm['missed'], share1(rm['missed'] / wanted), bm['missed'], share1(bm['missed'] / wanted)))
    print('| recall de arista | %.4f | %.4f |' % (1 - rm['missed'] / wanted, 1 - bm['missed'] / wanted))
    print('| las que no pone ninguna de las dos | %d | %d |' % (a['bothMissed'], a['bothMissed']))
    print()

    print('**Hipótesis 1: lo que deja fuera cada regla, por causa.**')
    print()
    print('| el vecino que falta está | el refresco | parte de sus fallos | la regla del lote | parte de sus fallos |')
    print('|---|---|---|---|---|')

    def part(x, total):
        return share1(x / total) if total else '—'

    print('| (a) en un posting al que la regla no tenía cómo llegar: ninguna arista seguida llevaba a él, o fuera de los sondeados | %d | %s | %d | %s |' % (
        rm['unreached'], part(rm['unreached'], rm['missed']), bm['unreached'], part(bm['unreached'], bm['missed'])))
    print('| (b) en un posting al que llevaba una arista seguida, sin leer | %d | %s | — | — |' % (
        rm['knownNotRead'], part(rm['knownNotRead'], rm['missed'])))
    print('| (c) en un posting leído, fuera de los candidatos por la estimación del código | %d | %s | %d | %s |' % (
        rm['read'], part(rm['read'], rm['missed']), bm['read'], part(bm['read'], bm['missed'])))
    print()
    not_read = (rm['unreached'] + rm['knownNotRead']) / rm['missed'] if rm['missed'] else None
    batch_read = bm['read'] / bm['missed'] if bm['missed'] else None
    reading = 'sin lectura'
    if repeats and not_read is not None and batch_read is not None:
        if not_read > 0.5 and batch_read > 0.5:
            reading = '**confirmada**'
        elif rm['read'] / rm['missed'] > 0.5:
            reading = '**refutada**'
        else:
            reading = '**a medias**'
    print('- **Lectura:** el %s de los fallos del refresco están en postings que su búsqueda no leyó, y el %s de los del lote, '
          'en postings que sí leyó. Hipótesis 1, %s.' % (share1(not_read), share1(batch_read), reading))
    if rm['missed']:
        print('  - De los que el refresco no leyó, a %d no llevaba ninguna arista seguida y a %d sí: el %s es del grafo y el %s del ef.' % (
            rm['unreached'], rm['knownNotRead'], part(rm['unreached'], rm['unreached'] + rm['knownNotRead']),
            part(rm['knownNotRead'], rm['unreached'] + rm['knownNotRead'])))
    print()

    print('**Hipótesis 2: las escrituras inversas del refresco.**')
    print()
    shown = [(ef, closed[ef]) for ef in present if ef in closed]
    reading = 'sin lectura'
    if repeats and shown and all(v is not None for _, v in shown):
        if all(v >= 0.5 for _, v in shown):
            reading = '**confirmada**'
        elif all(v < 0.25 for _, v in shown):
            reading = '**refutada**'
        else:
            reading = '**a medias**'
    print('- **Lectura:** quitar el efecto de las inversas cierra el %s de lo que separa al índice refrescado de la regla del lote, a ef %s. '
          'Hipótesis 2, %s.' % (' y el '.join(share(v) for _, v in shown), ' y '.join(str(ef) for ef, _ in shown), reading))
    print('- **En la muestra,** las escrituras inversas de los demás sacaron %d aristas de las listas que dejó cada refresco, y %d eran vecinos exactos (%s). '
          'Metieron %d, y %d lo eran (%s).' % (
              a['inverseOut'], a['inverseOutExact'], part(a['inverseOutExact'], a['inverseOut']),
              a['inverseIn'], a['inverseInExact'], part(a['inverseInExact'], a['inverseIn'])))
    print()

    print('**Hipótesis 3: los fallos, por las aristas entrantes del posting del vecino.**')
    print()
    titles = (('final', 'aristas entrantes del posting, en el índice del 100 %'),
              ('finalPerMember', 'aristas entrantes por miembro, en el índice del 100 %'),
              ('refreshed', 'aristas entrantes del posting, en el índice refrescado'),
              ('rebuilt', 'aristas entrantes del posting, en el reconstruido en lote'))
    readings = {}
    for key, title in titles:
        fifths = (a.get('fifths') or {}).get(key)
        if not fifths:
            continue
        print('*%s:*' % (title[0].upper() + title[1:]))
        print()
        print('| quinta parte | aristas entrantes | vecinos que pide la regla | fallos del refresco | tasa | cuota de sus fallos | '
              'fallos del lote | tasa | cuota de sus fallos |')
        print('|---|---|---|---|---|---|---|---|---|')
        spec = '%.2f' if key.endswith('PerMember') else '%.0f'
        for i, f in enumerate(fifths):
            print('| %d | de %s a %s | %d | %d | %s | %s | %d | %s | %s |' % (
                i + 1, spec % f['from'], spec % f['to'], f['wanted'], f['refreshMissed'], part(f['refreshMissed'], f['wanted']),
                part(f['refreshMissed'], rm['missed']), f['batchMissed'], part(f['batchMissed'], f['wanted']),
                part(f['batchMissed'], bm['missed'])))
        print()
        if rm['missed'] and bm['missed']:
            readings[key] = (fifths[0]['refreshMissed'] / rm['missed'], fifths[0]['batchMissed'] / bm['missed'])
    if 'final' in readings:
        rs, bs = readings['final']
        if not repeats:
            reading = 'sin lectura'
        elif rs >= H3_REFRESH_SHARE and bs <= H3_BATCH_SHARE:
            reading = '**confirmada**'
        elif rs < H3_REFUTED_SHARE or bs >= rs - H3_REFUTED_GAP:
            reading = '**refutada**'
        else:
            reading = '**a medias**'
        print('- **Lectura,** sobre las aristas entrantes en el índice del 100 %%: la quinta parte con menos tiene el %s de los fallos del refresco '
              'y el %s de los del lote. Hipótesis 3, %s.' % (share1(rs), share1(bs), reading))
        print('  - Los umbrales: confirmada con el 30 % o más del refresco y el 25 % o menos del lote; refutada con menos del 25 % del refresco, '
              'o con la cuota del lote a 3 puntos o menos de la del refresco.')
        for key, title in titles[1:]:
            if key in readings:
                print('  - Con las %s: el %s y el %s.' % (title, share1(readings[key][0]), share1(readings[key][1])))
        print()

    spreads = a.get('incoming') or {}
    if spreads:
        print('**Las aristas entrantes de un posting, en cada estado:**')
        print()
        print('| | mínimo | p1 | p5 | p10 | p25 | p50 | p75 | p90 | p99 | máximo | media |')
        print('|---|---|---|---|---|---|---|---|---|---|---|---|')
        for key, title in (('final', 'al 100 %'), ('refreshed', 'todo refrescado'), ('rebuilt', 'reconstruidas en lote'),
                           ('finalPerMember', 'al 100 %, por miembro'), ('refreshedPerMember', 'todo refrescado, por miembro'),
                           ('rebuiltPerMember', 'reconstruidas en lote, por miembro')):
            s = spreads.get(key)
            if not s:
                continue
            spec = '%.2f' if key.endswith('PerMember') else '%.0f'
            print('| %s | %s |' % (title, ' | '.join(spec % s[k] for k in (
                'min', 'p1', 'p5', 'p10', 'p25', 'p50', 'p75', 'p90', 'p99', 'max', 'mean'))))
        print()


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] if len(sys.argv) > 4 else '80')
