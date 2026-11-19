#!/usr/bin/env python3
"""v6.1 (bench-report.md, "v6.1: limpieza en dos fases"), from the file
run_v61.sh leaves: the maintenance pass in two phases, repair and eliminate,
in its cells over 30 % deleted at random, each next to a batch index of the
vectors that are left, and the churn with the best of them. Counts: nothing
is timed.

usage: repair_verdict.py <dir of the run> > repair-verdict.md

The criterion is that of v6.0: a cell meets it if it is within 0.005 of
recall of its reference at equal ef, with loads at 0.95 at +15 % or less and
at 0.99 at +10 % or less, against the batch index with S = 236. The cheapest
cell that meets it is reported, by an order of cost fixed before measuring.
The noise floor is the larger of that of v2.0 and what the two runs of v6.0
differed: 0.0012 of recall, 1.1 % of loads at 0.95 and 3.0 % at 0.99.
"""
import json
import os
import sys

LABEL = 'dbpedia-1m-repair'
SLACK, LOADS95, LOADS99 = 0.005, 1.15, 1.10
NOISE_RECALL, NOISE95, NOISE99 = 0.0012, 0.011, 0.03
CELLS = {
    'v60-4': 'referencia: la celda 4 de v6.0',
    'r50-m118': 'la mitad o más, fusión de 118 (mía)',
    'r25-m118': 'el 25 % o más, fusión de 118',
    'r25-m177': 'el 25 % o más, fusión de 177',
    'r1-m118': 'una o más, fusión de 118',
    'r1-m177': 'una o más, fusión de 177',
}
COST = ('r25-m118', 'r25-m177', 'r1-m118', 'r1-m177')


def num(x):
    """An integer with its thousands apart."""
    s = '%.0f' % x
    groups = []
    while len(s) > 3:
        groups.insert(0, s[-3:])
        s = s[:-3]
    return ' '.join([s] + groups)


def signed(x, digits=4):
    return ('%s%.' + str(digits) + 'f') % ('+' if x >= 0 else '−', abs(x))


def percent(ratio):
    rel = ratio - 1
    return '%s%.1f %%' % ('+' if rel >= 0 else '−', abs(100 * rel))


def mark(met, close):
    return ('sí' if met else 'no') + (', a menos del suelo de ruido' if close else '')


def conditions(cell):
    """The three conditions of a cell: (text, met, within the noise floor)."""
    v = cell['verdict']
    out = [('%s en ef %d' % (signed(v['worstRecallDifference']), v['worstRecallEF']),
            v['worstRecallDifference'] >= -SLACK, abs(v['worstRecallDifference'] + SLACK) < NOISE_RECALL)]
    for reached, loads, ratio, bar, floor, what in (
            (cell['reached95'], cell['loads95'], v['loads95OverReference'], LOADS95, NOISE95, '0.95'),
            (cell['reached99'], cell['loads99'], v['loads99OverReference'], LOADS99, NOISE99, '0.99')):
        if reached and ratio:
            out.append(('%.2f, %s' % (loads, percent(ratio)), ratio <= bar, abs(ratio - bar) < floor))
        else:
            out.append(('no llega a %s' % what, False, False))
    return out


def name(cell):
    return CELLS.get(cell.get('label'), cell.get('label') or cell['name'])


def criterion_table(cells):
    print('| celda | peor recall a igual ef | cumple | cargas a 0.95 | cumple | cargas a 0.99 | cumple | las tres |')
    print('|---|---|---|---|---|---|---|---|')
    for cell in cells:
        if not cell.get('points'):
            print('| %s | sin medir: %s | | | | | | |' % (name(cell), cell['coverage']))
            continue
        conds = conditions(cell)
        print('| %s | %s | %s | %s | %s | %s | %s | %s |' % (
            name(cell), conds[0][0], mark(conds[0][1], conds[0][2]), conds[1][0], mark(conds[1][1], conds[1][2]),
            conds[2][0], mark(conds[2][1], conds[2][2]), '**sí**' if cell['verdict']['meets'] else 'no'))
    print()


def cost_tables(cells):
    print('| celda | vectores reparados | de los vivos | cargas por reparación | cargas de la fase | endpoints por '
          'reparación | distancias exactas por reparación | aristas inversas |')
    print('|---|---|---|---|---|---|---|---|')
    for cell in cells:
        r = cell.get('refresh')
        if not r:
            continue
        w, n = r['work'], max(r['refreshed'], 1)
        print('| %s | %s | %.1f %% | %.1f | %s | %s | %.1f | %s |' % (
            name(cell), num(r['refreshed']), 100 * r['refreshed'] / max(cell['counts']['live'], 1),
            w['searchLoads'] / n, num(w['searchLoads']), num(w['searchEndpoints'] / n), w['searchRescored'] / n,
            num(w['inverseEdges'])))
    print()
    print('| celda | postings reescritos | borrados soltados | aristas quitadas sin reparar | fusiones | no hechas '
          'por el tamaño | miembros movidos | splits tras una fusión | postings sin vivos retirados |')
    print('|---|---|---|---|---|---|---|---|---|')
    for cell in cells:
        w = cell['work']
        print('| %s | %s | %s | %s | %s | %s | %s | %s | %s |' % (
            name(cell), num(w['postingsRewritten']), num(w['deletedMembersDropped']), num(w.get('edgesDropped', 0)),
            num(w['merges']), num(w.get('mergesSkippedBySize', 0)), num(w['liveMembersMoved']),
            num(w['splitsAfterMerge']), num(w['emptyPostingsRetired'])))
    print()
    print('| celda | postings | tamaño medio, en vivos | miembros que ocupan sitio | por debajo de 118 | por debajo de '
          '177 | aristas vivas por vector | aristas muertas |')
    print('|---|---|---|---|---|---|---|---|')
    for cell in cells:
        c = cell['counts']
        print('| %s | %s | %.1f | %.1f | %s | %s | %.2f | %.3f |' % (
            name(cell), num(c['postings']), c['liveMembers'] / max(c['postings'], 1),
            c['members'] / max(c['postings'], 1), num(c['postingsUnderMin']), num(c['postingsUnderThreeQuarters']),
            (c['edges'] - c['deadEdges']) / max(c['live'], 1), c['deadEdges'] / max(c['edges'], 1)))
    print()


def fill_tables(stage, target):
    ref = stage['reference']
    fills = {f['targetSize']: f for f in stage.get('fills') or [] if f.get('points')}
    if not fills:
        print('Sin referencias de llenado.')
        print()
        return
    print('| referencia en lote | postings | cargas a 0.95 | sobre la de S = %d | cargas a 0.99 | sobre la de S = %d |' % (
        target, target))
    print('|---|---|---|---|---|---|')
    print('| S = %d, la del criterio | %s | %.2f | — | %.2f | — |' % (
        target, num(ref['postings']), ref['loads95'], ref['loads99']))
    for size in sorted(fills):
        f = fills[size]
        print('| S = %d, de llenado | %s | %.2f | %s | %.2f | %s |' % (
            size, num(f['postings']), f['loads95'], percent(f['loads95'] / ref['loads95']), f['loads99'],
            percent(f['loads99'] / ref['loads99'])))
    print()
    print('| celda | su referencia | peor recall a igual ef contra ella | cargas a 0.95 | cargas a 0.99 | dentro de '
          'los dos umbrales |')
    print('|---|---|---|---|---|---|')
    for cell in stage['cells']:
        f = cell.get('fill')
        if not f:
            continue
        print('| %s | S = %d | %s | %s | %s | %s |' % (
            name(cell), f['targetSize'], signed(f['worstRecallDifference']),
            percent(f['loads95OverReference']) if f['loads95OverReference'] else 'no llega',
            percent(f['loads99OverReference']) if f['loads99OverReference'] else 'no llega',
            'sí' if f['loadsWithin'] else 'no'))
    print()


def work_tables(stage):
    ref = stage['reference']
    cells = [c for c in stage['cells'] if c.get('points')]
    if not cells or not ref.get('points'):
        return
    at = next((i for i, p in enumerate(ref['points']) if p['ef'] == 160), None)
    if at is not None:
        print('Lo que ve una búsqueda, en ef 160:')
        print()
        print('| | recall | cargas | vectores puntuados en el scan | endpoints puntuados |')
        print('|---|---|---|---|---|')
        p = ref['points'][at]
        print('| referencia, el lote con S = 236 | %.4f | %.2f | %s | %s |' % (
            p['recall'], p['loads'], num(p['scanned']), num(p['endpoints'])))
        for cell in cells:
            p = cell['points'][at]
            print('| %s | %.4f | %.2f | %s | %s |' % (
                name(cell), p['recall'], p['loads'], num(p['scanned']), num(p['endpoints'])))
        print()
    print('Recall a igual ef, frente a la referencia:')
    print()
    print('| celda | %s |' % ' | '.join('ef %s' % num(p['ef']) for p in ref['points']))
    print('|---|%s' % ('---|' * len(ref['points'])))
    for cell in cells:
        print('| %s | %s |' % (name(cell), ' | '.join(
            signed(p['recall'] - q['recall']) for p, q in zip(cell['points'], ref['points']))))
    print()
    print('Cargas a igual ef, frente a la referencia:')
    print()
    print('| celda | %s |' % ' | '.join('ef %s' % num(p['ef']) for p in ref['points']))
    print('|---|%s' % ('---|' * len(ref['points'])))
    for cell in cells:
        print('| %s | %s |' % (name(cell), ' | '.join(
            percent(p['loads'] / q['loads']) for p, q in zip(cell['points'], ref['points']))))
    print()


def main():
    path = os.path.join(sys.argv[1], LABEL + '.json')
    if not os.path.exists(path):
        print('La corrida no ha dejado resultados todavía.')
        return
    result = json.load(open(path))
    stages = {s['name']: s for s in result['stages']}
    target = result['config']['targetPostingSize']

    print('### La corrida')
    print()
    print('- **Dataset:** %s, %s vectores, %s queries, k = %d. Malla de ef: %s.' % (
        result['dataset'], num(result['vectors']), num(result['queries']), result['k'],
        ', '.join(num(ef) for ef in result['grid'])))
    cfg = result['config']
    print('- **Índice:** S = %d, P = %d, R = %d, split a T = %d, ef de inserción %d.' % (
        cfg['targetPostingSize'], cfg['buildProbe'], cfg['rescueLimit'], result['threshold'], result['insertEF']))
    if result.get('build'):
        b = result['build']
        print('- **El lote del 100 %%:** %s postings de %.1f miembros de media.' % (num(b['postings']), b['sizeMean']))
    if result.get('notRun'):
        print('- **Sin correr, por la hora:** %s.' % ', '.join(result['notRun']))
    stopped = [s for s in result['stages'] if s.get('stopped')]
    for s in stopped:
        print('- **STOP en %s:** %s.' % (s['name'], s['stopped']))
    if not stopped:
        print('- **Ningún stop:** ningún vector borrado devuelto, cobertura completa, ninguna lista reparada hacia un '
              'borrado y ningún borrado tras la segunda fase, en todo lo medido.')
    print()

    stage = stages.get('repair30')
    print('### El criterio: con el 30 % borrado al azar')
    print()
    if stage is None or not stage['cells'] or not stage['reference'].get('points'):
        print('Sin medir todavía.')
        print()
    else:
        ref = stage['reference']
        print('- **Corpus:** %s vectores, %s borrados. Quedan %s.' % (
            num(stage['corpus']), num(stage['deleted']), num(stage['corpus'] - stage['deleted'])))
        print('- **Referencia, el lote de los que quedan con S = %d:** %s postings. Cargas a 0.95: %.2f. A 0.99: %.2f.' % (
            target, num(ref['postings']), ref['loads95'], ref['loads99']))
        print()
        criterion_table(stage['cells'])
        by = {c.get('label'): c for c in stage['cells'] if c.get('points')}
        meeting = [k for k in COST if k in by and by[k]['verdict']['meets']]
        measured = [k for k in COST if k in by]
        if meeting:
            print('**La celda más barata que cumple, de las cuatro del encargo: %s.** Cumplen: %s.' % (
                CELLS[meeting[0]], '; '.join(CELLS[k] for k in meeting)))
        elif len(measured) == len(COST):
            print('**Ninguna de las cuatro celdas del encargo cumple el criterio.**')
        else:
            print('**Sin veredicto: faltan celdas por medir.**')
        print()
        print('### Lo que cuesta la pasada')
        print()
        cost_tables(stage['cells'])
        print('### Contra su referencia de llenado')
        print()
        fill_tables(stage, target)
        print('### A igual ef')
        print()
        work_tables(stage)

    churn = stages.get('repairchurn')
    if churn is not None:
        print('### Churn, con la celda %s' % CELLS.get(result.get('bestRepair'), result.get('bestRepair') or '?'))
        print()
        if result.get('bestRepairWhy'):
            print('- **Por qué esa celda:** %s.' % {
                'the cheapest cell that meets the criterion with 30 % deleted at random':
                    'es la más barata que cumple el criterio con el 30 % al azar',
                'given with -bench.repaircell': 'dada a mano',
            }.get(result['bestRepairWhy'], 'ninguna cumple el criterio, y es la de menos cargas a 0.99 sobre su '
                  'referencia, quedándose la más barata entre dos a menos del suelo de ruido'))
        if churn.get('stopped'):
            print('- **STOP:** %s.' % churn['stopped'])
        if churn.get('rounds'):
            print()
            print('| ronda | borrados | insertados | postings | tamaño medio | aristas muertas | splits de la ronda | '
                  'cargas por insert | reparados en la ronda | cargas por reparación | fusiones, acumuladas | no hechas '
                  'por el tamaño, acumuladas |')
            print('|---|---|---|---|---|---|---|---|---|---|---|---|')
            for r in churn['rounds']:
                c, w, ins, rep = r['counts'], r['work'], r['inserts'], r['refresh']
                print('| %d | %s | %s | %s | %.1f | %.3f | %s | %.1f | %s | %.1f | %s | %s |' % (
                    r['round'], num(r['deleted']), num(r['inserted']), num(c['postings']),
                    c['liveMembers'] / max(c['postings'], 1), c['deadEdges'] / max(c['edges'], 1), num(ins['splits']),
                    ins['searchLoads'] / max(ins['inserted'], 1), num(rep['refreshed']),
                    rep['work']['searchLoads'] / max(rep['refreshed'], 1), num(w['merges']),
                    num(w.get('mergesSkippedBySize', 0))))
            print()
        if churn['cells'] and churn['reference'].get('points'):
            ref = churn['reference']
            print('- **Corpus:** %s vectores, %s borrados y %s insertados.' % (
                num(churn['corpus']), num(churn['deleted']), num(churn.get('inserted', 0))))
            print('- **Referencia, el lote del conjunto final con S = %d:** %s postings. Cargas a 0.95: %.2f. A 0.99: '
                  '%.2f.' % (target, num(ref['postings']), ref['loads95'], ref['loads99']))
            print()
            criterion_table(churn['cells'])
            cost_tables_churn(churn)
            fill_tables(churn, target)
            work_tables(churn)


def cost_tables_churn(churn):
    cell = churn['cells'][-1]
    c, w = cell['counts'], cell['work']
    repaired = sum(r['refresh']['refreshed'] for r in churn['rounds'])
    loads = sum(r['refresh']['work']['searchLoads'] for r in churn['rounds'])
    print('- **Al acabar:** %s postings de %.1f vivos de media, %s por debajo de 118 y %s por debajo de 177.' % (
        num(c['postings']), c['liveMembers'] / max(c['postings'], 1), num(c['postingsUnderMin']),
        num(c['postingsUnderThreeQuarters'])))
    print('- **Reparación, en las diez rondas:** %s vectores, %s cargas, %.1f por reparación.' % (
        num(repaired), num(loads), loads / max(repaired, 1)))
    print('- **Eliminación:** %s postings reescritos, %s borrados soltados, %s aristas quitadas sin reparar, %s '
          'fusiones, %s no hechas por el tamaño, %s miembros movidos, %s splits tras una fusión.' % (
              num(w['postingsRewritten']), num(w['deletedMembersDropped']), num(w.get('edgesDropped', 0)),
              num(w['merges']), num(w.get('mergesSkippedBySize', 0)), num(w['liveMembersMoved']),
              num(w['splitsAfterMerge'])))
    print()


if __name__ == '__main__':
    main()
