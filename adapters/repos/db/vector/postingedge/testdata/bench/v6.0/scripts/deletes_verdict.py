#!/usr/bin/env python3
"""v6.0 (bench-report.md, "v6.0: deletes en memoria"), from the file
run_v60.sh leaves: every point of deletes with its four cells, each on top of
the one before, next to a batch index of the vectors that are left, and the
churn with the best cell. Counts: nothing is timed.

usage: deletes_verdict.py <dir of the run> > deletes-verdict.md

The user's criterion: the deletes hold if, with 30 % deleted at random, some
cell is within 0.005 of recall of its reference at equal ef, with loads at
0.95 at +15 % or less and at 0.99 at +10 % or less. The simplest cell that
meets it is reported. Read as in v2.2: the recall at every ef of the grid,
the worst difference counting; the loads read at each recall between the two
ef around it. The noise floor is that of v2.0: 0.0009 of recall, 0.5 % of
loads at 0.95 and 3.0 % at 0.99.
"""
import json
import os
import sys

LABEL = 'dbpedia-1m-deletes'
SLACK, LOADS95, LOADS99 = 0.005, 1.15, 1.10
NOISE_RECALL, NOISE95, NOISE99 = 0.0009, 0.005, 0.03
STAGES = {
    'random10': 'el 10 % al azar',
    'random30': 'el 30 % al azar',
    'random50': 'el 50 % al azar',
    'clustered30': 'el 30 % en regiones enteras',
    'churn': 'churn: el 30 % borrado y otro tanto insertado',
}
CELLS = {
    1: '1, borrado lógico',
    2: '2, endpoints borrados sin puntuar',
    3: '3, más limpieza física',
    4: '4, más refresco',
}


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


def near(value, bar, floor):
    """Whether a condition is met or failed by less than the noise floor."""
    return abs(value - bar) < floor


def conditions(cell):
    """The three conditions of a cell: (text, met, within the noise floor)."""
    v = cell['verdict']
    out = [('%s en ef %d' % (signed(v['worstRecallDifference']), v['worstRecallEF']),
            v['worstRecallDifference'] >= -SLACK, near(v['worstRecallDifference'], -SLACK, NOISE_RECALL))]
    if cell['reached95'] and v['loads95OverReference']:
        out.append(('%.2f, %s' % (cell['loads95'], percent(v['loads95OverReference'])),
                    v['loads95OverReference'] <= LOADS95, near(v['loads95OverReference'], LOADS95, NOISE95)))
    else:
        out.append(('no llega a 0.95', False, False))
    if cell['reached99'] and v['loads99OverReference']:
        out.append(('%.2f, %s' % (cell['loads99'], percent(v['loads99OverReference'])),
                    v['loads99OverReference'] <= LOADS99, near(v['loads99OverReference'], LOADS99, NOISE99)))
    else:
        out.append(('no llega a 0.99', False, False))
    return out


def mark(met, close):
    return ('sí' if met else 'no') + (', a menos del suelo de ruido' if close else '')


def stage_tables(stage):
    ref = stage['reference']
    print('- **Corpus:** %s vectores, %s borrados%s. Quedan %s.' % (
        num(stage['corpus']), num(stage['deleted']),
        ' y %s insertados' % num(stage['inserted']) if stage.get('inserted') else '',
        num(stage['corpus'] - stage['deleted'] + stage.get('inserted', 0))))
    if stage.get('regions'):
        print('- **Regiones:** %d, con %s postings en total.' % (stage['regions'], num(stage['regionPostings'])))
    if not ref.get('points'):
        print('- **Sin referencia todavía.**')
        print()
        return
    print('- **Referencia, el lote de los que quedan:** %s postings de %.1f miembros. Cargas a 0.95: %.2f. A 0.99: %s.' % (
        num(ref['postings']), ref['sizeMean'], ref['loads95'],
        '%.2f, entre ef %d y %d' % (ref['loads99'], ref['crossing99'][0], ref['crossing99'][1]) if ref['reached99']
        else 'no llega en la malla'))
    if stage.get('truthFallbacks'):
        print('- **Ground truth:** %s queries con menos de 10 vivos entre sus 100 vecinos del dataset, resueltas por '
              'fuerza bruta.' % num(stage['truthFallbacks']))
    if stage.get('stopped'):
        print('- **STOP:** %s.' % stage['stopped'])
    print()
    cells = stage['cells']
    if not cells:
        return

    print('| celda | peor recall a igual ef | cumple | cargas a 0.95 | cumple | cargas a 0.99 | cumple | cruza 0.99 entre ef | '
          'las tres |')
    print('|---|---|---|---|---|---|---|---|---|')
    for cell in cells:
        if not cell.get('points'):
            print('| %s | sin medir: %s | | | | | | | |' % (CELLS[cell['cell']], cell['coverage']))
            continue
        conds = conditions(cell)
        crossing = '%d y %d' % tuple(cell['crossing99']) if cell['reached99'] else '—'
        print('| %s | %s | %s | %s | %s | %s | %s | %s | %s |' % (
            CELLS[cell['cell']], conds[0][0], mark(conds[0][1], conds[0][2]), conds[1][0], mark(conds[1][1], conds[1][2]),
            conds[2][0], mark(conds[2][1], conds[2][2]), crossing, '**sí**' if cell['verdict']['meets'] else 'no'))
    print()

    print('Recall y cargas a igual ef:')
    print()
    measured = [c for c in cells if c.get('points')]
    print('| ef | referencia: recall | cargas | %s |' % ' | '.join(
        'celda %d: recall | cargas' % c['cell'] for c in measured))
    print('|---|---|---|%s' % ('---|---|' * len(measured)))
    for i, point in enumerate(ref['points']):
        row = '| %d | %.4f | %.2f |' % (point['ef'], point['recall'], point['loads'])
        for c in measured:
            p = c['points'][i]
            row += ' %.4f | %.2f |' % (p['recall'], p['loads'])
        print(row)
    print()

    print('| celda | postings | miembros vivos por posting | miembros que ocupan sitio | bajo el mínimo | vacíos | '
          'aristas muertas | vectores con la mitad o más |')
    print('|---|---|---|---|---|---|---|---|')
    for cell in cells:
        c = cell['counts']
        print('| %s | %s | %.1f | %.1f | %s | %s | %.3f | %s |' % (
            CELLS[cell['cell']], num(c['postings']), c['liveMembers'] / max(c['postings'], 1),
            c['members'] / max(c['postings'], 1), num(c['postingsUnderMin']), num(c['postingsEmpty']),
            c['deadEdges'] / max(c['edges'], 1), num(c['vectorsHalfDead'])))
    print()

    last = cells[-1]
    work = last['work']
    if work['postingsRewritten'] or work['merges'] or work['emptyPostingsRetired'] or last.get('refresh'):
        print('- **Limpieza:** %s postings reescritos, con %s miembros vivos copiados; %s borrados soltados; %s fusiones, que '
              'movieron %s miembros vivos; %s postings retirados sin ningún vivo; %s splits tras una fusión.' % (
                  num(work['postingsRewritten']), num(work['liveMembersCopied']), num(work['deletedMembersDropped']),
                  num(work['merges']), num(work['liveMembersMoved']), num(work['emptyPostingsRetired']),
                  num(work['splitsAfterMerge'])))
    refresh = last.get('refresh')
    if refresh:
        w, n = refresh['work'], max(refresh['refreshed'], 1)
        print('- **Refresco:** %s vectores, el %.1f %% de los vivos. Por vector: %.1f cargas, %s endpoints puntuados y '
              '%.1f distancias exactas. Entraron %s aristas inversas.' % (
                  num(refresh['refreshed']), 100 * refresh['refreshed'] / max(last['counts']['live'], 1),
                  w['searchLoads'] / n, num(w['searchEndpoints'] / n), w['searchRescored'] / n, num(w['inverseEdges'])))
    print()


def main():
    d = sys.argv[1]
    path = os.path.join(d, LABEL + '.json')
    if not os.path.exists(path):
        print('La corrida no ha dejado resultados todavía.')
        return
    result = json.load(open(path))
    stages = {s['name']: s for s in result['stages']}

    print('### La corrida')
    print()
    print('- **Dataset:** %s, %s vectores, %s queries, k = %d. Malla de ef: %s.' % (
        result['dataset'], num(result['vectors']), num(result['queries']), result['k'],
        ', '.join(num(ef) for ef in result['grid'])))
    cfg = result['config']
    print('- **Índice:** S = %d, P = %d, R = %d, split a T = %d, ef de inserción %d. Limpieza: se reescribe por encima '
          'del %.0f %% de borrados y se fusiona por debajo de %d vivos.' % (
              cfg['targetPostingSize'], cfg['buildProbe'], cfg['rescueLimit'], result['threshold'], result['insertEF'],
              100 * result['rewriteOver'], result['minLive']))
    if result.get('build'):
        b = result['build']
        print('- **El lote del 100 %%:** %s postings de %.1f miembros de media.' % (num(b['postings']), b['sizeMean']))
    if result.get('notRun'):
        print('- **Sin correr, por la hora:** %s.' % ', '.join(STAGES.get(s, s) for s in result['notRun']))
    stopped = [s for s in result['stages'] if s.get('stopped')]
    if stopped:
        for s in stopped:
            print('- **STOP en %s:** %s.' % (STAGES.get(s['name'], s['name']), s['stopped']))
    else:
        print('- **Ningún stop:** ningún vector borrado devuelto y cobertura completa en todo lo medido.')
    print()

    print('### El criterio: con el 30 % borrado al azar')
    print()
    main_stage = stages.get('random30')
    if main_stage is None or not main_stage['cells'] or not main_stage['reference'].get('points'):
        print('Sin medir todavía.')
        print()
    else:
        print('Los deletes se sostienen si alguna celda queda a %.3f de recall o menos de su referencia a igual ef, con '
              'cargas a 0.95 un %.0f %% o menos por encima y a 0.99 un %.0f %% o menos.' % (
                  SLACK, 100 * (LOADS95 - 1), 100 * (LOADS99 - 1)))
        print()
        stage_tables(main_stage)
        meeting = [c for c in main_stage['cells'] if c.get('points') and c['verdict']['meets']]
        if meeting:
            print('**Los deletes se sostienen: la celda más simple que cumple es la %s.**' % CELLS[meeting[0]['cell']])
        elif len(main_stage['cells']) == 4 and not main_stage.get('stopped'):
            print('**Ninguna celda cumple el criterio: por la regla como está escrita, los deletes no se sostienen con '
                  'estas cuatro celdas.**')
        else:
            print('**Sin veredicto: faltan celdas por medir.**')
        print()

    for name in ('random10', 'random50', 'clustered30'):
        stage = stages.get(name)
        if stage is None:
            continue
        print('### %s' % (STAGES[name][0].upper() + STAGES[name][1:]))
        print()
        stage_tables(stage)

    churn = stages.get('churn')
    if churn is not None:
        print('### Churn, con la celda %s' % CELLS.get(result.get('best'), '?'))
        print()
        if result.get('bestWhy'):
            print('- **Por qué esa celda:** %s.' % {
                'the simplest cell that meets the criterion with 30 % deleted at random':
                    'es la más simple que cumple el criterio al 30 % al azar',
            }.get(result['bestWhy'], 'ninguna cumple el criterio al 30 % al azar, y es la de menos cargas a 0.99 sobre su '
                  'referencia, quedándose la más simple entre dos a menos del suelo de ruido'))
        if churn.get('rounds'):
            print()
            print('| ronda | borrados | insertados | postings | vivos por posting | miembros que ocupan sitio | '
                  'aristas muertas | splits de la ronda | cargas por insert | reescritos | fusiones | refrescados |')
            print('|---|---|---|---|---|---|---|---|---|---|---|---|')
            for r in churn['rounds']:
                c, w, ins = r['counts'], r['work'], r['inserts']
                print('| %d | %s | %s | %s | %.1f | %.1f | %.3f | %s | %.1f | %s | %s | %s |' % (
                    r['round'], num(r['deleted']), num(r['inserted']), num(c['postings']),
                    c['liveMembers'] / max(c['postings'], 1), c['members'] / max(c['postings'], 1),
                    c['deadEdges'] / max(c['edges'], 1), num(ins['splits']),
                    ins['searchLoads'] / max(ins['inserted'], 1), num(w['postingsRewritten']), num(w['merges']),
                    num(r['refresh']['refreshed'])))
            print()
            print('- **Reescritos y fusiones** son los acumulados; **refrescados**, los de la ronda.')
        print()
        stage_tables(churn)


if __name__ == '__main__':
    main()
