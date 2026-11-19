#!/usr/bin/env python3
"""v6.0 (bench-report.md, "v6.0: deletes en memoria"), from the file
run_v60.sh leaves: every point of deletes with its four cells, each on top of
the one before, next to a batch index of the vectors that are left, and the
churn with the best cell. Counts: nothing is timed.

usage: deletes_verdict.py <dir of the run> [<dir of the second run>] > deletes-verdict.md

With a second run (the amendments 1 to 3): the cell 1b, in which a deleted
member is still a source of routing; the fill references, batch indexes of
the vectors that are left built with the mean live size of the postings of a
cell, which tell a failure of fill from one of edges; and what the cells 1 to
4 differ between the two runs, which is the noise between runs. The verdict
of the original criterion for the cells 1 to 4 comes from the first run; that
of the cell 1b and the fill reading, from the second.

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
ORDER = ('random30', 'random10', 'random50', 'clustered30', 'churn')
CELLS = {
    '1': '1, borrado lógico',
    '1b': '1b, borrado lógico con los borrados como fuentes',
    '2': '2, endpoints borrados sin puntuar',
    '3': '3, más limpieza física',
    '4': '4, más refresco',
}


def key(cell):
    return '%d%s' % (cell['cell'], cell.get('variant', ''))


def label(cell):
    return CELLS[key(cell)]


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
            print('| %s | sin medir: %s | | | | | | | |' % (label(cell), cell['coverage']))
            continue
        conds = conditions(cell)
        crossing = '%d y %d' % tuple(cell['crossing99']) if cell['reached99'] else '—'
        print('| %s | %s | %s | %s | %s | %s | %s | %s | %s |' % (
            label(cell), conds[0][0], mark(conds[0][1], conds[0][2]), conds[1][0], mark(conds[1][1], conds[1][2]),
            conds[2][0], mark(conds[2][1], conds[2][2]), crossing, '**sí**' if cell['verdict']['meets'] else 'no'))
    print()

    print('Recall y cargas a igual ef:')
    print()
    measured = [c for c in cells if c.get('points')]
    print('| ef | referencia: recall | cargas | %s |' % ' | '.join(
        'celda %s: recall | cargas' % key(c) for c in measured))
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
            label(cell), num(c['postings']), c['liveMembers'] / max(c['postings'], 1),
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


def fill_reading(cell):
    """What the fill reference says of a cell, as the amendment 2 reads it."""
    v, f = cell['verdict'], cell.get('fill')
    if v['meets']:
        return 'cumple el criterio: no hay fallo que leer'
    if v['worstRecallDifference'] < -SLACK:
        return 'falla el recall a igual ef: la lectura no aplica'
    if f is None:
        return 'sin referencia de llenado'
    if not (cell['reached95'] and cell['reached99']):
        return 'no llega a 0.99 en la malla: no hay cargas que separar'
    if f['loadsWithin']:
        return '**fallo de llenado:** de la política de fusión, no de las aristas'
    return 'fuera también contra su referencia: el fallo no es solo de llenado'


def over(ratio, bar, floor):
    """A ratio of loads over a reference, with whether it is within its bar."""
    if not ratio:
        return 'no llega', 'no'
    return percent(ratio), mark(ratio <= bar, near(ratio, bar, floor))


def fill_tables(stage, target):
    """The fill references of a point and every cell against its own."""
    ref = stage['reference']
    fills = {f['targetSize']: f for f in stage.get('fills') or [] if f.get('points')}
    if not fills or not ref.get('points'):
        print('Sin referencias de llenado en este punto.')
        print()
        return
    print('| referencia en lote | postings | miembros por posting | cargas a 0.95 | sobre la de S = %d | cargas a 0.99 | '
          'sobre la de S = %d |' % (target, target))
    print('|---|---|---|---|---|---|---|')
    print('| S = %d, la del criterio | %s | %.1f | %.2f | | %s | |' % (
        target, num(ref['postings']), ref['sizeMean'], ref['loads95'],
        '%.2f' % ref['loads99'] if ref['reached99'] else 'no llega'))
    for size in sorted(fills):
        f = fills[size]
        both = ref['reached99'] and f['reached99']
        print('| S = %d, de llenado | %s | %.1f | %.2f | %s | %s | %s |' % (
            size, num(f['postings']), f['sizeMean'], f['loads95'], percent(f['loads95'] / ref['loads95']),
            '%.2f' % f['loads99'] if f['reached99'] else 'no llega',
            percent(f['loads99'] / ref['loads99']) if both else '—'))
    print()
    print('| celda | peor recall a igual ef contra S = %d | cargas contra S = %d: a 0.95 | a 0.99 | su referencia | '
          'contra ella: a 0.95 | dentro | a 0.99 | dentro | peor recall a igual ef contra ella | lectura |' % (target, target))
    print('|---|---|---|---|---|---|---|---|---|---|---|')
    for cell in stage['cells']:
        f = cell.get('fill')
        if not cell.get('points') or f is None:
            continue
        v = cell['verdict']
        a95, in95 = over(f['loads95OverReference'], LOADS95, NOISE95)
        a99, in99 = over(f['loads99OverReference'], LOADS99, NOISE99)
        print('| %s | %s | %s | %s | S = %d | %s | %s | %s | %s | %s | %s |' % (
            label(cell), signed(v['worstRecallDifference']), over(v['loads95OverReference'], LOADS95, NOISE95)[0],
            over(v['loads99OverReference'], LOADS99, NOISE99)[0], f['targetSize'], a95, in95, a99, in99,
            signed(f['worstRecallDifference']), fill_reading(cell)))
    print()


def source_tables(stages):
    """The cell 1b at every point, next to the cells 1 and 2 of its run."""
    print('| punto | celda | peor recall a igual ef | cumple | cargas a 0.95 | cumple | cargas a 0.99 | cumple | las tres |')
    print('|---|---|---|---|---|---|---|---|---|')
    for name in ORDER:
        stage = stages.get(name)
        if stage is None or name == 'churn':
            continue
        for cell in stage['cells']:
            if key(cell) not in ('1', '1b', '2') or not cell.get('points'):
                continue
            conds = conditions(cell)
            print('| %s | %s | %s | %s | %s | %s | %s | %s | %s |' % (
                STAGES[name], key(cell), conds[0][0], mark(conds[0][1], conds[0][2]), conds[1][0],
                mark(conds[1][1], conds[1][2]), conds[2][0], mark(conds[2][1], conds[2][2]),
                '**sí**' if cell['verdict']['meets'] else 'no'))
    print()
    print('El trabajo de una búsqueda a igual ef, en ef 160: un borrado que la celda 1b puntúa cuenta como un vector '
          'puntuado más en el scan.')
    print()
    print('| punto | celda | recall | cargas | vectores puntuados en el scan | endpoints puntuados |')
    print('|---|---|---|---|---|---|')
    for name in ORDER:
        stage = stages.get(name)
        if stage is None or name == 'churn':
            continue
        for cell in stage['cells']:
            if key(cell) not in ('1', '1b', '2') or not cell.get('points'):
                continue
            p = next((p for p in cell['points'] if p['ef'] == 160), None)
            if p is None:
                continue
            print('| %s | %s | %.4f | %.2f | %s | %s |' % (
                STAGES[name], key(cell), p['recall'], p['loads'], num(p['scanned']), num(p['endpoints'])))
    print()


def twins(first, second):
    """The points and cells both runs measured: (stage, name, first, second)."""
    s1 = {s['name']: s for s in first['stages']}
    s2 = {s['name']: s for s in second['stages']}
    for name in ORDER:
        if name not in s1 or name not in s2:
            continue
        a, b = s1[name], s2[name]
        if a['reference'].get('points') and b['reference'].get('points'):
            yield name, 'referencia', a['reference'], b['reference']
        for cell in a['cells']:
            if cell.get('variant') or not cell.get('points'):
                continue
            twin = next((c for c in b['cells'] if key(c) == key(cell) and c.get('points')), None)
            if twin is not None:
                yield name, key(cell), cell, twin


def noise_tables(first, second):
    """What the two runs differ where they measured the same."""
    print('| punto | índice | cargas a 0.95: primera | segunda | diferencia | cargas a 0.99: primera | segunda | '
          'diferencia |')
    print('|---|---|---|---|---|---|---|---|')
    worst95, worst99 = 0.0, 0.0
    for name, who, a, b in twins(first, second):
        d95 = b['loads95'] / a['loads95'] if a['reached95'] and b['reached95'] else 0
        d99 = b['loads99'] / a['loads99'] if a['reached99'] and b['reached99'] else 0
        if name != 'churn':
            worst95 = max(worst95, abs(d95 - 1) if d95 else 0)
            worst99 = max(worst99, abs(d99 - 1) if d99 else 0)
        print('| %s | %s | %s | %s | %s | %s | %s | %s |' % (
            STAGES[name], who, '%.2f' % a['loads95'] if a['reached95'] else 'no llega',
            '%.2f' % b['loads95'] if b['reached95'] else 'no llega', percent(d95) if d95 else '—',
            '%.2f' % a['loads99'] if a['reached99'] else 'no llega',
            '%.2f' % b['loads99'] if b['reached99'] else 'no llega', percent(d99) if d99 else '—'))
    print()
    print('- **La mayor diferencia entre las dos corridas,** sin el churn: %.1f %% a 0.95 y %.1f %% a 0.99. El suelo de '
          'ruido de v2.0, que es el que marca las condiciones: %.1f %% y %.1f %%.' % (
              100 * worst95, 100 * worst99, 100 * NOISE95, 100 * NOISE99))
    print()
    print('Las cantidades del criterio, cada celda contra la referencia de S de su corrida:')
    print()
    print('| punto | celda | peor recall a igual ef: primera | segunda | cargas a 0.95 sobre la referencia: primera | '
          'segunda | a 0.99: primera | segunda | las tres: primera | segunda |')
    print('|---|---|---|---|---|---|---|---|---|---|')
    changed = []
    for name, who, a, b in twins(first, second):
        if who == 'referencia':
            continue
        va, vb = a['verdict'], b['verdict']
        if va['meets'] != vb['meets']:
            changed.append('la celda %s con %s' % (who, STAGES[name]))
        print('| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |' % (
            STAGES[name], who, signed(va['worstRecallDifference']), signed(vb['worstRecallDifference']),
            over(va['loads95OverReference'], LOADS95, NOISE95)[0], over(vb['loads95OverReference'], LOADS95, NOISE95)[0],
            over(va['loads99OverReference'], LOADS99, NOISE99)[0], over(vb['loads99OverReference'], LOADS99, NOISE99)[0],
            'sí' if va['meets'] else 'no', 'sí' if vb['meets'] else 'no'))
    print()
    if changed:
        print('- **El veredicto cambia de una corrida a otra en:** %s. No se distingue del ruido.' % '; '.join(changed))
    else:
        print('- **Ningún veredicto cambia de una corrida a otra.**')
    print()


def load(d):
    path = os.path.join(d, LABEL + '.json')
    if not os.path.exists(path):
        return None
    return json.load(open(path))


def second_run(first, result):
    """The sections of the second run: the amendments 1 to 3."""
    stages = {s['name']: s for s in result['stages']}
    target = result['config']['targetPostingSize']
    print('## La segunda corrida: la celda 1b, las referencias de llenado y el ruido')
    print()
    header(result)
    print('- **De dónde sale cada veredicto:** el del criterio original para las celdas 1 a 4, de la primera corrida; el '
          'de la celda 1b y la lectura de llenado, de esta, donde cada celda tiene sus referencias en la misma corrida.')
    print()

    print('### La celda 1b: un miembro borrado sigue siendo fuente de ruteo')
    print()
    source_tables(stages)
    main_stage = stages.get('random30')
    b = next((c for c in main_stage['cells'] if key(c) == '1b' and c.get('points')), None) if main_stage else None
    if b is None:
        print('**Sin veredicto de la celda 1b: no se midió con el 30 % al azar.**')
    elif b['verdict']['meets']:
        print('**La celda 1b cumple el criterio con el 30 % borrado al azar: es la celda más simple que lo cumple.**')
    else:
        failed = [name for name, cond in zip(('el recall a igual ef', 'las cargas a 0.95', 'las cargas a 0.99'),
                                             conditions(b)) if not cond[1]]
        print('**La celda 1b no cumple el criterio con el 30 %% borrado al azar: falla en %s.**' % ' y en '.join(failed))
    print()

    print('### La lectura de llenado: cada celda contra un lote con el tamaño vivo de sus postings')
    print()
    print('Si una celda cumple el recall a igual ef y falla solo en cargas, y contra su referencia de llenado las cargas '
          'quedan dentro de los dos umbrales (+%.0f %% a 0.95 y +%.0f %% a 0.99), el fallo es de llenado y se atribuye a '
          'la política de fusión, no a las aristas. El criterio original no cambia.' % (
              100 * (LOADS95 - 1), 100 * (LOADS99 - 1)))
    print()
    for name in ORDER:
        stage = stages.get(name)
        if stage is None:
            continue
        print('**%s:**' % (STAGES[name][0].upper() + STAGES[name][1:]))
        print()
        fill_tables(stage, target)

    print('### El ruido entre las dos corridas: las celdas 1 a 4, medidas dos veces')
    print()
    noise_tables(first, result)

    for name in ORDER:
        stage = stages.get(name)
        if stage is None or name == 'churn':
            continue
        print('### Segunda corrida, %s' % STAGES[name])
        print()
        stage_tables(stage)
    churn_tables(result, stages.get('churn'), 'Segunda corrida, churn')


def header(result):
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


def churn_tables(result, churn, title):
    if churn is None:
        return
    print('### %s, con la celda %s' % (title, CELLS.get(str(result.get('best')), '?')))
    print()
    if result.get('bestWhy'):
        print('- **Por qué esa celda:** %s.' % {
            'the simplest cell that meets the criterion with 30 % deleted at random':
                'es la más simple que cumple el criterio al 30 % al azar',
            'given with -bench.deletecell': 'es la que eligió la primera corrida',
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


def main():
    result = load(sys.argv[1])
    if result is None:
        print('La corrida no ha dejado resultados todavía.')
        return
    second = load(sys.argv[2]) if len(sys.argv) > 2 else None
    stages = {s['name']: s for s in result['stages']}

    print('### La corrida')
    print()
    header(result)
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
        plain = [c for c in main_stage['cells'] if not c.get('variant')]
        meeting = [c for c in plain if c.get('points') and c['verdict']['meets']]
        if meeting:
            print('**Los deletes se sostienen: la celda más simple que cumple, de las cuatro, es la %s.**' % label(meeting[0]))
        elif len(plain) == 4 and not main_stage.get('stopped'):
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

    churn_tables(result, stages.get('churn'), 'Churn')
    if second is not None:
        second_run(result, second)


if __name__ == '__main__':
    main()
