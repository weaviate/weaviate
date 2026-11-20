#!/usr/bin/env python3
"""v6.2b (bench-report.md, "v6.2b: churn largo y borrado por regiones"), from
the file run_v62b.sh leaves: the cells B and C of v6.2 over a churn of thirty
rounds, measured after rounds 10, 20 and 30 against the batch index of the
vectors live then, and over 30 % deleted by regions. Counts: nothing is timed.

usage: long_verdict.py <dir of the run> > long-verdict.md

The criterion is that of v6.0, v6.1 and v6.2: within 0.005 of recall of the
reference at equal ef, with loads at 0.95 at +15 % or less and at 0.99 at
+10 % or less, against the batch index with S = 236. The rule is the user's:
B stands if it meets the criterion at the last round measured; if it does not
and C does, the net of searches is mandatory. The noise floor is that of v6.1:
0.0012 of recall, 1.1 % of loads at 0.95 and 3.0 % at 0.99.
"""
import json
import os
import sys

LABEL = 'dbpedia-1m-long'
SLACK, LOADS95, LOADS99 = 0.005, 1.15, 1.10
NOISE_RECALL, NOISE95, NOISE99 = 0.0012, 0.011, 0.03
CELLS = {
    'v61': 'la de v6.1',
    'A': 'A: umbral acumulado',
    'B': 'B: sustitución local',
    'C': 'C: sustitución con red',
}
GROUPS = {
    'all': 'toda la muestra',
    'inserted in the churn': 'insertados en el churn',
    'with a substitute': 'con algún sustituto',
    'repaired by search': 'reparados por búsqueda',
    'neither repaired nor with a substitute': 'ni reparados ni con sustituto',
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


def mark(met, close):
    return ('sí' if met else 'no') + (', a menos del suelo de ruido' if close else '')


def name(cell):
    return CELLS.get(cell.get('label'), cell.get('label') or cell['name'])


def measured(cell):
    return bool(cell.get('points')) and cell.get('verdict', {}).get('worstRecallEF', 0) > 0


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


def criterion_rows(label, stage):
    """The rows of the criterion table for the cells of one measure."""
    for cell in stage['cells']:
        if not measured(cell):
            print('| %s | %s | sin medir: %s | | | | | | |' % (label, name(cell), cell.get('coverage', '')))
            continue
        conds = conditions(cell)
        print('| %s | %s | %s | %s | %s | %s | %s | %s | %s |' % (
            label, name(cell), conds[0][0], mark(conds[0][1], conds[0][2]), conds[1][0], mark(conds[1][1], conds[1][2]),
            conds[2][0], mark(conds[2][1], conds[2][2]), '**sí**' if cell['verdict']['meets'] else 'no'))


def reference_line(stage):
    ref = stage['reference']
    return 'el lote de S = %d de %s vectores vivos, en %s postings: %.2f cargas a 0.95 y %.2f a 0.99' % (
        ref['targetSize'], num(ref['vectors']), num(ref['postings']), ref['loads95'], ref['loads99'])


def per(memory, key, deleted=None):
    return memory[key] / max(deleted or memory['deleted'], 1)


def cost_rows(label, stage):
    for cell in stage['cells']:
        m = cell.get('memory')
        if not m:
            continue
        dead = m['edgesSubstituted'] + m['edgesDropped']
        print('| %s | %s | %.2f | %.1f | %.2f | %.1f | %.1f | %.1f | %s |' % (
            label, name(cell), per(m, 'vectorsRepaired'), per(m, 'searchLoads'), per(m, 'postingsOfSecondPhase'),
            (m['searchRescored'] + m['inverseDistances']) / max(m['deleted'], 1), per(m, 'codeDistances'),
            per(m, 'rehearsalCodeDistances'), '%.1f %%' % (100 * m['edgesSubstituted'] / dead) if dead else '—'))


def edge_row(label, group):
    subs = group.get('substitutes', 0)
    return '| %s | %s | %.4f | %.4f | %.2f | %s | %s |' % (
        label, num(group['vectors']), group['recall'], group['postingRecall'], group['degree'],
        num(subs) if subs else '—',
        '%s, un %.1f %%' % (num(group['substitutesTrue']), 100 * group['substitutesTrue'] / subs) if subs else '—')


def edges_table(stage):
    print('| | vectores | recall de arista | recall de postings | aristas por vector | sustitutos en la lista | de '
          'ellos, aristas verdaderas |')
    print('|---|---|---|---|---|---|---|')
    ref = stage.get('reference', {}).get('edgeRecall')
    if ref:
        print(edge_row('el lote de S = %d' % stage['reference']['targetSize'], ref['all']))
    for cell in stage['cells']:
        edges = (cell.get('memory') or {}).get('edgeRecall')
        if not edges:
            continue
        print(edge_row('**%s**' % name(cell), edges['all']))
        for group in edges.get('groups') or []:
            print(edge_row('&nbsp;&nbsp;%s' % GROUPS.get(group['name'], group['name']), group))
    print()


def last_memories(stage):
    """For every cell of the long churn, what it did up to the last round it was measured after."""
    out = {}
    for cp in stage.get('checkpoints') or []:
        for cell in cp.get('cells') or []:
            if cell.get('memory'):
                out[cell['label']] = (cell, cell['memory'])
    return [out[key] for key in sorted(out)]


def rounds_tables(stage):
    cells = last_memories(stage)
    if not cells:
        return
    print('**Por ronda: aristas por vector vivo después de la pasada, y la fracción de ellas que son sustitutos.**')
    print()
    print('| ronda | ' + ' | '.join('%s: aristas por vector | sustitutos' % name(cell) for cell, _ in cells) + ' |')
    print('|---|' + '---|---|' * len(cells))
    for r in range(max(len(m['rounds']) for _, m in cells)):
        row = []
        for _, m in cells:
            if r >= len(m['rounds']):
                row += ['', '']
                continue
            counts, memory = m['rounds'][r]['counts'], m['passes'][r]['memory']
            edges = max(counts['edges'], 1)
            row += ['%.2f' % (counts['edges'] / max(counts['live'], 1)), '%.1f %%' % (100 * memory['substituteEdges'] / edges)]
        print('| %d | %s |' % (r + 1, ' | '.join(row)))
    print()
    for cell, m in cells:
        print('**%s, por ronda:**' % name(cell))
        print()
        print('| ronda | listas reparadas | cargas | aristas sustituidas | quitadas sin sustituto | postings de la fase 2 | '
              'fusiones | el contador: media por vector vivo | vectores con el contador por encima de cero |')
        print('|---|---|---|---|---|---|---|---|---|')
        for r, (rnd, p) in enumerate(zip(m['rounds'], m['passes'])):
            live = max(rnd['counts']['live'], 1)
            print('| %d | %s | %s | %s | %s | %s | %s | %.3f | %s |' % (
                r + 1, num(p['repair']['refreshed']), num(p['repair']['work']['searchLoads']),
                num(p['substitution']['edgesSubstituted']), num(p['edgesDropped']), num(p['postingsOfSecondPhase']),
                num(p['merges']), p['memory']['edgesLost'] / live, num(p['memory']['vectorsWithEdgesLost'])))
        print()


def rule(stage):
    """The rule of the user, read at the last round measured."""
    checkpoints = [cp for cp in stage.get('checkpoints') or [] if cp.get('cells')]
    if not checkpoints:
        return
    last = checkpoints[-1]
    by = {cell['label']: cell for cell in last['cells'] if measured(cell)}
    b, c = by.get('B'), by.get('C')
    print('### La regla')
    print()
    print('B se sostiene si en la última ronda medida sigue dentro del criterio. Si falla y C cumple, la red de búsqueda '
          'pasa a ser obligatoria. La última medida es la de la %s.' % last['name'].replace('round', 'ronda'))
    print()
    if not b:
        print('**B no tiene medida en esa ronda.**')
    elif b['verdict']['meets']:
        print('**B se sostiene:** cumple las tres cantidades en la %s.' % last['name'].replace('round', 'ronda'))
        if c:
            print('C %s.' % ('también cumple' if c['verdict']['meets'] else 'no cumple'))
    elif c and c['verdict']['meets']:
        print('**B no se sostiene y C cumple: la red de búsqueda pasa a ser obligatoria.**')
    elif c:
        print('**No cumple ninguna de las dos.** La regla no dice nada de este caso.')
    else:
        print('**B no se sostiene,** y C no tiene medida en esa ronda.')
    print()


def main(out):
    path = os.path.join(out, LABEL + '.json')
    if not os.path.exists(path):
        print('Sin datos: no está %s.' % path)
        return
    result = json.load(open(path))
    stages = {stage['name']: stage for stage in result['stages']}
    long, clustered = stages.get('memorylong'), stages.get('memoryclustered')
    print('### Qué corrió')
    print()
    print('- **Dataset:** %s, %s vectores y %s queries, k = %d.' % (
        result['dataset'], num(result['vectors']), num(result['queries']), result['k']))
    print('- **Umbral de split** %d, **ef de inserción** %d, y la fusión de la ganadora de v6.1.' % (
        result['threshold'], result['insertEF']))
    if long:
        rounds = len(last_memories(long)[0][1]['rounds']) if last_memories(long) else 0
        print('- **El churn largo:** una base de %s vectores; %s borrados y %s insertados en las rondas previstas; %d '
              'rondas hechas por la celda que más lleva; %.0f minutos.%s' % (
                  num(long['corpus']), num(long['deleted']), num(long['inserted']), rounds, long['seconds'] / 60,
                  ' **STOP: %s**' % long['stopped'] if long.get('stopped') else ''))
    else:
        print('- **El churn largo:** no corrió.')
    if clustered:
        print('- **El borrado por regiones:** %s vectores de partida, %s borrados en %d regiones, %s postings; %.0f '
              'minutos.%s' % (num(clustered['corpus']), num(clustered['deleted']), clustered.get('regions', 0),
                              num(clustered.get('regionPostings', 0)), clustered['seconds'] / 60,
                              ' **STOP: %s**' % clustered['stopped'] if clustered.get('stopped') else ''))
    else:
        print('- **El borrado por regiones:** no corrió.')
    if result.get('notRun'):
        print('- **Sin correr,** por la hora: %s.' % ', '.join(result['notRun']))
    print()

    if long and any(cp.get('cells') for cp in long.get('checkpoints') or []):
        print('### El churn largo: el criterio en cada ronda medida')
        print()
        for cp in long['checkpoints']:
            if cp.get('reference', {}).get('points'):
                print('- **%s:** %s.' % (cp['name'].replace('round', 'Ronda'), reference_line(cp)))
        print()
        print('| ronda | celda | peor recall a igual ef | entra | cargas a 0.95 | entra | cargas a 0.99 | entra | las tres |')
        print('|---|---|---|---|---|---|---|---|---|')
        for cp in long['checkpoints']:
            if cp.get('cells') and cp.get('reference', {}).get('points'):
                criterion_rows(cp['name'].replace('round ', ''), cp)
        print()
        rule(long)
        print('### El churn largo: lo que cuesta, por vector borrado hasta cada ronda medida')
        print()
        print('| ronda | celda | listas reparadas por búsqueda | cargas de búsqueda | postings de la fase 2 | distancias '
              'exactas | distancias entre códigos | del ensayo, aparte | aristas muertas con sustituto |')
        print('|---|---|---|---|---|---|---|---|---|')
        for cp in long['checkpoints']:
            if cp.get('cells'):
                cost_rows(cp['name'].replace('round ', ''), cp)
        print()
        print('### El churn largo, ronda a ronda')
        print()
        rounds_tables(long)
        print('### El churn largo: recall de arista en cada ronda medida')
        print()
        for cp in long['checkpoints']:
            if not cp.get('cells'):
                continue
            print('**%s:**' % cp['name'].replace('round', 'Ronda'))
            print()
            edges_table(cp)

    if clustered and clustered.get('cells'):
        print('### El 30 % borrado por regiones')
        print()
        if clustered.get('reference', {}).get('points'):
            print('Contra %s.' % reference_line(clustered))
            print()
            print('| | celda | peor recall a igual ef | entra | cargas a 0.95 | entra | cargas a 0.99 | entra | las tres |')
            print('|---|---|---|---|---|---|---|---|---|')
            criterion_rows('por regiones', clustered)
            print()
        print('| | celda | listas reparadas por búsqueda | cargas de búsqueda | postings de la fase 2 | distancias exactas '
              '| distancias entre códigos | del ensayo, aparte | aristas muertas con sustituto |')
        print('|---|---|---|---|---|---|---|---|---|')
        cost_rows('por borrado', clustered)
        print()
        print('| celda | vivos | postings | vivos por posting | listas reparadas | aristas sustituidas | quitadas sin '
              'sustituto | fusiones | aristas por vector vivo |')
        print('|---|---|---|---|---|---|---|---|---|')
        for cell in clustered['cells']:
            m, counts = cell.get('memory'), cell['counts']
            if not m:
                continue
            print('| %s | %s | %s | %.1f | %s | %s | %s | %s | %.2f |' % (
                name(cell), num(counts['live']), num(counts['postings']), counts['liveMembers'] / max(counts['postings'], 1),
                num(m['vectorsRepaired']), num(m['edgesSubstituted']), num(m['edgesDropped']), num(m['merges']),
                counts['edges'] / max(counts['live'], 1)))
        print()
        print('**Recall de arista:**')
        print()
        edges_table(clustered)


if __name__ == '__main__':
    main(sys.argv[1])
