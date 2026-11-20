#!/usr/bin/env python3
"""v6.2 (bench-report.md, "v6.2: reparación en el churn"), from the file
run_v62.sh leaves: the maintenance pass of v6.1 with three repairs in the
churn, A (a count of the edges a list has lost), B (a local substitute by a
distance between two codes) and C (both), next to the pass of v6.1 as it was;
and B and C with 30 % deleted at once, next to the winner of v6.1. Counts:
nothing is timed.

usage: memory_verdict.py <dir of the run> > memory-verdict.md

The criterion is that of v6.0 and v6.1, read in the churn: a cell meets it if
it is within 0.005 of recall of its reference at equal ef, with loads at 0.95
at +15 % or less and at 0.99 at +10 % or less, against the batch index with
S = 236 of the vectors that are live when the rounds end. The cheapest cell
that meets it, among A, B and C, is the one with the fewest search loads a
deleted vector; under 10 % between two, the one with the fewest exact
distances. The noise floor is that of v6.1: 0.0012 of recall, 1.1 % of loads
at 0.95 and 3.0 % at 0.99.
"""
import json
import os
import sys

LABEL = 'dbpedia-1m-memory'
SLACK, LOADS95, LOADS99 = 0.005, 1.15, 1.10
NOISE_RECALL, NOISE95, NOISE99 = 0.0012, 0.011, 0.03
TIE = 0.10
CELLS = {
    'v61': 'la de v6.1, repetida (mía)',
    'A': 'A: umbral acumulado',
    'B': 'B: sustitución local',
    'C': 'C: sustitución con red',
}
STAGES = (('memorychurn', 'El churn: diez rondas del 3 %'), ('memory30', 'El 30 % de una vez'))
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


def measured(cell):
    return bool(cell.get('points')) and 'verdict' in cell and cell['verdict'].get('worstRecallEF', 0) > 0


def criterion_table(stage):
    ref = stage['reference']
    print('**Contra el lote de S = %d de los %s vectores vivos:** %s postings; cargas a 0.95 %.2f y a 0.99 %.2f.' % (
        ref['targetSize'], num(ref['vectors']), num(ref['postings']), ref['loads95'], ref['loads99']))
    print()
    print('| celda | peor recall a igual ef | entra | cargas a 0.95 | entra | cargas a 0.99 | entra | las tres |')
    print('|---|---|---|---|---|---|---|---|')
    for cell in stage['cells']:
        if not measured(cell):
            print('| %s | sin medir: %s | | | | | | |' % (name(cell), cell['coverage']))
            continue
        conds = conditions(cell)
        print('| %s | %s | %s | %s | %s | %s | %s | %s |' % (
            name(cell), conds[0][0], mark(conds[0][1], conds[0][2]), conds[1][0], mark(conds[1][1], conds[1][2]),
            conds[2][0], mark(conds[2][1], conds[2][2]), '**sí**' if cell['verdict']['meets'] else 'no'))
    print()


def fill_table(stage):
    cells = [cell for cell in stage['cells'] if cell.get('fill')]
    if not cells:
        return
    print('**Contra su referencia de llenado,** el lote con el tamaño medio de posting que deja cada celda. Sin veredicto.')
    print()
    print('| celda | postings | vivos por posting | lote de | peor recall a igual ef | cargas a 0.95 | cargas a 0.99 | '
          'cargas dentro |')
    print('|---|---|---|---|---|---|---|---|')
    for cell in cells:
        fill, counts = cell['fill'], cell['counts']
        print('| %s | %s | %.1f | S = %d | %s | %s | %s | %s |' % (
            name(cell), num(counts['postings']), counts['liveMembers'] / max(counts['postings'], 1), fill['targetSize'],
            signed(fill['worstRecallDifference']), percent(fill['loads95OverReference']),
            percent(fill['loads99OverReference']), 'sí' if fill['loadsWithin'] else 'no'))
    print()


def per_deleted(memory, key):
    return memory[key] / max(memory['deleted'], 1)


def exact(memory):
    return memory['searchRescored'] + memory['inverseDistances']


def cost_table(stage):
    print('**Coste por vector borrado,** sobre los %s borrados de la etapa.' % num(stage['deleted']))
    print()
    print('| celda | listas reparadas por búsqueda | cargas de búsqueda | postings leídos y escritos por la fase 2 | '
          'distancias exactas | de ellas, de aristas inversas | distancias entre códigos | del ensayo, aparte |')
    print('|---|---|---|---|---|---|---|---|')
    for cell in stage['cells']:
        m = cell.get('memory')
        if not m:
            continue
        print('| %s | %.2f | %.1f | %.2f | %.1f | %.1f | %.1f | %.1f |' % (
            name(cell), per_deleted(m, 'vectorsRepaired'), per_deleted(m, 'searchLoads'),
            per_deleted(m, 'postingsOfSecondPhase'), exact(m) / max(m['deleted'], 1), per_deleted(m, 'inverseDistances'),
            per_deleted(m, 'codeDistances'), per_deleted(m, 'rehearsalCodeDistances')))
    print()
    print('| celda | listas reparadas | cargas | por reparación | aristas sustituidas | quitadas sin sustituto | '
          'con sustituto, de las muertas | fusiones |')
    print('|---|---|---|---|---|---|---|---|')
    for cell in stage['cells']:
        m = cell.get('memory')
        if not m:
            continue
        dead = m['edgesSubstituted'] + m['edgesDropped']
        print('| %s | %s | %s | %s | %s | %s | %s | %s |' % (
            name(cell), num(m['vectorsRepaired']), num(m['searchLoads']),
            '%.1f' % (m['searchLoads'] / m['vectorsRepaired']) if m['vectorsRepaired'] else '—',
            num(m['edgesSubstituted']), num(m['edgesDropped']),
            '%.1f %%' % (100 * m['edgesSubstituted'] / dead) if dead else '—', num(m['merges'])))
    print()
    print('- **Las aristas muertas que una reparación quita de la lista que construye no están en ninguna de las dos '
          'columnas:** se cuentan las que llegan a la fase 2.')
    print()


def rounds_table(stage):
    cells = [cell for cell in stage['cells'] if cell.get('memory') and cell['memory'].get('rounds')]
    if not cells:
        return
    print('**Aristas por vector vivo después de la pasada de cada ronda,** y entre paréntesis cuántas son sustitutos.')
    print()
    print('| ronda | ' + ' | '.join(name(cell) for cell in cells) + ' |')
    print('|---|' + '---|' * len(cells))
    for r in range(max(len(cell['memory']['rounds']) for cell in cells)):
        row = []
        for cell in cells:
            m = cell['memory']
            if r >= len(m['rounds']):
                row.append('')
                continue
            counts, memory = m['rounds'][r]['counts'], m['passes'][r]['memory']
            live = max(counts['live'], 1)
            text = '%.2f' % (counts['edges'] / live)
            if memory['substituteEdges']:
                text += ' (%.2f)' % (memory['substituteEdges'] / live)
            row.append(text)
        print('| %d | %s |' % (r + 1, ' | '.join(row)))
    print()
    for cell in cells:
        m = cell['memory']
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


def edge_row(label, group):
    subs = group.get('substitutes', 0)
    return '| %s | %s | %.4f | %.4f | %.2f | %s | %s |' % (
        label, num(group['vectors']), group['recall'], group['postingRecall'], group['degree'],
        num(subs) if subs else '—',
        '%s, un %.1f %%' % (num(group['substitutesTrue']), 100 * group['substitutesTrue'] / subs) if subs else '—')


def edges_table(stage):
    print('**Recall de arista al final,** el de v2.x, sobre una muestra de vectores vivos, la misma en todas las celdas '
          'y en el lote.')
    print()
    print('| | vectores | recall de arista | recall de postings | aristas por vector | sustitutos en la lista | de '
          'ellos, aristas verdaderas |')
    print('|---|---|---|---|---|---|---|')
    ref = stage['reference'].get('edgeRecall')
    if ref:
        print(edge_row('el lote de S = %d' % stage['reference']['targetSize'], ref['all']))
    for fill in stage.get('fills') or []:
        if fill.get('edgeRecall'):
            print(edge_row('el lote de S = %d, de llenado' % fill['targetSize'], fill['edgeRecall']['all']))
    for cell in stage['cells']:
        edges = (cell.get('memory') or {}).get('edgeRecall')
        if not edges:
            continue
        print(edge_row('**%s**' % name(cell), edges['all']))
        for group in edges.get('groups') or []:
            print(edge_row('&nbsp;&nbsp;%s' % GROUPS.get(group['name'], group['name']), group))
    print()


def cheapest(stage):
    """The cheapest cell that meets the criterion in the churn, among A, B and C."""
    cells = [cell for cell in stage['cells'] if cell.get('label') in ('A', 'B', 'C') and measured(cell)]
    met = [cell for cell in cells if cell['verdict']['meets']]
    print('### La celda más barata que cumple')
    print()
    if not met:
        reached = [cell for cell in cells if cell['reached99'] and cell['verdict']['loads99OverReference']]
        print('**Ninguna de las tres cumple en el churn.**')
        if reached:
            best = min(reached, key=lambda cell: cell['verdict']['loads99OverReference'])
            print('La de menos cargas a 0.99 sobre su referencia es %s: %s.' % (
                name(best), percent(best['verdict']['loads99OverReference'])))
        print()
        return None
    met.sort(key=lambda cell: per_deleted(cell['memory'], 'searchLoads'))
    best = met[0]
    tied = [cell for cell in met[1:]
            if per_deleted(cell['memory'], 'searchLoads') <= per_deleted(best['memory'], 'searchLoads') * (1 + TIE)
            or per_deleted(cell['memory'], 'searchLoads') - per_deleted(best['memory'], 'searchLoads') < 0.05]
    if tied:
        pool = [best] + tied
        best = min(pool, key=lambda cell: exact(cell['memory']))
        print('**%s,** por distancias exactas: %s quedan a menos de un 10 %% en cargas de búsqueda.' % (
            name(best), ' y '.join(name(cell) for cell in pool)))
    else:
        print('**%s.**' % name(best))
    print()
    print('| celda | cumple en el churn | cargas de búsqueda por borrado | distancias exactas por borrado |')
    print('|---|---|---|---|')
    for cell in cells:
        m = cell['memory']
        print('| %s | %s | %.1f | %.1f |' % (name(cell), 'sí' if cell['verdict']['meets'] else 'no',
                                             per_deleted(m, 'searchLoads'), exact(m) / max(m['deleted'], 1)))
    print()
    return best


def main(out):
    path = os.path.join(out, LABEL + '.json')
    if not os.path.exists(path):
        print('Sin datos: no está %s.' % path)
        return
    result = json.load(open(path))
    stages = {stage['name']: stage for stage in result['stages']}
    print('### Qué corrió')
    print()
    print('- **Dataset:** %s, %s vectores y %s queries, k = %d.' % (
        result['dataset'], num(result['vectors']), num(result['queries']), result['k']))
    print('- **Umbral de split** %d, **ef de inserción** %d, y la fusión de la ganadora de v6.1.' % (
        result['threshold'], result['insertEF']))
    for key, title in STAGES:
        stage = stages.get(key)
        if not stage:
            print('- **%s:** no corrió.' % title)
            continue
        print('- **%s:** %s vectores de partida, %s borrados%s, en %.0f minutos.%s' % (
            title, num(stage['corpus']), num(stage['deleted']),
            ' y %s insertados' % num(stage['inserted']) if stage.get('inserted') else '', stage['seconds'] / 60,
            ' **STOP: %s**' % stage['stopped'] if stage.get('stopped') else ''))
    if result.get('notRun'):
        print('- **Sin correr,** por la hora: %s.' % ', '.join(result['notRun']))
    print()
    best = None
    for key, title in STAGES:
        stage = stages.get(key)
        if not stage or not stage.get('cells'):
            continue
        print('### %s' % title)
        print()
        if stage.get('reference', {}).get('points'):
            criterion_table(stage)
            fill_table(stage)
        else:
            print('Sin referencia: la etapa no llegó a construirla.')
            print()
        cost_table(stage)
        rounds_table(stage)
        edges_table(stage)
        if key == 'memorychurn' and stage.get('reference', {}).get('points'):
            best = cheapest(stage)
    churn, at_once = stages.get('memorychurn'), stages.get('memory30')
    if churn and at_once and churn.get('reference', {}).get('points') and at_once.get('reference', {}).get('points'):
        print('### El churn y el 30 % de una vez, celda a celda')
        print()
        print('| celda | cumple en el churn | entra con el 30 % de una vez |')
        print('|---|---|---|')
        by_label = {cell.get('label'): cell for cell in at_once['cells']}
        for cell in churn['cells']:
            other = by_label.get(cell.get('label'))
            if cell.get('label') == 'A':
                other = by_label.get('v61')
            print('| %s | %s | %s |' % (
                name(cell), 'sí' if measured(cell) and cell['verdict']['meets'] else 'no',
                '—' if not other or not measured(other) else (
                    ('sí' if other['verdict']['meets'] else 'no') + (
                        ': es la ganadora de v6.1, con los contadores a cero' if cell.get('label') == 'A' else ''))))
        print()
    if best is not None:
        print('**Veredicto:** %s.' % name(best))


if __name__ == '__main__':
    main(sys.argv[1])
