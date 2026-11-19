#!/usr/bin/env python3
"""v4.0, the seventh amendment (bench-report.md, "HFresh como en un nodo,
junto a las dos celdas"), from the files run_v40_node.sh leaves: the HFresh
index of v4.0 with its centroid graph cleaned, at its best configuration for
0.95 and for 0.99, its postings read one by one and 8 at a time, and the
cells of 512 bits in memory and of the halo of the posting-edge index. Every
query of the six curves ran with the budget the admission limiter of a node
granted it.

usage: node_verdict.py <dir of the session> <dir of v4.0> > node-verdict.md

The criteria of v4.0 are read again with these numbers: the cell of 512 bits
has less latency than HFresh, cold, at 0.95, and reaches 0.99; and the halo
is not slower. The p50 with 1 thread, first round (n1); the second round
(n2) tells drift. Under 10 % between two curves is a tie. What v4.0
published is another session, with the graph not cleaned and a budget of 8:
it is put next to these, never mixed with them.
"""
import csv
import json
import os
import sys

LABEL = 'dbpedia-1m-node'
OLD = 'dbpedia-1m-disk'
ROUNDS = ('n1', 'n2')
DRIFT = 0.10
TIE = 0.10
TARGETS = (0.95, 0.99)
CURVES = (
    ('hfresh-best95', 'HFresh, postings uno a uno', 0.95),
    ('hfresh-best95-par', 'HFresh, postings de 8 en 8', 0.95),
    ('hfresh-best99', 'HFresh, postings uno a uno', 0.99),
    ('hfresh-best99-par', 'HFresh, postings de 8 en 8', 0.99),
    ('r512', 'posting-edge, 512 bits en RAM', None),
    ('halo512', 'posting-edge, halo de 512 bits', None),
)
CELLS = (('r512', 'posting-edge, 512 bits en RAM'), ('halo512', 'posting-edge, halo de 512 bits'))
# What v4.0 published of every curve: the first of these rounds that has it.
PUBLISHED = {
    'hfresh-best95': ('p1', 'r1'), 'hfresh-best95-par': ('p1',), 'hfresh-best99': ('p1', 'r1'),
    'hfresh-best99-par': ('p1',), 'r512': ('p1', 'r1'), 'halo512': ('p1', 'r1'),
}
HF_PARTS = (('centroides', 'centroidms'), ('leer los postings', 'readms'), ('scan', 'scanms'), ('rescore', 'rescorems'))


def load(d, name):
    path = os.path.join(d, name)
    return json.load(open(path)) if os.path.exists(path) else None


def ms(x):
    return '—' if x is None else '%.2f' % x


def num(x):
    s = '%.0f' % x
    return s if len(s) < 4 else s[:-3] + ' ' + s[-3:]


def mb(x):
    return '%.2f MB' % (x / 1e6)


def times(x):
    return '—' if x is None else '%.2fx' % x


def signed(rel):
    return '%s%.1f %%' % ('+' if rel >= 0 else '−', abs(100 * rel))


def middle(passes):
    if not passes:
        return None
    return sorted(passes, key=lambda p: p['qps'])[len(passes) // 2]


def point(result, target, threads):
    for p in (result or {}).get('points', []):
        if abs(p['target'] - target) < 1e-9 and p['threads'] == threads:
            return p
    return None


def measure(result, target, threads, cold):
    """The latencies of a curve at a point: the cold ones, or the middle warm pass."""
    p = point(result, target, threads)
    if p is None:
        return None
    return p.get('cold') if cold else middle(p.get('warm'))


def p50(m):
    return None if m is None else m['p50ms']


def over(a, b):
    if a is None or b is None or not b:
        return None
    return a / b


def best(reads, manifest, target):
    """The configuration of HFresh that reads the least for a recall, with what it reads."""
    if reads is None:
        return None
    name = reads['best'].get('%.2f' % target)
    return next((c for c in reads['counts'] if c['name'] == name), None)


def main():
    d, v40 = sys.argv[1], sys.argv[2]
    manifest = load(d, LABEL + '-hfresh-manifest.json')
    reads = load(d, LABEL + '-hfresh-reads.json')
    if manifest is None:
        print('No hay manifiesto del índice limpio: la preparación sin cronometrar no ha corrido.')
        return
    old_manifest = load(v40, OLD + '-hfresh-manifest.json')
    old_reads = load(v40, OLD + '-hfresh-reads.json')
    cells = {c['spec']['name']: c for c in (load(v40, OLD + '-manifest.json') or {}).get('cells', [])}
    results = {(r, c): load(d, '%s-%s-%s.json' % (LABEL, r, c)) for r in ROUNDS for c, _, _ in CURVES}
    published = {}
    for curve, rounds in PUBLISHED.items():
        for r in rounds:
            published[curve] = load(v40, '%s-%s-%s.json' % (OLD, r, curve))
            if published[curve] is not None:
                break

    print('### El grafo de centroides de HFresh y su mejor configuración, sin cronometrar')
    print()
    cleaned = manifest.get('cleaned')
    if cleaned:
        before, after = cleaned['before'], cleaned['after']
        print('- **El grafo, antes de limpiar:** %s nodos, %s de ellos tombstones, para %s postings vivos.' % (
            num(before['nodes']), num(before['tombstones']), num(before['postings'])))
        print('- **Limpio:** %s nodos y %s tombstones, en %d ronda%s de limpieza y %.1f s.' % (
            num(after['nodes']), num(after['tombstones']), cleaned['rounds'], '' if cleaned['rounds'] == 1 else 's',
            cleaned['seconds']))
    if old_manifest:
        print('- **Por defecto** (searchProbe %d, rescoreLimit %d): recall %.4f sin limpiar y %.4f limpio.' % (
            manifest['default']['searchProbe'], manifest['default']['rescoreLimit'], old_manifest['default']['recall'],
            manifest['default']['recall']))
        print('- **El mejor recall del barrido,** con el rescoreLimit por defecto: %.4f sin limpiar y %.4f limpio.' % (
            old_manifest['maxRecall'], manifest['maxRecall']))
    print()
    print('La configuración que menos lee para cada recall:')
    print()
    print('| recall | índice | rescoreLimit | searchProbe | recall medido | lecturas de postings | lecturas de rescore | '
          'lecturas por query | bytes por query |')
    print('|---|---|---|---|---|---|---|---|---|')
    for target in TARGETS:
        for label, which in (('sin limpiar, v4.0', best(old_reads, old_manifest, target)),
                             ('limpio', best(reads, manifest, target))):
            if which is None:
                continue
            print('| %.2f | %s | %d | %d | %.4f | %.1f | %.1f | %.1f | %s |' % (
                target, label, which['rescoreLimit'], which['searchProbe'], which['recall'], which['postingReads'],
                which['rescoreReads'], which['reads'], mb(which['postingBytes'] + which['rescoreBytes'])))
    print()
    if cells:
        print('Lo que leen las dos celdas, del manifiesto de v4.0:')
        print()
        print('| recall | celda | ef | cargas | lecturas de bloques | lecturas de rescore | lecturas por query | '
              'bytes por query |')
        print('|---|---|---|---|---|---|---|---|')
        dims = (load(v40, OLD + '-manifest.json') or {}).get('dims', 0)
        for target in TARGETS:
            for name, label in CELLS:
                cell = cells.get(name)
                p = next((p for p in (cell or {}).get('points', []) if abs(p['target'] - target) < 1e-9), None)
                if p is None or not p['reached']:
                    continue
                print('| %.2f | %s | %d | %.2f | %.1f | %.1f | %.1f | %s |' % (
                    target, label, p['ef'], p['loads'], p['storeReads'], p['rescored'], p['storeReads'] + p['rescored'],
                    mb(p['storeBytes'] + p['rescored'] * 4 * dims)))
        print()

    if not any(results.values()):
        print('La sesión cronometrada no ha corrido todavía.')
        return

    granted = {}
    for threads in (1, 8):
        p = point(results[('n1', 'hfresh-best95')], 0.95, threads)
        if p is not None and p.get('granted'):
            granted[threads] = p['granted']
    if granted:
        print('**El presupuesto que dio el limitador:** %.1f por query con 1 thread y %.1f con 8.' % (
            granted.get(1, 0), granted.get(8, 0)))
        print()

    for target in TARGETS:
        for cold, temp in ((True, 'en frío'), (False, 'en caliente')):
            rows = []
            for name, label, only in CURVES:
                if only is not None and abs(only - target) > 1e-9:
                    continue
                one = measure(results[('n1', name)], target, 1, cold)
                eight = measure(results[('n1', name)], target, 8, cold)
                if one is None and eight is None:
                    continue
                p = point(results[('n1', name)], target, 1) or point(results[('n1', name)], target, 8)
                where = 'ef %d' % p['ef'] if 'ef' in p else 'searchProbe %d, rescoreLimit %d' % (
                    p['searchProbe'], p['rescoreLimit'])
                row = '| %s | %s |' % (label, where)
                for m in (one, eight):
                    if m is None:
                        row += ' — | — | — | — |'
                    else:
                        row += ' %s | %s | %s | %s |' % (ms(m['p50ms']), ms(m['p95ms']), ms(m['p99ms']), num(m['qps']))
                rows.append(row)
            if not rows:
                continue
            print('### Recall %.2f, %s, primera ronda' % (target, temp))
            print()
            print('| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |')
            print('|---|---|---|---|---|---|---|---|---|---|')
            print('\n'.join(rows))
            print()

    print('### HFresh entre cada celda: cuántas veces el p50 de la celda')
    print()
    print('| recall | HFresh | celda | en frío, 1 thread | en frío, 8 threads | en caliente, 1 thread | '
          'en caliente, 8 threads |')
    print('|---|---|---|---|---|---|---|')
    for target in TARGETS:
        for hf, hf_label, only in CURVES:
            if only is None or abs(only - target) > 1e-9:
                continue
            for cell, cell_label in CELLS:
                row = '| %.2f | %s | %s |' % (target, hf_label.replace('HFresh, ', ''), cell_label.replace('posting-edge, ', ''))
                any_ = False
                for cold in (True, False):
                    for threads in (1, 8):
                        r = over(p50(measure(results[('n1', hf)], target, threads, cold)),
                                 p50(measure(results[('n1', cell)], target, threads, cold)))
                        any_ = any_ or r is not None
                        row += ' %s |' % times(r)
                if any_:
                    print(row)
    print()

    print('### Los criterios de v4.0, con estos números')
    print()
    serial = p50(measure(results[('n1', 'hfresh-best95')], 0.95, 1, True))
    par = p50(measure(results[('n1', 'hfresh-best95-par')], 0.95, 1, True))
    for name, label in CELLS:
        mine = p50(measure(results[('n1', name)], 0.95, 1, True))
        if mine is None or serial is None:
            print('- **%s:** sin medir en frío.' % label)
            continue
        r = serial / mine
        if abs(r - 1) < TIE:
            what = 'empata con HFresh'
        elif r > 1:
            what = 'tiene menos latencia que HFresh'
        else:
            what = 'tiene más latencia que HFresh'
        line = '- **%s %s, en frío a 0.95 con 1 thread:** %s ms frente a %s, %s.' % (
            label[0].upper() + label[1:], what, ms(mine), ms(serial), times(r))
        if par is not None:
            line += ' Frente a HFresh con sus postings de 8 en 8, que no es producto: %s ms, %s.' % (ms(par), times(par / mine))
        print(line)
        cell = cells.get(name)
        if cell:
            reached = all(p['reached'] for p in cell['points'])
            print('  - %s a 0.99.' % ('Llega' if reached else 'No llega'))
    print()

    print('### Junto a lo publicado en v4.0, que es otra sesión, con el grafo sin limpiar y presupuesto de 8')
    print()
    print('| índice | recall | | publicado: 1 thread, p50 ms | esta sesión | | publicado: 8 threads, p50 ms | esta sesión | |')
    print('|---|---|---|---|---|---|---|---|---|')
    for name, label, only in CURVES:
        for target in TARGETS:
            if only is not None and abs(only - target) > 1e-9:
                continue
            for cold, temp in ((True, 'en frío'), (False, 'en caliente')):
                row = '| %s | %.2f | %s |' % (label, target, temp)
                any_ = False
                for threads in (1, 8):
                    a = p50(measure(published.get(name), target, threads, cold))
                    b = p50(measure(results[('n1', name)], target, threads, cold))
                    any_ = any_ or b is not None
                    rel = '—' if a is None or b is None else signed(b / a - 1)
                    row += ' %s | %s | %s |' % (ms(a), ms(b), rel)
                if any_:
                    print(row)
    print()
    print('- **No es la misma configuración de HFresh** cuando el searchProbe de arriba cambia entre las dos filas.')
    print()

    rows = []
    for name, label, target in CURVES:
        if target is None:
            continue
        for cold, temp in ((True, 'en frío'), (False, 'en caliente')):
            for which, source in (('publicado', published.get(name)), ('esta sesión', results[('n1', name)])):
                m = measure(source, target, 1, cold)
                if m is None:
                    continue
                rows.append('| %s | %.2f | %s | %s | %s | %s |' % (
                    label, target, temp, which, ms(m['meanms']), ' | '.join(ms(m[key]) for _, key in HF_PARTS)))
    if rows:
        print('### Las partes de la latencia media de HFresh, con 1 thread')
        print()
        print('| índice | recall | | | media, ms | %s |' % ' | '.join(name for name, _ in HF_PARTS))
        print('|---|---|---|---|---|%s' % ('---|' * len(HF_PARTS)))
        print('\n'.join(rows))
        print()

    print('### Deriva entre rondas y actividad')
    print()
    drifted, measured = [], 0
    for name, label, only in CURVES:
        for target in TARGETS:
            if only is not None and abs(only - target) > 1e-9:
                continue
            for threads in (1, 8):
                for cold, temp in ((True, 'frío'), (False, 'caliente')):
                    a = p50(measure(results[('n1', name)], target, threads, cold))
                    b = p50(measure(results[('n2', name)], target, threads, cold))
                    if a is None or b is None:
                        continue
                    measured += 1
                    if abs(b / a - 1) > DRIFT:
                        drifted.append('%s, a %.2f con %d thread%s, en %s: %s → %s ms (%s)' % (
                            label, target, threads, '' if threads == 1 else 's', temp, ms(a), ms(b), signed(b / a - 1)))
    if measured == 0:
        print('- **La segunda ronda no ha corrido:** no hay con qué medir la deriva.')
    elif drifted:
        print('- **Medianas que se movieron más de un 10 %% entre rondas** (%d de %d):' % (len(drifted), measured))
        for line in drifted:
            print('  - ' + line)
    else:
        print('- **Ninguna mediana se movió más de un 10 %% entre rondas,** de %d.' % measured)
    stretches = os.path.join(d, LABEL + '-stretches.tsv')
    if os.path.exists(stretches):
        rows = list(csv.DictReader(open(stretches), delimiter='\t'))
        used = [r for r in rows if int(r['idle at end']) >= 0 and int(r['idle at end']) < int(r['seconds'])]
        events = [r for r in rows if int(r['events']) > 0]
        failed = [r for r in rows if r['passed'] != '1']
        if rows:
            print('- **La sesión:** %d tramos, de %s a %s.' % (len(rows), rows[0]['start'], rows[-1]['end']))
        if used:
            print('- **Tramos con teclado o trackpad:** ' + ', '.join('%s %s' % (r['round'], r['curve']) for r in used) + '.')
        else:
            print('- **Ningún tramo tuvo teclado ni trackpad.**')
        if events:
            print('- **Tramos con eventos de energía o de actividad:** ' + ', '.join(
                '%s %s (%s)' % (r['round'], r['curve'], r['events']) for r in events) + '.')
        if failed:
            print('- **Tramos que no pasaron:** ' + ', '.join('%s %s' % (r['round'], r['curve']) for r in failed) + '.')


if __name__ == '__main__':
    main()
