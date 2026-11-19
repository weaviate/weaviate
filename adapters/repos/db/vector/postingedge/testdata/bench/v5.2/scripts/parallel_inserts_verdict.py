#!/usr/bin/env python3
"""v5.2 (bench-report.md, "v5.2: inserts con búsquedas en paralelo"), from the
manifests run_v52.sh leaves, one per cell: the insert of always (n1), 4 and 8
searchers over batches of 32 inserts (n4, n8), and 8 searchers over batches
of 8 (n8w8).

usage: parallel_inserts_verdict.py <dir of the run> > parallel-inserts-verdict.md

The user's criterion: it serves if n8 gives 4 times or more the inserts a
second of n1, with its loads at 0.95 and at 0.99 within 5 % of those of n1.
The loads are those of the smallest ef that reaches each recall, in each
index; loading less does not break it. Two indexes grown apart are not the
same index, because the centroid graph draws its levels: n4 and n8 have the
same batches, and what they differ by is the noise between two runs.
"""
import csv
import json
import os
import sys

LABEL = 'dbpedia-1m-par'
BASE = 'n1'
MAIN = 'n8'
SPEEDUP = 4.0
LOADS = 0.05
CELLS = (
    ('n1', 'N = 1, el insert de siempre'),
    ('n4', 'N = 4, tandas de 32'),
    ('n8', 'N = 8, tandas de 32'),
    ('n8w8', 'N = 8, tandas de 8'),
)
TARGETS = (0.95, 0.99)
PARTS = (('escritura', 'writeNs'), ('aristas inversas', 'inverseNs'), ('splits', 'splitNs'),
         ('filas de aristas', 'rowNs'), ('ciclo del store', 'cycleNs'))


def load(d, name):
    path = os.path.join(d, name)
    return json.load(open(path)) if os.path.exists(path) else None


def num(x):
    """An integer with its thousands apart."""
    s = '%.0f' % x
    groups = []
    while len(s) > 3:
        groups.insert(0, s[-3:])
        s = s[:-3]
    return ' '.join([s] + groups)


def times(x):
    return '—' if x is None else '%.2fx' % x


def signed(rel):
    return '%s%.1f %%' % ('+' if rel >= 0 else '−', abs(100 * rel))


def point(manifest, target):
    for p in (manifest or {}).get('points', []):
        if abs(p['target'] - target) < 1e-9 and p['reached']:
            return p
    return None


def main():
    d = sys.argv[1]
    cells = {name: load(d, '%s-%s-manifest.json' % (LABEL, name)) for name, _ in CELLS}
    have = [(name, label) for name, label in CELLS if cells[name] is not None]
    if not have:
        print('Ninguna celda ha corrido todavía.')
        return
    base = cells[BASE]
    first = cells[have[0][0]]
    print('### Las celdas')
    print()
    print('- **Dataset:** %s, %s vectores. Del %d %% al 100 %%: %s inserts por celda.' % (
        first['dataset'], num(first['vectors']), first['start'], num(first['inserts'])))
    path = 'el de v5.0' if first['consolidate'] == 0 else 'el de v5.1, con K = %d' % first['consolidate']
    print('- **Camino:** %s. Split con T = %d, ef de inserción %d, códigos de ruteo de %d bits en RAM.' % (
        path, first['threshold'], first['insertEF'], first['routingBits']))
    print()

    print('### Cobertura en cada checkpoint, con el reloj parado')
    print()
    stops = [s['percent'] for s in first['stretches']]
    print('| celda | %s | al acabar |' % ' | '.join('%d %%' % s for s in stops))
    print('|---|%s---|' % ('---|' * len(stops)))
    for name, label in have:
        m = cells[name]
        row = ['completa' if s.get('coverage') == 'complete' else (s.get('coverage') or 'sin comprobar')
               for s in m['stretches']]
        row += ['—'] * (len(stops) - len(row))
        final = m.get('coverage') or 'sin acabar'
        print('| %s | %s | %s |' % (label, ' | '.join(row), 'completa' if final == 'complete' else final))
    print()

    print('### Inserts por segundo')
    print()
    print('| celda | %s | toda la corrida | frente a N = 1 |' % ' | '.join('hasta el %d %%' % s for s in stops))
    print('|---|%s---|---|' % ('---|' * len(stops)))
    for name, label in have:
        m = cells[name]
        row = [num(s['perSecond']) for s in m['stretches']]
        row += ['—'] * (len(stops) - len(row))
        done = len(m['stretches']) == len(stops)
        speedup = m['perSecond'] / base['perSecond'] if base and done and len(base['stretches']) == len(stops) else None
        print('| %s | %s | %s | %s |' % (label, ' | '.join(row), num(m['perSecond']) if done else '—', times(speedup)))
    print()
    if cells.get(BASE) and base.get('flushSeconds'):
        print('- **Con aristas consolidadas,** el tiempo lleva el volcado de lo pendiente al final.')
        print()

    print('### Dónde se va el tiempo')
    print()
    print('| celda | el aplicador, del tiempo de reloj | los buscadores, del suyo, buscando | búsqueda por insert, ms | '
          'aplicar un insert, ms | %s |' % ' | '.join(name for name, _ in PARTS))
    print('|---|---|---|---|---|%s' % ('---|' * len(PARTS)))
    for name, label in have:
        m = cells[name]
        s, n = m['stats'], max(m['stats']['inserted'], 1)
        search = (s['assignNs'] + s['searchNs']) / n / 1e6
        apply = s.get('applyPhaseNs', 0) / n / 1e6
        parts = ' | '.join('%.3f' % (s.get(key, 0) / n / 1e6) for _, key in PARTS)
        print('| %s | %.1f %% | %.0f %% | %.2f | %.3f | %s |' % (
            label, 100 * m.get('applyShare', 0), 100 * m.get('searchBusy', 0), search, apply, parts))
    print()
    print('- **El aplicador** es el tiempo de reloj con las escrituras de los inserts, una detrás de otra. Lo demás es '
          'el tiempo de reloj con las búsquedas corriendo.')
    print('- **Los buscadores, buscando:** lo que suman sus búsquedas entre N veces el tiempo de reloj de sus fases. '
          'Lo que falta hasta 100 es espera.')
    print()

    rows = []
    for name, label in have:
        m = cells[name]
        if m['batch'] <= 1 and m['searchers'] <= 1:
            continue
        s = m['stats']
        n = max(s.get('batchInserts', 0), 1)
        rows.append('| %s | %s | %s | %s | %s | %s |' % (
            label, num(s.get('batches', 0)), num(s.get('plansReassigned', 0)),
            '%.3f %%' % (100 * s.get('plansReassigned', 0) / n), num(s.get('plansWithStaleLists', 0)),
            '%.3f %%' % (100 * s.get('plansWithStaleLists', 0) / n)))
    if rows:
        print('### Lo que el aplicador tuvo que rehacer')
        print()
        print('| celda | tandas | planes que eligieron posting otra vez | de los planes | planes que releyeron alguna lista | '
              'de los planes |')
        print('|---|---|---|---|---|---|')
        print('\n'.join(rows))
        print()

    rows = []
    for name, label in have:
        m = cells[name]
        s, n = m['stats'], max(m['stats']['inserted'], 1)
        search = (s['assignNs'] + s['searchNs']) / n / 1e9
        apply = s.get('applyPhaseNs', 0) / n / 1e9
        if search <= 0 or apply <= 0:
            continue
        chain = min(m['searchers'] / search, 1 / apply)
        limit = 'los buscadores' if m['searchers'] / search < 1 / apply else 'el aplicador'
        rel = times(chain / base['perSecond']) if base else '—'
        rows.append('| %s | %s | %s | %s | %s |' % (label, num(m['perSecond']), num(chain), rel, limit))
    if rows:
        print('### Lo que daría una cadena continua, calculado y no medido')
        print()
        print('El menor de N entre el tiempo de una búsqueda y 1 entre el tiempo de aplicar un insert, con los tiempos '
              'medidos en cada celda.')
        print()
        print('| celda | medido, inserts por segundo | cadena continua | frente a N = 1 medido | la limita |')
        print('|---|---|---|---|---|')
        print('\n'.join(rows))
        print()

    print('### Calidad del índice que deja cada celda')
    print()
    print('| celda | postings | splits | 0.95: ef | recall | cargas | frente a N = 1 | 0.99: ef | recall | cargas | '
          'frente a N = 1 |')
    print('|---|---|---|---|---|---|---|---|---|---|---|')
    for name, label in have:
        m = cells[name]
        if not m.get('points'):
            print('| %s | sin buscar todavía | | | | | | | | | |' % label)
            continue
        row = '| %s | %s | %s |' % (label, num(m['meta']['postings']), num(m['stats']['splits']))
        for target in TARGETS:
            p, ref = point(m, target), point(base, target)
            if p is None:
                row += ' no llega | | | |'
                continue
            rel = '—' if ref is None or name == BASE else signed(p['loads'] / ref['loads'] - 1)
            row += ' %d | %.4f | %.2f | %s |' % (p['ef'], p['recall'], p['loads'], rel)
        print(row)
    print()
    rows = []
    for name, label in have:
        for p in cells[name].get('at') or []:
            ref = next((q for q in (base or {}).get('points', []) if q['reached'] and q['ef'] == p['ef']), None)
            if ref is None:
                continue
            rows.append('| %s | %d | %.4f | %.4f | %.2f | %.2f | %s |' % (
                label, p['ef'], p['recall'], ref['recall'], p['loads'], ref['loads'],
                signed(p['loads'] / ref['loads'] - 1)))
    if rows:
        print('Con el ef de N = 1:')
        print()
        print('| celda | ef | recall | el de N = 1 | cargas | las de N = 1 | |')
        print('|---|---|---|---|---|---|---|')
        print('\n'.join(rows))
        print()
    a, b = cells.get('n4'), cells.get('n8')
    if a and b and a.get('points') and b.get('points'):
        noise = []
        for target in TARGETS:
            pa, pb = point(a, target), point(b, target)
            if pa and pb:
                noise.append('%s a %.2f (%.2f y %.2f cargas)' % (
                    signed(pb['loads'] / pa['loads'] - 1).lstrip('+−'), target, pa['loads'], pb['loads']))
        if noise:
            print('- **El ruido entre dos corridas:** N = 4 y N = 8 llevan la misma tanda y difieren en un %s.' % (
                ' y un '.join(noise)))
            print()

    print('### El criterio')
    print()
    print('Sirve si N = 8 da %.0f veces o más los inserts por segundo de N = 1, con cargas a 0.95 y a 0.99 a un %.0f %% '
          'o menos por encima de las de N = 1.' % (SPEEDUP, 100 * LOADS))
    print()
    main_cell = cells.get(MAIN)
    if not base or not main_cell or not main_cell.get('points') or not base.get('points'):
        print('- **Sin medir todavía:** faltan N = 1 o N = 8.')
    else:
        speedup = main_cell['perSecond'] / base['perSecond']
        ok = speedup >= SPEEDUP
        print('- **Inserts por segundo:** %s frente a %s, %s. %s' % (
            num(main_cell['perSecond']), num(base['perSecond']), times(speedup),
            'Se cumple.' if ok else 'No se cumple.'))
        for target in TARGETS:
            p, ref = point(main_cell, target), point(base, target)
            if p is None or ref is None:
                ok = False
                print('- **Cargas a %.2f:** una de las dos celdas no llega. No se cumple.' % target)
                continue
            rel = p['loads'] / ref['loads'] - 1
            good = rel <= LOADS
            ok = ok and good
            print('- **Cargas a %.2f:** %.2f frente a %.2f, %s. %s' % (
                target, p['loads'], ref['loads'], signed(rel), 'Se cumple.' if good else 'No se cumple.'))
        print('- **%s**' % ('Sirve.' if ok else 'No sirve, por la regla como está escrita.'))
    print()

    stretches = os.path.join(d, LABEL + '-stretches.tsv')
    if os.path.exists(stretches):
        rows = list(csv.DictReader(open(stretches), delimiter='\t'))
        if rows:
            print('### La sesión')
            print()
            for r in rows:
                used = int(r['idle at end']) >= 0 and int(r['idle at end']) < int(r['seconds'])
                print('- **%s:** de %s a %s%s%s%s.' % (
                    dict(CELLS).get(r['curve'], r['curve']), r['start'], r['end'],
                    ', con teclado o trackpad durante la corrida' if used else ', sin teclado ni trackpad',
                    ', %s eventos de energía o de actividad' % r['events'] if int(r['events']) > 0 else '',
                    '' if r['passed'] == '1' else '. NO PASÓ'))
            print('- **El tiempo de cada corrida** lleva, además de los inserts, la base, las comprobaciones de '
                  'cobertura y las búsquedas del final, que no se cronometran.')


if __name__ == '__main__':
    main()
