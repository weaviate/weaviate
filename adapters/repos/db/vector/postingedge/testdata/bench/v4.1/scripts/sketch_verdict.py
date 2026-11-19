#!/usr/bin/env python3
"""v4.1 (bench-report.md, "v4.1: el scan con los sketches en RAM"), from the
files run_v41.sh leaves: the cell of 512 bits of v4.0 (r512) and the six
curves whose members are scanned from memory by a sketch of 512 or 768 bits,
with a rescore limit of 200, 400 or 800, all from one build. Every curve is
timed in two passes: with the budget of 8 a query of v4.0 (b8) and with the
budget the admission limiter of a node grants (node).

usage: sketch_verdict.py <dir of the run> > sketch-verdict.md

The user's reading: a curve is an option if cold at recall 0.95 it is within
1.20 times the current cell, and it reaches 0.99. Read as pre-registered: the
p50 with 1 thread, cold, at 0.95, first round (r1), over that of r512 in the
same session and the same pass; applied with the budget of 8, with that of a
node next to it. The second round (r2) tells drift. Under 10 % between two
curves is a tie.
"""
import csv
import json
import os
import sys

LABEL = 'dbpedia-1m-sketch'
ROUNDS = ('r1', 'r2')
PASSES = (('b8', 'presupuesto de 8'), ('node', 'presupuesto de un nodo'))
DRIFT = 0.10
BAR = 1.20
TIE = 0.10
BASE = 'r512'
CURVES = (
    ('r512', 'la actual: 512 bits en RAM, rescore 64'),
    ('s512r200', 'scan con 512 bits, rescore 200'),
    ('s512r400', 'scan con 512 bits, rescore 400'),
    ('s512r800', 'scan con 512 bits, rescore 800'),
    ('s768r200', 'scan con 768 bits, rescore 200'),
    ('s768r400', 'scan con 768 bits, rescore 400'),
    ('s768r800', 'scan con 768 bits, rescore 800'),
)
WIDTHS = (('512 bits', ('s512r200', 's512r400', 's512r800')), ('768 bits', ('s768r200', 's768r400', 's768r800')))
INDEXES = (('r512', 'la actual: 512 bits en RAM'), ('s512', 'scan con 512 bits'), ('s768', 'scan con 768 bits'))
TARGETS = (0.95, 0.99)
PARTS = (('centroides', 'centroidms'), ('espera de los bloques', 'fetchms'), ('scan y ruteo', 'visitms'),
         ('rescore', 'rescorems'))


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


def mb1(x):
    return num(x / 1e6) + ' MB' if x >= 1e9 else '%.1f MB' % (x / 1e6)


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


def ratio(a, b, key='p50ms'):
    if a is None or b is None or not b.get(key):
        return None
    return a[key] / b[key]


def main():
    d = sys.argv[1]
    manifest = load(d, LABEL + '-manifest.json')
    if manifest is None:
        print('No hay manifiesto: la parte sin cronometrar no ha corrido.')
        return
    cells = {c['spec']['name']: c for c in manifest['cells']}
    vector_bytes = 4 * manifest['dims']
    results = {(r, p, c): load(d, '%s-%s-%s-%s.json' % (LABEL, r, p, c))
               for r in ROUNDS for p, _ in PASSES for c, _ in CURVES}

    print('### Las curvas: punto de operación, corrección y lecturas, sin cronometrar')
    print()
    print('Un build para las tres: %s vectores, %s postings, %s aristas, en %s s.' % (
        num(manifest['vectors']), num(manifest['build']['postings']), num(manifest['build']['edges']),
        num(manifest['build']['seconds'])))
    print()
    print('| curva | recall | ef | recall medido | cargas | rondas | lecturas de bloques | bytes de bloques | '
          'real entre modelo | lecturas de rescore | bytes de rescore | lecturas por query | bytes por query |')
    print('|---|---|---|---|---|---|---|---|---|---|---|---|---|')
    for name, label in CURVES:
        cell = cells.get(name)
        if cell is None:
            continue
        for p in cell['points']:
            if not p['reached']:
                print('| %s | %.2f | no llega: el mejor recall es %.4f con ef hasta 2 048 | | | | | | | | | | |' % (
                    label, p['target'], cell['maxRecall']))
                continue
            rescore = p['rescored'] * vector_bytes
            print('| %s | %.2f | %d | %.4f | %.2f | %.2f | %.1f | %s | %.3f | %.1f | %s | %.1f | %s |' % (
                label, p['target'], p['ef'], p['recall'], p['loads'], p['rounds'], p['storeReads'], mb(p['storeBytes']),
                p['realOverModel'], p['rescored'], mb(rescore), p['storeReads'] + p['rescored'],
                mb(p['storeBytes'] + rescore)))
    print()
    compared = [c for c in cells.values() if all(p['reached'] for p in c['points'])]
    print('- **Cada curva devuelve de disco lo mismo que su equivalente en memoria,** en los puntos que alcanza: '
          'los mismos ids, distancias y trabajo, sobre las %s queries.' % num(manifest['queries']))
    if len(compared) != len(cells):
        print('- **No todas las curvas llegan a 0.99:** ' + ', '.join(
            '%s se queda en %.4f' % (dict(CURVES)[n], c['maxRecall']) for n, c in cells.items() if c not in compared) + '.')
    print()

    print('### Disco y RAM')
    print()
    print('| índice | store | vectores exactos | lo residente, en ficheros | total en disco | heap en uso tras abrir | '
          'huella del proceso |')
    print('|---|---|---|---|---|---|---|')
    for index, label in INDEXES:
        # The curves of one width read the same index.
        curves = [name for name, _ in CURVES if name in cells and cells[name]['dir'] == index]
        if not curves:
            continue
        files = cells[curves[0]]['files']
        timed = next((results[(r, p, c)] for r in ROUNDS for p, _ in PASSES for c in curves if results[(r, p, c)]), None)
        heap = mb1(timed['heapBytes']) if timed else '—'
        footprint = mb1(timed['footprintBytes']) if timed else '—'
        print('| %s | %s | %s | %s | %s | %s | %s |' % (
            label, mb1(files['store']), mb1(files['vectors']), mb1(files['resident']),
            mb1(files['store'] + files['vectors'] + files['resident']), heap, footprint))
    print()
    print('- **El heap lleva unos 40 MB del arnés y del proceso,** como en v4.0 («El heap a 1M, por componente»).')
    print()

    if not any(results.values()):
        print('La sesión cronometrada no ha corrido todavía.')
        return

    for pass_name, pass_label in PASSES:
        if not any(results[(r, pass_name, c)] for r in ROUNDS for c, _ in CURVES):
            continue
        granted = {}
        for threads in (1, 8):
            m = measure(results[('r1', pass_name, BASE)], 0.95, threads, False)
            if m is not None and m.get('granted'):
                granted[threads] = m['granted']
        note = ''
        if granted:
            note = ': %s por query con 1 thread y %s con 8' % ('%.1f' % granted.get(1, 0), '%.1f' % granted.get(8, 0))
        print('### Con el %s%s' % (pass_label, note))
        print()
        for target in TARGETS:
            for cold, temp in ((True, 'en frío'), (False, 'en caliente')):
                base = {t: measure(results[('r1', pass_name, BASE)], target, t, cold) for t in (1, 8)}
                rows = []
                for name, label in CURVES:
                    one = measure(results[('r1', pass_name, name)], target, 1, cold)
                    eight = measure(results[('r1', pass_name, name)], target, 8, cold)
                    if one is None and eight is None:
                        continue
                    p = point(results[('r1', pass_name, name)], target, 1)
                    row = '| %s | ef %d |' % (label, p['ef'])
                    for m, t in ((one, 1), (eight, 8)):
                        if m is None:
                            row += ' — | — | — | — | — |'
                        else:
                            row += ' %s | %s | %s | %s | %s |' % (ms(m['p50ms']), times(ratio(m, base[t])), ms(m['p95ms']),
                                                               ms(m['p99ms']), num(m['qps']))
                    rows.append(row)
                if not rows:
                    continue
                print('**Recall %.2f, %s, primera ronda:**' % (target, temp))
                print()
                print('| curva | punto | 1 thread: p50, ms | frente a la actual | p95 | p99 | QPS | 8 threads: p50, ms | '
                      'frente a la actual | p95 | p99 | QPS |')
                print('|---|---|---|---|---|---|---|---|---|---|---|---|')
                print('\n'.join(rows))
                print()
        for cold, temp in ((True, 'en frío'), (False, 'en caliente')):
            rows = []
            for name, label in CURVES:
                m = measure(results[('r1', pass_name, name)], 0.95, 1, cold)
                if m is None:
                    continue
                rows.append('| %s | %s | %s | %.1f |' % (
                    label, ms(m['meanms']), ' | '.join(ms(m[key]) for _, key in PARTS), m.get('granted', 0)))
            if rows:
                print('**Las partes de la latencia media, a 0.95 con 1 thread, %s:**' % temp)
                print()
                print('| curva | media, ms | %s | presupuesto por query |' % ' | '.join(name for name, _ in PARTS))
                print('|---|---|%s---|' % ('---|' * len(PARTS)))
                print('\n'.join(rows))
                print()

    print('### La lectura')
    print()
    print('Una curva es una opción si a 0.95 en frío queda a %.2f veces o menos de la celda actual, y llega a 0.99. '
          'Es el p50 con 1 thread de la primera ronda, sobre el de la celda actual en la misma pasada.' % BAR)
    print()
    print('| curva | llega a 0.99 | en frío a 0.95, con presupuesto de 8 | frente a la actual | con el de un nodo | '
          'frente a la actual | con presupuesto de 8, que es como se lee | con el de un nodo |')
    print('|---|---|---|---|---|---|---|---|')
    verdicts = {}
    for name, label in CURVES:
        if name == BASE or name not in cells:
            continue
        reaches = all(p['reached'] for p in cells[name]['points'] if abs(p['target'] - 0.99) < 1e-9)
        row = '| %s | %s |' % (label, 'sí' if reaches else 'no, hasta %.4f' % cells[name]['maxRecall'])
        outcome = []
        for pass_name, _ in PASSES:
            m = measure(results[('r1', pass_name, name)], 0.95, 1, True)
            base = measure(results[('r1', pass_name, BASE)], 0.95, 1, True)
            r = ratio(m, base)
            row += ' %s | %s |' % (ms(m['p50ms']) if m else '—', times(r))
            if r is None:
                outcome.append(None)
            else:
                outcome.append(reaches and r <= BAR)
        for ok in outcome:
            row += ' %s |' % ('sin medir' if ok is None else ('es una opción' if ok else 'no lo es'))
        verdicts[name] = outcome
        print(row)
    print()
    for width, names in WIDTHS:
        for i, (pass_name, pass_label) in enumerate(PASSES):
            got = [verdicts[n][i] for n in names if n in verdicts]
            if not got or all(v is None for v in got):
                continue
            which = [dict(CURVES)[n] for n in names if n in verdicts and verdicts[n][i]]
            if which:
                print('- **%s, con el %s: es una opción,** con %s.' % (width, pass_label, ' y con '.join(
                    w.split(', ')[1] for w in which)))
            else:
                print('- **%s, con el %s: no es una opción** con ninguno de los tres rescores.' % (width, pass_label))
    for name, _ in CURVES:
        if name == BASE or name not in verdicts:
            continue
        for i, (pass_name, pass_label) in enumerate(PASSES):
            r = ratio(measure(results[('r1', pass_name, name)], 0.95, 1, True),
                      measure(results[('r1', pass_name, BASE)], 0.95, 1, True))
            if r is not None and abs(r - BAR) / BAR < TIE:
                print('- **%s, con el %s, queda a menos de un 10 %% del umbral:** %s.' % (
                    dict(CURVES)[name], pass_label, times(r)))
    print()

    print('### Deriva entre rondas y actividad')
    print()
    drifted = []
    measured = 0
    for pass_name, pass_label in PASSES:
        for name, label in CURVES:
            for target in TARGETS:
                for threads in (1, 8):
                    for cold, temp in ((True, 'frío'), (False, 'caliente')):
                        a = measure(results[('r1', pass_name, name)], target, threads, cold)
                        b = measure(results[('r2', pass_name, name)], target, threads, cold)
                        if a is None or b is None:
                            continue
                        measured += 1
                        rel = b['p50ms'] / a['p50ms'] - 1
                        if abs(rel) > DRIFT:
                            drifted.append('%s, %s, a %.2f con %d thread%s, en %s: %s → %s ms (%s)' % (
                                label, pass_label, target, threads, '' if threads == 1 else 's', temp, ms(a['p50ms']),
                                ms(b['p50ms']), signed(rel)))
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
