#!/usr/bin/env python3
"""The cell of the sixth amendment of v4.0 (bench-report.md, "Enmienda 6"):
HFresh with the postings of a search read several at a time, next to itself
reading them one by one and to two cells of posting-edge, timed in one
session of two rounds (p1 the measure, p2 tells drift).

usage: parallel_verdict.py <dir of the run> > parallel-verdict.md
"""
import json
import os
import sys

LABEL = 'dbpedia-1m-disk'
ROUNDS = ('p1', 'p2')
TIE = 0.10
# curve -> (label, the recall it is for, 0 for a cell of posting-edge with both)
CURVES = (
    ('hfresh-best95', 'HFresh, la mejor a 0.95, en serie', 0.95),
    ('hfresh-best95-par', 'HFresh, la mejor a 0.95, en paralelo', 0.95),
    ('hfresh-best99', 'HFresh, la mejor a 0.99, en serie', 0.99),
    ('hfresh-best99-par', 'HFresh, la mejor a 0.99, en paralelo', 0.99),
    ('r512', '512 bits en RAM', 0),
    ('halo512', 'halo de 512 bits', 0),
)


def load(d, name):
    path = os.path.join(d, name)
    return json.load(open(path)) if os.path.exists(path) else None


def ms(x):
    return '—' if x is None else ('%.2f' % x if x < 100 else '%.1f' % x)


def num(x):
    s = '%.0f' % x
    return s if len(s) < 4 else s[:-3] + ' ' + s[-3:]


def signed(rel):
    return '%s%.1f %%' % ('+' if rel >= 0 else '−', abs(100 * rel))


def middle(passes):
    if not passes:
        return None
    return sorted(passes, key=lambda p: p['qps'])[len(passes) // 2]


def point(result, target, threads):
    """The point of a curve for a recall: of a cell of posting-edge, the one
    of that target; of a configuration of HFresh, its only one."""
    for p in (result or {}).get('points', []):
        if p['threads'] != threads:
            continue
        if 'ef' not in p or abs(p['target'] - target) < 1e-9:
            return p
    return None


def lat(p, state):
    if p is None:
        return None
    return middle(p.get('warm')) if state == 'warm' else p.get('cold')


def versus(x, base):
    rel = x / base - 1
    if abs(rel) < TIE:
        return 'empate'
    return 'menos' if rel < 0 else 'más'


def medians(result):
    out = {}
    for p in (result or {}).get('points', []):
        name = '%.2f' % p['target'] if 'ef' in p else 'recall %.4f' % p['recall']
        warm = middle(p.get('warm'))
        if warm:
            out[(name, p['threads'], 'caliente')] = warm['p50ms']
        if p.get('cold'):
            out[(name, p['threads'], 'frío')] = p['cold']['p50ms']
    return out


def read_stretches(d):
    path = os.path.join(d, LABEL + '-stretches.tsv')
    out = {}
    if not os.path.exists(path):
        return out
    for line in open(path):
        f = line.rstrip('\n').split('\t')
        if len(f) < 9 or f[0] == 'round':
            continue
        out[(f[0], f[1])] = {'start': f[2], 'end': f[3], 'seconds': int(f[4]), 'idle1': int(f[6])}
    return out


def used(stretch):
    if stretch is None or stretch['idle1'] < 0:
        return None
    return stretch['idle1'] < stretch['seconds']


def clock(stretch):
    return '—' if stretch is None else '%s a %s' % (stretch['start'][11:16], stretch['end'][11:16])


def main(d):
    results = {c: {r: load(d, '%s-%s-%s.json' % (LABEL, r, c)) for r in ROUNDS} for c, _, _ in CURVES}
    stretches = read_stretches(d)
    first = {c: results[c]['p1'] for c, _, _ in CURVES}
    labels = {c: label for c, label, _ in CURVES}
    par = next((first[c] for c in ('hfresh-best95-par', 'hfresh-best99-par') if first[c] is not None), None)

    print('### Estado')
    print()
    for r in ROUNDS:
        done = [c for c, _, _ in CURVES if results[c][r] is not None]
        spans = [stretches[(r, c)] for c in done if (r, c) in stretches]
        if done:
            print('- **Ronda %s:** %s%s.' % (r[1:], ', '.join(done), '' if not spans else ', de %s a %s' % (
                min(s['start'] for s in spans)[:16], max(s['end'] for s in spans)[11:16])))
    if par is not None:
        print('- **En paralelo:** %d lecturas de posting en vuelo. En serie, una.' % par.get('readers', 0))
        print('- **Cómo se tira la caché:** `%s`' % (par['drop'] or 'no se tira: sin medida en frío'))
    print()

    for target, serial, parallel in ((0.95, 'hfresh-best95', 'hfresh-best95-par'), (0.99, 'hfresh-best99', 'hfresh-best99-par')):
        for state, name in (('warm', 'en caliente'), ('cold', 'en frío')):
            rows = []
            for c in (serial, parallel, 'r512', 'halo512'):
                cells, what = [], None
                for threads in (1, 8):
                    p = point(first[c], target, threads)
                    x = lat(p, state)
                    if p is not None:
                        what = 'ef %d' % p['ef'] if 'ef' in p else 'searchProbe %d, rescoreLimit %d' % (p['searchProbe'], p['rescoreLimit'])
                    cells += ['—'] * 4 if x is None else [ms(x['p50ms']), ms(x['p95ms']), ms(x['p99ms']), num(x['qps'])]
                p = point(first[c], target, 1)
                x = lat(p, state)
                reads = '—' if x is None else ms(x['readms'] if 'readms' in x else x['fetchms'])
                if what is not None:
                    rows.append('| %s | %s | %s | %s |' % (labels[c], what, ' | '.join(cells), reads))
            if not rows:
                continue
            print('### Recall %.2f, %s' % (target, name))
            print()
            print('| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS | de la latencia media con 1 thread, leer postings o bloques, ms |')
            print('|---|---|---|---|---|---|---|---|---|---|---|')
            print('\n'.join(rows))
            print()

    print('### En paralelo entre en serie')
    print()
    print('El p50 de HFresh con sus postings leídos en paralelo entre el suyo leyéndolos en serie, en la misma sesión.')
    print()
    print('| | 0.95, 1 thread | 0.95, 8 threads | 0.99, 1 thread | 0.99, 8 threads |')
    print('|---|---|---|---|---|')
    for state, name in (('warm', 'en caliente'), ('cold', 'en frío')):
        cells = []
        for target, serial, parallel in ((0.95, 'hfresh-best95', 'hfresh-best95-par'), (0.99, 'hfresh-best99', 'hfresh-best99-par')):
            for threads in (1, 8):
                a, b = lat(point(first[parallel], target, threads), state), lat(point(first[serial], target, threads), state)
                cells.append('—' if not a or not b else '%.2fx (%s y %s ms)' % (a['p50ms'] / b['p50ms'], ms(a['p50ms']), ms(b['p50ms'])))
        print('| %s | %s |' % (name, ' | '.join(cells)))
    print()

    print('### Posting-edge entre HFresh')
    print()
    print('El p50 de la celda entre el de HFresh, en serie y en paralelo. Menos de un 10 % de diferencia es un empate.')
    print()
    print('| celda | estado | HFresh | 0.95, 1 thread | 0.95, 8 threads | 0.99, 1 thread | 0.99, 8 threads |')
    print('|---|---|---|---|---|---|---|')
    for cell in ('r512', 'halo512'):
        for state, name in (('warm', 'en caliente'), ('cold', 'en frío')):
            for how, suffix in (('en serie', ''), ('en paralelo', '-par')):
                cells = []
                for target in (0.95, 0.99):
                    for threads in (1, 8):
                        a = lat(point(first[cell], target, threads), state)
                        b = lat(point(first['hfresh-best%d%s' % (round(100 * target), suffix)], target, threads), state)
                        cells.append('—' if not a or not b else '%.2fx, %s' % (a['p50ms'] / b['p50ms'], versus(a['p50ms'], b['p50ms'])))
                print('| %s | %s | %s | %s |' % (labels[cell], name, how, ' | '.join(cells)))
    print()

    print('### Los criterios 2 y 3 de v4.0, leídos contra HFresh en paralelo')
    print()
    print('A título informativo: los criterios pre-registrados eran contra el HFresh de main, que lee en serie, y su veredicto no se toca.')
    print()
    base = lat(point(first['hfresh-best95-par'], 0.95, 1), 'cold')
    serial = lat(point(first['hfresh-best95'], 0.95, 1), 'cold')
    if base is None:
        print('Sin medida en frío de HFresh en paralelo todavía.')
    else:
        print('- **HFresh, la mejor a 0.95, en frío con 1 thread:** p50 de %s ms en paralelo%s. Lo esperado antes de medir eran unos 12 ms.' % (
            ms(base['p50ms']), '' if serial is None else ', %s en serie' % ms(serial['p50ms'])))
        for cell, criterion, need in (('r512', 'Criterio 2, la celda de 512 bits en RAM', ('menos',)),
                                      ('halo512', 'Criterio 3, el halo de 512 bits', ('menos', 'empate'))):
            x = lat(point(first[cell], 0.95, 1), 'cold')
            if x is None:
                print('- **%s:** sin medida.' % criterion)
                continue
            how = versus(x['p50ms'], base['p50ms'])
            print('- **%s: %s.** p50 de %s ms frente a %s de HFresh en paralelo (%.2fx): %s.' % (
                criterion, 'se cumpliría' if how in need else 'NO se cumpliría', ms(x['p50ms']), ms(base['p50ms']),
                x['p50ms'] / base['p50ms'], {'menos': 'menos latencia', 'más': 'más latencia', 'empate': 'empate'}[how]))
    print()

    print('### Las curvas: dos rondas, deriva y actividad')
    print()
    print('| curva | ronda 1 | ronda 2 | teclado o trackpad | mayor diferencia de p50 entre rondas | marca |')
    print('|---|---|---|---|---|---|')
    for c, label, _ in CURVES:
        if results[c]['p1'] is None:
            continue
        seen = [used(stretches.get((r, c))) for r in ROUNDS]
        if any(s is None for s in seen):
            act = 'sin registro'
        else:
            where = [r for r, s in zip(ROUNDS, seen) if s]
            act = 'no' if not where else 'sí, en la ronda %s' % ' y la '.join(r[1:] for r in where)
        m1, m2 = medians(results[c]['p1']), medians(results[c]['p2'])
        both = [(k, m1[k], m2[k], m2[k] / m1[k] - 1) for k in m1 if k in m2]
        why = []
        if not both:
            worst = 'sin segunda ronda'
        else:
            k, x, y, rel = max(both, key=lambda z: abs(z[3]))
            worst = '%s (%s, %d %s, en %s: %s y %s ms)' % (signed(rel), k[0], k[1], 'thread' if k[1] == 1 else 'threads', k[2], ms(x), ms(y))
            if any(abs(z[3]) >= TIE for z in both):
                why.append('deriva')
        if act.startswith('sí'):
            why.append('actividad')
        print('| %s | %s | %s | %s | %s | %s |' % (
            label, clock(stretches.get(('p1', c))), clock(stretches.get(('p2', c))), act, worst,
            '**%s**' % ' y '.join(why) if why else '—'))
    print()
    print('- **Deriva:** alguna mediana de la curva se separa un 10 % o más entre las dos rondas, sobre la primera.')
    print('- **Actividad:** al acabar el tramo, el teclado y el trackpad llevaban sin usarse menos de lo que duró.')
    print()
    print('### Lo que vale y lo que no')
    print()
    print('- **Los números absolutos de esta máquina no valen para producción.** Vale la comparación entre curvas de la misma sesión.')
    print('- **HFresh en paralelo no es el HFresh de main:** es su búsqueda con una sola cosa cambiada, cuántos postings pide a la vez.')
    print('- **Un solo grado de paralelismo,** y sin medir en Linux.')


if __name__ == '__main__':
    main(sys.argv[1])
