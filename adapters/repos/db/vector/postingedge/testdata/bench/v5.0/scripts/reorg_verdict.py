#!/usr/bin/env python3
"""The control of reorganization of v5.0 (bench-report.md, "control de
reorganización sobre el índice crecido"), from the files run_v50_timed.sh
leaves: the index the timed run grew, as it is and laid out again in two
cells, next to the cell written in batch by v4.0.

usage: reorg_verdict.py <dir of the run> > reorg-verdict.md

The user's reading: if the second cell (one segment, vectors in posting
order) is within 1.20 times the batch cell, cold, at recall 0.95, what is
missing is a reorganization in the background, and the format holds. If not,
what is left is reported by phase, fetching the blocks or the rescore, cold
and warm. And what the first cell recovers by itself.

Read as pre-registered: the p50 with 1 thread, cold, at 0.95, first round
(c1); the second round (c2) tells drift.
"""
import json
import os
import sys

LABEL = 'dbpedia-1m-grow'
ROUNDS = ('c1', 'c2')
DRIFT = 0.10
BAR = 1.20
CURVES = (
    ('grown', 'crecido, tal cual'),
    ('grown1', 'celda 1: un segmento'),
    ('grown2', 'celda 2: un segmento y vectores en orden de posting'),
    ('batch', 'escrito en lote (v4.0, 512 bits en RAM)'),
)
STATES = {
    'as it is': 'crecido, tal cual',
    'one segment': 'celda 1: un segmento',
    'one segment, vectors in posting order': 'celda 2: un segmento y vectores en orden de posting',
}
POSTINGS = 'postingedge_grow_postings'
PARTS = (('traer los bloques', 'fetchms'), ('scan y ruteo', 'visitms'), ('rescore', 'rescorems'))


def load(d, name):
    path = os.path.join(d, name)
    return json.load(open(path)) if os.path.exists(path) else None


def ms(x):
    return '—' if x is None else '%.2f' % x


def num(x):
    s = '%.0f' % x
    return s if len(s) < 4 else s[:-3] + ' ' + s[-3:]


def kb(x):
    return '%.2f KB' % (x / 1e3)


def mb(x):
    return '%.2f MB' % (x / 1e6)


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


def lat(result, target, threads, state):
    p = point(result, target, threads)
    if p is None:
        return None
    return middle(p.get('warm')) if state == 'warm' else p.get('cold')


def medians(result):
    out = {}
    for p in (result or {}).get('points', []):
        warm = middle(p.get('warm'))
        if warm:
            out[('%.2f' % p['target'], p['threads'], 'caliente')] = warm['p50ms']
        if p.get('cold'):
            out[('%.2f' % p['target'], p['threads'], 'frío')] = p['cold']['p50ms']
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
    reorg = load(d, LABEL + '-reorg.json')
    results = {c: {r: load(d, '%s-%s-%s.json' % (LABEL, r, c)) for r in ROUNDS} for c, _ in CURVES}
    first = {c: results[c]['c1'] for c, _ in CURVES}
    labels = dict(CURVES)
    stretches = read_stretches(d)

    print('### Las dos celdas: corrección y lecturas, sin cronometrar')
    print()
    if reorg is None:
        print('Sin escribir todavía.')
        print()
    else:
        print('| índice | frente al crecido tal cual, %s queries por punto | segmentos del bucket de postings | bucket de postings | fichero de vectores |' % num(reorg['queries']))
        print('|---|---|---|---|---|')
        for s in reorg['states']:
            equal = '—' if s['name'] == 'as it is' else (
                'los mismos ids, distancias y trabajo' if s['equal'] == 'the same as the index as it is' else '**' + s['equal'] + '**')
            print('| %s | %s | %d | %s | %s |' % (
                STATES.get(s['name'], s['name']), equal, s['segments'].get(POSTINGS, 0), mb(s['files'][POSTINGS]), mb(s['vectorBytes'])))
        print()
        for name, seconds in sorted(reorg.get('seconds', {}).items()):
            print('- **Escribir la %s** llevó %.0f s.' % (STATES.get(name, name).split(':')[0], seconds))
        print()
        print('**Lo que el store lee por carga,** en modo pread:')
        print()
        print('| índice | recall | ef | cargas | lecturas por carga | bytes por carga | por el modelo | real entre modelo |')
        print('|---|---|---|---|---|---|---|---|')
        for s in reorg['states']:
            for p in s['points']:
                print('| %s | %.2f | %d | %.2f | %.2f | %s | %s | %.3f |' % (
                    STATES.get(s['name'], s['name']), p['target'], p['ef'], p['loads'], p['storeReads'] / p['loads'],
                    kb(p['storeBytes'] / p['loads']), kb(p['modelBytes'] / p['loads']), p['storeBytes'] / p['modelBytes']))
        print()
        one = next((s for s in reorg['states'] if s['name'] == 'one segment'), None)
        if one is not None and one['points']:
            p = one['points'][0]
            members = p['memberBytes'] / 208.0
            model = p['modelBytes']
            form = (model + 4 * members + 18 * members + p['loads'] * 34) / model
            print('- **El criterio 1 con el bucket en un segmento, a 1M:** %.3f veces el modelo a %.2f, con el límite en 1.10. El formato solo, por su aritmética, son %.3f.' % (
                p['storeBytes'] / model, p['target'], form))
        print()

    for state, name in (('warm', 'en caliente'), ('cold', 'en frío')):
        for target in (0.95, 0.99):
            rows = []
            for c, label in CURVES:
                cells, ef = [], None
                for threads in (1, 8):
                    p = point(first[c], target, threads)
                    x = lat(first[c], target, threads, state)
                    if p is not None:
                        ef = p['ef']
                    cells += ['—'] * 4 if x is None else [ms(x['p50ms']), ms(x['p95ms']), ms(x['p99ms']), num(x['qps'])]
                if ef is not None:
                    rows.append('| %s | ef %d | %s |' % (label, ef, ' | '.join(cells)))
            if not rows:
                continue
            print('### Recall %.2f, %s' % (target, name))
            print()
            print('| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |')
            print('|---|---|---|---|---|---|---|---|---|---|')
            print('\n'.join(rows))
            print()

    print('### Cada índice entre el escrito en lote')
    print()
    print('Por el p50.')
    print()
    print('| índice | estado | 0.95, 1 thread | 0.95, 8 threads | 0.99, 1 thread | 0.99, 8 threads |')
    print('|---|---|---|---|---|---|')
    for c in ('grown', 'grown1', 'grown2'):
        for state, name in (('warm', 'en caliente'), ('cold', 'en frío')):
            cells = []
            for target in (0.95, 0.99):
                for threads in (1, 8):
                    a, b = lat(first[c], target, threads, state), lat(first['batch'], target, threads, state)
                    cells.append('—' if not a or not b else '%.2fx' % (a['p50ms'] / b['p50ms']))
            print('| %s | %s | %s |' % (labels[c], name, ' | '.join(cells)))
    print()

    print('### La lectura del usuario')
    print()
    cell2, batch, grown, cell1 = (lat(first[c], 0.95, 1, 'cold') for c in ('grown2', 'batch', 'grown', 'grown1'))
    if not cell2 or not batch:
        print('Sin medida en frío de la celda 2 y del lote todavía.')
        print()
    else:
        ratio = cell2['p50ms'] / batch['p50ms']
        if ratio <= BAR:
            print('**La celda 2 queda a %.2f veces del lote, en frío a 0.95: dentro del 1.20.** Lo que falta es reorganización en segundo plano, y el formato vale. p50 de %s ms frente a %s.' % (
                ratio, ms(cell2['p50ms']), ms(batch['p50ms'])))
        else:
            print('**La celda 2 queda a %.2f veces del lote, en frío a 0.95: por encima del 1.20.** p50 de %s ms frente a %s. Lo que queda, abajo, por fase.' % (
                ratio, ms(cell2['p50ms']), ms(batch['p50ms'])))
        print()

    print('**Lo que queda entre cada índice y el lote, por fase,** en la latencia media a 0.95 con 1 thread, en ms:')
    print()
    print('| índice | estado | total | traer los bloques | scan y ruteo | rescore |')
    print('|---|---|---|---|---|---|')
    for state, name in (('cold', 'en frío'), ('warm', 'en caliente')):
        base = lat(first['batch'], 0.95, 1, state)
        if not base:
            continue
        print('| %s | %s | %s | %s |' % (labels['batch'], name, ms(base['meanms']), ' | '.join(ms(base[k]) for _, k in PARTS)))
        for c in ('grown', 'grown1', 'grown2'):
            x = lat(first[c], 0.95, 1, state)
            if not x:
                continue
            print('| %s, de más | %s | %s | %s |' % (
                labels[c], name, ('%+.2f' % (x['meanms'] - base['meanms'])).replace('-', '−'),
                ' | '.join(('%+.2f' % (x[k] - base[k])).replace('-', '−') for _, k in PARTS)))
    print()

    print('**Lo que recupera cada celda,** de la distancia entre el crecido tal cual y el lote, a 0.95 con 1 thread:')
    print()
    print('| celda | estado | del p50 | de traer los bloques | del rescore |')
    print('|---|---|---|---|---|')
    for c in ('grown1', 'grown2'):
        for state, name in (('cold', 'en frío'), ('warm', 'en caliente')):
            g, b, x = lat(first['grown'], 0.95, 1, state), lat(first['batch'], 0.95, 1, state), lat(first[c], 0.95, 1, state)
            if not g or not b or not x:
                continue

            def share(key):
                gap = g[key] - b[key]
                if abs(gap) < 0.02:
                    return 'sin distancia que cerrar'
                return '%.0f %%' % (100 * (g[key] - x[key]) / gap)
            print('| %s | %s | %s | %s | %s |' % (labels[c], name, share('p50ms'), share('fetchms'), share('rescorems')))
    print()
    if grown and cell1 and batch:
        print('- **La celda 1 sola,** en frío: p50 de %s ms donde el crecido tal cual tarda %s y el lote %s: %.2f veces el lote.' % (
            ms(cell1['p50ms']), ms(grown['p50ms']), ms(batch['p50ms']), cell1['p50ms'] / batch['p50ms']))
    print()

    print('### Las curvas: dos rondas, deriva y actividad')
    print()
    print('| curva | ronda 1 | ronda 2 | teclado o trackpad | mayor diferencia de p50 entre rondas | marca |')
    print('|---|---|---|---|---|---|')
    for c, label in CURVES:
        if results[c]['c1'] is None:
            continue
        seen = [used(stretches.get((r, c))) for r in ROUNDS]
        if any(s is None for s in seen):
            act = 'sin registro'
        else:
            where = [r for r, s in zip(ROUNDS, seen) if s]
            act = 'no' if not where else 'sí, en la ronda %s' % ' y la '.join(r[1:] for r in where)
        m1, m2 = medians(results[c]['c1']), medians(results[c]['c2'])
        both = [(k, m1[k], m2[k], m2[k] / m1[k] - 1) for k in m1 if k in m2]
        why = []
        if not both:
            worst = 'sin segunda ronda'
        else:
            k, x, y, rel = max(both, key=lambda z: abs(z[3]))
            worst = '%s (%s, %d %s, en %s: %s y %s ms)' % (signed(rel), k[0], k[1], 'thread' if k[1] == 1 else 'threads', k[2], ms(x), ms(y))
            if any(abs(z[3]) >= DRIFT for z in both):
                why.append('deriva')
        if act.startswith('sí'):
            why.append('actividad')
        print('| %s | %s | %s | %s | %s | %s |' % (
            label, clock(stretches.get(('c1', c))), clock(stretches.get(('c2', c))), act, worst,
            '**%s**' % ' y '.join(why) if why else '—'))
    print()
    print('- **Deriva:** alguna mediana de la curva se separa un 10 % o más entre las dos rondas, sobre la primera.')
    print('- **Actividad:** al acabar el tramo, el teclado y el trackpad llevaban sin usarse menos de lo que duró.')
    print()
    print('### Lo que vale y lo que no')
    print()
    print('- **Los números absolutos de esta máquina no valen para producción.** Vale la comparación entre curvas de la misma sesión.')
    print('- **Las celdas son el mismo índice con sus bytes en otro sitio:** no miden una reorganización en segundo plano, sino lo que dejaría.')
    print('- **El formato de las filas no cambia en ninguna celda:** el marco por valor del Set sigue ahí.')


if __name__ == '__main__':
    main(sys.argv[1])
