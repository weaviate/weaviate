#!/usr/bin/env python3
"""v5.4 (bench-report.md, "v5.4: la disposición de v5.3 sobre un bucket Map"),
from the files run_v54.sh leaves: the index that grows with the row of edges
of a posting as one value next to its members, in a bucket of postings that
is a Map; its gate, the index of the curves of v5.3 laid out again over a
Map; what its growth made the store write and what a load reads between
compactions, next to v5.0, to K = 64 of v5.1 and to v5.3; what a load reads
cold, row by row; and its timed curves next to v5.3 laid out again and to the
cell written in batch.

usage: map_verdict.py <dir of the run> <dir of v5.3> <dir of v5.1> [--gate] > map-verdict.md

The reading is the user's, with four conditions: laid out again, its p50 with
one thread at 0.95 is at 1.20 times that of the batch or less cold, and at
1.25 or less warm, in the first round; the store writes 10 GB or less while
it grows; and without compacting a load reads 1.5 times what it uses or less.
Under 10 % between two indexes is a tie.

With --gate it prints the gate alone and exits with 3 if it stops the run:
compacted, a load reads more than 1.10 times what it uses, or the warm p50 at
0.95 with one thread over the Map is more than 5 % over that of v5.3 laid out
again, by the median of the three passes of each.
"""
import csv
import json
import os
import statistics
import sys

LABEL = 'dbpedia-1m-map'
V53_LABEL = 'dbpedia-1m-rows'
V51_LABEL = 'dbpedia-1m-comb'
COLD_BAR, WARM_BAR, TIE = 1.20, 1.25, 0.10
WRITTEN_BAR, UNCOMPACTED_BAR = 10e9, 1.5
GATE_BYTES_BAR, GATE_WARM_BAR, GATE_WARM_NOTE = 1.10, 1.05, 1.02
NEW = 'mapo'
CURVES = (
    ('mapo', 'v5.4, reorganizado'),
    ('rowso', 'v5.3, reorganizado'),
    ('batch', 'escrito en lote (v4.0, 512 bits en RAM)'),
    ('gate', 'el índice de v5.3, reorganizado sobre un Map (mía)'),
)
# The four indexes the combined control of v5.1 counted, the two of v5.3 and
# the two of v5.4: where each count is, and its label.
READS = (
    ('v51', 'k64', 'K = 64 de v5.1, tal cual'),
    ('v51', 'grown', 'crecido de v5.0, tal cual'),
    ('v53', 'rows', 'v5.3, tal cual (en un segmento)'),
    ('out', 'map', '**v5.4, tal cual**'),
    ('v51', 'k64o', 'K = 64 de v5.1, reorganizado'),
    ('v51', 'grown2', 'la celda 2 de v5.0'),
    ('v53', 'rowso', 'v5.3, reorganizado'),
    ('out', 'mapo', '**v5.4, reorganizado**'),
)
STATES = {
    'as it is': 'v5.4, tal cual',
    'one segment': 'un segmento',
    'one segment, vectors in posting order': 'v5.4, reorganizado',
}
GATE_STATES = {
    'as it is': 'el índice de v5.3, tal cual (Set)',
    'one segment, vectors in posting order': 'el mismo, reorganizado sobre un Map',
}
POSTINGS = 'postingedge_grow_postings'


def num(x):
    """An integer with its thousands apart."""
    s = '%.0f' % x
    groups = []
    while len(s) > 3:
        groups.insert(0, s[-3:])
        s = s[:-3]
    return ' '.join([s] + groups)


def percent(ratio):
    rel = ratio - 1
    return '%s%.1f %%' % ('+' if rel >= 0 else '−', abs(100 * rel))


def load(out, name, label=None):
    path = os.path.join(out, '%s-%s.json' % (label or LABEL, name))
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def warm(point):
    """The warm pass of median QPS."""
    passes = sorted(point['warm'], key=lambda p: p['qps'])
    return passes[len(passes) // 2]


def points_of(result):
    """(target, threads) -> point."""
    return {(round(p['target'], 2), p['threads']): p for p in result['points']} if result else {}


def state_of(point, state):
    return point['cold'] if state == 'cold' else warm(point)


def reorg(out, name='reorg', title='### El índice reorganizado: corrección y lo que lee el store, sin cronometrar',
          states=None):
    states = states or STATES
    result = load(out, name)
    if not result:
        return
    print(title)
    print()
    print('| índice | frente al índice tal cual, %s queries por punto | segmentos del bucket de postings | bucket de '
          'postings | fichero de vectores |' % num(result['queries']))
    print('|---|---|---|---|---|')
    for state in result['states']:
        same = '—' if state['name'] == 'as it is' else (
            'los mismos ids, distancias y trabajo' if state['equal'] == 'the same as the index as it is'
            else '**STOP:** ' + state['equal'])
        files, segments = state['files'], state['segments']
        print('| %s | %s | %d | %.2f MB | %.2f MB |' % (
            states.get(state['name'], state['name']), same, segments.get('postingedge_grow_postings', 0),
            files.get(POSTINGS, 0) / 1e6, state['vectorBytes'] / 1e6))
    print()
    for name, seconds in sorted(result.get('seconds', {}).items()):
        print('- **Escribir «%s»** llevó %.0f s.' % (states.get(name, name), seconds))
    print()
    print('| índice | recall | ef | cargas | lecturas por carga | bytes por carga | por el modelo | real entre modelo |')
    print('|---|---|---|---|---|---|---|---|')
    for state in result['states']:
        for p in state['points']:
            print('| %s | %.2f | %d | %.2f | %.2f | %.2f KB | %.2f KB | %.3f |' % (
                states.get(state['name'], state['name']), p['target'], p['ef'], p['loads'], p['storeReads'] / p['loads'],
                p['storeBytes'] / p['loads'] / 1e3, p['modelBytes'] / p['loads'] / 1e3, p['storeBytes'] / p['modelBytes']))
    print()


def reads(out, v53, v51):
    where = {'out': (out, LABEL), 'v53': (v53, V53_LABEL), 'v51': (v51, V51_LABEL)}
    results = [(label, load(where[place][0], 'reads-' + name, where[place][1])) for place, name, label in READS]
    results = [(label, r) for label, r in results if r]
    if not any(load(out, 'reads-' + name) for place, name, _ in READS if place == 'out'):
        return
    print('### Lo que lee una carga en frío, fila a fila, sin cronometrar')
    print()
    first = results[0][1]
    print('Las %d primeras queries, de una en una, con las filas de cada carga leídas una detrás de otra. Las filas de '
          'v5.0, de v5.1 y de v5.3 son las que se contaron el 20: no se vuelven a medir.' % first['queries'])
    print()
    print('**Lo que cuenta el store,** en modo pread. Es lo mismo en frío que en caliente.')
    print()
    print('| índice | recall | cargas por query | fila de miembros: lecturas | bytes | fila de aristas: lecturas | bytes | '
          'las dos: lecturas | bytes |')
    print('|---|---|---|---|---|---|---|---|---|')
    for label, r in results:
        for p in r['points']:
            print('| %s | %.2f | %.2f | %.2f | %.2f KB | %.2f | %.2f KB | %.2f | %.2f KB |' % (
                label, p['target'], p['loads'], p['storeMembersReads'], p['storeMembersBytes'] / 1e3,
                p['storeEdgesReads'], p['storeEdgesBytes'] / 1e3, p['storeMembersReads'] + p['storeEdgesReads'],
                (p['storeMembersBytes'] + p['storeEdgesBytes']) / 1e3))
    print()
    print('| índice | segmentos: miembros | filas de aristas | bucket de miembros | bucket de filas de aristas |')
    print('|---|---|---|---|---|')
    for label, r in results:
        own = 'postingedge_grow_edges' in r['segments']
        print('| %s | %d | %s | %.2f MB | %s |' % (
            label, r['segments'].get('postingedge_grow_postings', 0),
            r['segments'].get('postingedge_grow_edges', '—') if own else '— (van en el de miembros)',
            r['files'].get('postingedge_grow_postings', 0) / 1e6,
            '%.2f MB' % (r['files'].get('postingedge_grow_edges', 0) / 1e6) if own else '—'))
    print()
    if any(r.get('noDisk') for _, r in results):
        print('**Lo que el sistema lee del disco no se cuenta aquí:** %s.' % next(
            r['noDisk'] for _, r in results if r.get('noDisk')))
        print()
        return
    print('**Lo que el sistema lee del disco para el proceso,** con el store leyendo por su mapping, tras tirar la caché '
          'antes de cada tanda de %d queries. Un page-in es un fallo de página que fue al disco; los bytes llevan lo que el '
          'sistema lee por delante.' % first['batch'])
    print()
    print('| índice | recall | se lee primero | fila de miembros: page-ins por carga | bytes | fila de aristas: page-ins | '
          'bytes | las dos: page-ins | bytes | el resto de la query: bytes |')
    print('|---|---|---|---|---|---|---|---|---|---|')
    for label, r in results:
        for p in r['points']:
            for d in p.get('disk') or []:
                print('| %s | %.2f | %s | %.2f | %.1f KB | %.2f | %.1f KB | %.2f | %.1f KB | %.1f KB |' % (
                    label, p['target'], 'los miembros' if d['first'] == 'members' else 'las aristas',
                    d['membersPageins'], d['membersBytes'] / 1e3, d['edgesPageins'], d['edgesBytes'] / 1e3,
                    d['membersPageins'] + d['edgesPageins'], (d['membersBytes'] + d['edgesBytes']) / 1e3,
                    d['restBytes'] / 1e3))
    print()
    print('- **El resto de la query** es su rescore, que lee los vectores con lecturas posicionales y no con page-ins.')
    print('- **Con las filas leídas a la vez,** como en las curvas, las dos lecturas de una carga se solapan: lo que se '
          'cuenta aquí es cuánto disco toca cada fila, no cuánto espera la ronda.')
    print()


def latency_tables(rounds):
    r1 = rounds.get('r1') or {}
    if not r1:
        return
    print('### Latencia, primera ronda')
    print()
    for target in (0.95, 0.99):
        for state, title in (('cold', 'en frío'), ('warm', 'en caliente')):
            print('**Recall %.2f, %s:**' % (target, title))
            print()
            print('| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |')
            print('|---|---|---|---|---|---|---|---|---|---|')
            for curve, label in CURVES:
                pts = points_of(r1.get(curve))
                one, eight = pts.get((target, 1)), pts.get((target, 8))
                if not one or not eight:
                    continue
                a, b = state_of(one, state), state_of(eight, state)
                print('| %s | ef %d | %.2f | %.2f | %.2f | %s | %.2f | %.2f | %.2f | %s |' % (
                    label, one['ef'], a['p50ms'], a['p95ms'], a['p99ms'], num(a['qps']), b['p50ms'], b['p95ms'],
                    b['p99ms'], num(b['qps'])))
            print()
    batch = points_of(r1.get('batch'))
    if batch:
        print('**Cada índice entre la celda en lote,** por el p50:')
        print()
        print('| índice | estado | 0.95, 1 thread | 0.95, 8 threads | 0.99, 1 thread | 0.99, 8 threads |')
        print('|---|---|---|---|---|---|')
        for curve, label in CURVES:
            if curve == 'batch':
                continue
            pts = points_of(r1.get(curve))
            for state, title in (('cold', 'en frío'), ('warm', 'en caliente')):
                cells = []
                for key in ((0.95, 1), (0.95, 8), (0.99, 1), (0.99, 8)):
                    if key in pts and key in batch:
                        cells.append('%.2fx' % (state_of(pts[key], state)['p50ms'] / state_of(batch[key], state)['p50ms']))
                    else:
                        cells.append('')
                if any(cells):
                    print('| %s | %s | %s |' % (label, title, ' | '.join(cells)))
        print()


def parts(rounds):
    """The parts of the mean latency and the rows of a load, at 0.95 with one thread."""
    r1 = rounds.get('r1') or {}
    if not r1:
        return
    for state, title in (('cold', 'en frío'), ('warm', 'en caliente')):
        print('**De la latencia media a 0.95 con 1 thread, %s,** y lo que cuesta traer cada fila, por carga:' % title)
        print()
        print('| índice | media, ms | traer los bloques | scan y ruteo | rescore | cargas | fila de miembros, µs | fila de '
              'aristas, µs |')
        print('|---|---|---|---|---|---|---|---|')
        for curve, label in CURVES:
            point = points_of(r1.get(curve)).get((0.95, 1))
            if not point:
                continue
            s = state_of(point, state)
            loads = s.get('loads') or 0
            print('| %s | %.2f | %.2f | %.2f | %.2f | %.2f | %s | %s |' % (
                label, s['meanms'], s['fetchms'], s['visitms'], s['rescorems'], loads,
                '%.1f' % (1e3 * s['memberms'] / loads) if loads and s.get('memberms') else '—',
                '%.1f' % (1e3 * s['edgems'] / loads) if loads and s.get('edgems') else '—'))
        print()


def written_of(manifest):
    return sum(v['bytes'] for v in (manifest.get('written') or {}).values())


def flushed_ratio(validate):
    """Bytes the store reads for a load over those its twin uses, flushed and not compacted, at 0.95."""
    for p in (validate or {}).get('flushed') or []:
        if round(p['target'], 2) == 0.95 and p.get('modelBytes'):
            return p['storeBytes'] / p['modelBytes']
    return None


def reading(rounds, out):
    print('### La lectura')
    print()
    print('Sustituye a v5.3 como formato si cumple las cuatro: reorganizado, a 0.95 y con 1 thread, queda a %.2f veces o '
          'menos del lote en frío y a %.2f o menos en caliente, por el p50 de la primera ronda sobre el del lote de la '
          'misma sesión; el store escribe %.0f GB o menos al crecer; y sin compactar una carga lee %.1f veces o menos lo '
          'que usa.' % (COLD_BAR, WARM_BAR, WRITTEN_BAR / 1e9, UNCOMPACTED_BAR))
    print()
    met = {}
    r1 = rounds.get('r1') or {}
    batch = points_of(r1.get('batch')).get((0.95, 1))
    new = points_of(r1.get(NEW)).get((0.95, 1))
    print('| condición | medido | hacía falta | |')
    print('|---|---|---|---|')
    for state, title, bar in (('cold', 'en frío', COLD_BAR), ('warm', 'en caliente', WARM_BAR)):
        if batch and new:
            base, mine = state_of(batch, state)['p50ms'], state_of(new, state)['p50ms']
            met[state] = mine / base <= bar
            print('| %s, v5.4 reorganizado entre el lote | %.2f ms entre %.2f: %.2fx | %.2f o menos | %s |' % (
                title, mine, base, mine / base, bar, '**sí**' if met[state] else '**no**'))
        else:
            print('| %s, v5.4 reorganizado entre el lote | sin cronometrar | %.2f o menos | |' % (title, bar))
    manifest = load(out, 'manifest')
    if manifest:
        written = written_of(manifest)
        met['written'] = written <= WRITTEN_BAR
        print('| lo que el store escribe al crecer | %.2f GB | %.0f GB o menos | %s |' % (
            written / 1e9, WRITTEN_BAR / 1e9, '**sí**' if met['written'] else '**no**'))
    else:
        print('| lo que el store escribe al crecer | sin medir | %.0f GB o menos | |' % (WRITTEN_BAR / 1e9))
    ratio = flushed_ratio(load(out, 'validate'))
    if ratio is not None:
        met['flushed'] = ratio <= UNCOMPACTED_BAR
        print('| sin compactar, lo que lee una carga entre lo que usa | %.3f | %.1f o menos | %s |' % (
            ratio, UNCOMPACTED_BAR, '**sí**' if met['flushed'] else '**no**'))
    else:
        print('| sin compactar, lo que lee una carga entre lo que usa | sin medir | %.1f o menos | |' % UNCOMPACTED_BAR)
    print()
    if len(met) == 4:
        print('**%s**' % ('Sustituye a v5.3 como formato: cumple las cuatro condiciones.' if all(met.values())
                          else 'No sustituye a v5.3, tal como se pre-registró: no cumple las cuatro condiciones.'))
        print()
    if not rounds:
        return
    print('| ronda | | v5.4, reorganizado, ms | el lote | entre el lote | v5.3, reorganizado | entre el lote | el índice de '
          'v5.3 sobre un Map | entre el lote |')
    print('|---|---|---|---|---|---|---|---|---|')
    for round_name in sorted(rounds):
        curves = rounds[round_name]
        batch = points_of(curves.get('batch')).get((0.95, 1))
        new = points_of(curves.get(NEW)).get((0.95, 1))
        if not batch or not new:
            continue
        for state, title in (('cold', 'en frío'), ('warm', 'en caliente')):
            base = state_of(batch, state)['p50ms']
            mine = state_of(new, state)['p50ms']
            others = []
            for curve in ('rowso', 'gate'):
                point = points_of(curves.get(curve)).get((0.95, 1))
                if point:
                    other = state_of(point, state)['p50ms']
                    others += ['%.2f' % other, '%.2fx' % (other / base)]
                else:
                    others += ['', '']
            print('| %s | %s | %.2f | %.2f | %.2fx | %s |' % (round_name, title, mine, base, mine / base, ' | '.join(others)))
    print()
    pairs = (('mapo', 'rowso', 'v5.4 reorganizado, frente a v5.3 reorganizado'),
             ('gate', 'rowso', 'El índice de v5.3 sobre un Map, frente al mismo sobre su Set'))
    for a, b, name in pairs:
        x, y = points_of(r1.get(a)).get((0.95, 1)), points_of(r1.get(b)).get((0.95, 1))
        if not x or not y:
            continue
        for state, title in (('cold', 'en frío'), ('warm', 'en caliente')):
            ratio = state_of(x, state)['p50ms'] / state_of(y, state)['p50ms']
            print('- **%s, %s:** %.2f veces, %s%s.' % (
                name, title, ratio, percent(ratio), ': un empate' if abs(ratio - 1) < TIE else ''))
    print()


def stretches(out, rounds):
    path = os.path.join(out, LABEL + '-stretches.tsv')
    if not os.path.exists(path):
        return
    with open(path) as f:
        rows = list(csv.DictReader(f, delimiter='\t'))
    by = {}
    for row in rows:
        by.setdefault(row['curve'], {})[row['round']] = row
    print('### Las curvas: dos rondas, deriva y actividad')
    print()
    print('| curva | ronda 1 | ronda 2 | teclado o trackpad | mayor diferencia de p50 entre rondas | marca |')
    print('|---|---|---|---|---|---|')
    for curve, label in CURVES:
        spans, active = [], []
        for round_name in ('r1', 'r2'):
            row = by.get(curve, {}).get(round_name)
            if not row:
                spans.append('')
                continue
            spans.append('%s a %s' % (row['start'][11:16], row['end'][11:16]))
            if int(row['idle at end']) < int(row['seconds']):
                active.append(round_name)
        worst, where = 0.0, ''
        a, b = points_of((rounds.get('r1') or {}).get(curve)), points_of((rounds.get('r2') or {}).get(curve))
        for key in a:
            if key not in b:
                continue
            for state, title in (('cold', 'en frío'), ('warm', 'en caliente')):
                x, y = state_of(a[key], state)['p50ms'], state_of(b[key], state)['p50ms']
                if abs(y / x - 1) > abs(worst):
                    worst = y / x - 1
                    where = '%.2f, %d thread%s, %s: %.2f y %.2f ms' % (key[0], key[1], 's' if key[1] > 1 else '', title, x, y)
        marks = []
        if abs(worst) >= 0.10:
            marks.append('**deriva**')
        if active:
            marks.append('**actividad**')
        print('| %s | %s | %s | %s | %s | %s |' % (
            label, spans[0], spans[1] if len(spans) > 1 else '',
            'sí, en la ronda %s' % ' y '.join(r[1:] for r in active) if active else 'no',
            '%s (%s)' % (percent(1 + worst), where) if where else '', ', '.join(marks) or '—'))
    print()
    print('- **Deriva:** alguna mediana de la curva se separa un 10 % o más entre las dos rondas, sobre la primera.')
    print('- **Actividad:** al acabar el tramo, el teclado y el trackpad llevaban sin usarse menos de lo que duró.')
    print()


def ram(rounds):
    r1 = rounds.get('r1') or {}
    if not r1:
        return
    print('### RAM residente al buscar')
    print()
    print('| índice | heap en uso tras abrir | huella del proceso |')
    print('|---|---|---|')
    for curve, label in CURVES:
        result = r1.get(curve)
        if result:
            print('| %s | %.2f MB | %.2f MB |' % (label, result['heapBytes'] / 1e6, result['footprintBytes'] / 1e6))
    print()


def grown(out, v53, v51):
    """What the growth wrote, next to v5.0, to K = 64 of v5.1 and to v5.3."""
    mine = load(out, 'manifest')
    if not mine:
        return
    others = (('v5.0, un valor por cambio', os.path.join(v51, '..', 'v5.0', 'dbpedia-1m-grow-manifest.json')),
              ('K = 64 de v5.1, filas en su bucket', os.path.join(v51, 'dbpedia-1m-cons-k64-manifest.json')),
              ('v5.3, filas junto a los miembros, en un Set', os.path.join(v53, V53_LABEL + '-manifest.json')))
    rows = []
    for label, path in others:
        if os.path.exists(path):
            with open(path) as f:
                rows.append((label, json.load(f)))
    rows.append(('**v5.4, filas junto a los miembros, en un Map**', mine))
    print('### El índice de las curvas: lo que su crecimiento escribió, sin cronometrar')
    print()
    print('| índice | postings | cobertura | 0.95: ef | 0.99: ef | el store escribió | lo que ocupa al final | entre lo '
          'que ocupa | filas escritas | lápidas de filas | lápidas de clave |')
    print('|---|---|---|---|---|---|---|---|---|---|---|')
    for label, m in rows:
        written = written_of(m)
        size = sum(v for k, v in (m.get('files') or {}).items() if k.startswith('postingedge_grow_'))
        efs = {round(p['target'], 2): p['ef'] for p in m.get('points') or [] if p.get('reached')}
        stats = m.get('stats') or {}
        tomb = stats.get('rowTombstones') or {}
        wrote = stats.get('rowWrites') or {}
        retired = stats.get('retireTombstones') or {}
        print('| %s | %s | %s | %s | %s | %.2f GB | %.1f MB | %.1f veces | %s | %s | %s |' % (
            label, num(m['meta']['postings']),
            'completa' if m.get('coverage') == 'complete' else m.get('coverage', '—'), efs.get(0.95, '—'),
            efs.get(0.99, '—'), written / 1e9, size / 1e6, written / size if size else 0,
            '%s, %.2f GB' % (num(wrote['n']), wrote['bytes'] / 1e9) if wrote.get('n') else '—',
            '%s, %.2f GB' % (num(tomb['n']), tomb['bytes'] / 1e9) if tomb.get('n') else '—',
            '%s, %.1f MB' % (num(retired['n']), retired['bytes'] / 1e6) if retired.get('n') else '—'))
    print()
    print('**Por dónde lo escribió,** en GB. El bucket de postings es el Set o el Map; los demás son Replace:')
    print()
    print('| índice | postings: log de escritura | postings: compactación | postings: lo demás | los demás buckets |')
    print('|---|---|---|---|---|')
    for label, m in rows:
        parts = {'wal': 0, 'compaction': 0, 'rest': 0, 'others': 0}
        for key, v in (m.get('written') or {}).items():
            strategy, _, what = key.partition('/')
            if strategy == 'replace':
                parts['others'] += v['bytes']
            elif what == 'appendWAL':
                parts['wal'] += v['bytes']
            elif what == 'compaction':
                parts['compaction'] += v['bytes']
            else:
                parts['rest'] += v['bytes']
        print('| %s | %.2f | %.2f | %.2f | %.2f |' % (
            label, parts['wal'] / 1e9, parts['compaction'] / 1e9, parts['rest'] / 1e9, parts['others'] / 1e9))
    print()
    print('- **Los postings** cuentan los retirados: son los ids dados.')
    print('- **El crecimiento no se cronometra:** corre de día, con otras cosas en la máquina.')
    print()


def validation(out, v53, v51):
    """What a load reads from the store in the three states the validation leaves it in."""
    mine = load(out, 'validate')
    if not mine:
        return
    others = (('v5.0, un valor por cambio', os.path.join(v51, '..', 'v5.0', 'dbpedia-1m-grow-validate.json')),
              ('K = 64 de v5.1, filas en su bucket', os.path.join(v51, 'dbpedia-1m-cons-k64-validate.json')),
              ('v5.3, en un Set', os.path.join(v53, V53_LABEL + '-validate.json')))
    rows = []
    for label, path in others:
        if os.path.exists(path):
            with open(path) as f:
                rows.append((label, json.load(f)))
    rows.append(('**v5.4, en un Map**', mine))
    print('### La validación: el índice junto a su gemelo, sin cronometrar')
    print()
    final = mine.get('final') or {}
    if final:
        print('- **Al acabar:** %s vectores, %s postings y %s splits; cobertura %s; frente al gemelo, %s.' % (
            num(final.get('live', 0)), num(final.get('postings', 0)), num(final.get('splits', 0)),
            'completa' if final.get('coverage') == 'complete' else '**%s**' % final.get('coverage'),
            'idéntico' if final.get('equal') == 'the same as the twin' else '**%s**' % final.get('equal')))
    if mine.get('reopened'):
        print('- **Cerrado y abierto otra vez:** %s.' % (
            'lo mismo que el gemelo en las queries de sus puntos' if mine['reopened'] == 'the same as the twin'
            else '**%s**' % mine['reopened']))
    checkpoints = mine.get('checkpoints') or []
    if checkpoints:
        bad = [c for c in checkpoints if c.get('coverage') != 'complete' or c.get('equal') != 'the same as the twin']
        print('- **Checkpoints:** %d, del %d al %d %%; %s.' % (
            len(checkpoints), checkpoints[0]['percent'], checkpoints[-1]['percent'],
            'en todos, cobertura completa e índice idéntico' if not bad else '**%d con diferencias**' % len(bad)))
    print()
    print('**Lo que el store lee por carga,** en los tres estados en que la validación lo deja:')
    print()
    print('| estado del store | índice | recall | segmentos | lecturas por carga | bytes por carga | lo que usa el gemelo | '
          'real entre lo que usa |')
    print('|---|---|---|---|---|---|---|---|')
    for key, title in (('uncompacted', 'como lo dejan los ciclos'), ('flushed', 'volcado, sin compactar'),
                       ('compacted', 'compactado')):
        for label, v in rows:
            for p in v.get(key) or []:
                if not p.get('reached') or not p.get('modelBytes'):
                    continue
                print('| %s | %s | %.2f | %d | %.2f | %.1f KB | %.1f KB | %.3f |' % (
                    title, label, p['target'], p.get('segments', 0), p['storeReads'] / p['loads'],
                    p['storeBytes'] / p['loads'] / 1e3, p['modelBytes'] / p['loads'] / 1e3, p['storeBytes'] / p['modelBytes']))
    print()
    print('- **En el primer estado el store cuenta de menos** en todos: lo que está en el memtable no pasa por él.')
    print()


def gate(out):
    """The gate of the pre-registration. Returns whether it stops the run, None while it cannot tell."""
    result = load(out, 'gate-reorg')
    if not result:
        return None
    reorg(out, 'gate-reorg', '### La puerta: el índice de v5.3, reorganizado sobre un Map, sin cronometrar', GATE_STATES)
    stop, known = False, True
    cell = [st for st in result['states'] if st['name'] != 'as it is']
    equal = bool(cell) and all(st['equal'] == 'the same as the index as it is' for st in cell)
    ratio = None
    for st in cell:
        for p in st['points']:
            if round(p['target'], 2) == 0.95 and p.get('modelBytes'):
                ratio = p['storeBytes'] / p['modelBytes']
    passes = {}
    for index in ('set', 'map'):
        for n in (1, 2, 3):
            r = load(out, 'gate-warm-%s-%d' % (index, n))
            if r:
                passes.setdefault(index, []).append(r)
    keys = ((0.95, 1), (0.95, 8), (0.99, 1), (0.99, 8))
    medians = {}
    if passes.get('set') and passes.get('map'):
        print('**En caliente, sin cronometrar e indicativo:** cada índice en su proceso, alternados, con la pasada de QPS '
              'mediana de cada proceso. El Set es «v5.3, reorganizado»; el Map tiene sus mismos postings.')
        print()
        print('| pasada | Set: p50 a 0.95, 1 thread, ms | Map | Map entre Set | Set: fila de miembros, µs por carga | Map | '
              'Set: fila de aristas, µs | Map |')
        print('|---|---|---|---|---|---|---|---|')
        for n in range(min(len(passes['set']), len(passes['map']))):
            a = warm(points_of(passes['set'][n])[(0.95, 1)])
            b = warm(points_of(passes['map'][n])[(0.95, 1)])
            print('| %d | %.3f | %.3f | %.3fx | %.1f | %.1f | %.1f | %.1f |' % (
                n + 1, a['p50ms'], b['p50ms'], b['p50ms'] / a['p50ms'], 1e3 * a['memberms'] / a['loads'],
                1e3 * b['memberms'] / b['loads'], 1e3 * a['edgems'] / a['loads'], 1e3 * b['edgems'] / b['loads']))
        for index in ('set', 'map'):
            for key in keys:
                medians[(index, key)] = statistics.median(
                    warm(points_of(r)[key])['p50ms'] for r in passes[index] if key in points_of(r))
        print()
        print('| por la mediana de las pasadas | 0.95, 1 thread | 0.95, 8 threads | 0.99, 1 thread | 0.99, 8 threads |')
        print('|---|---|---|---|---|')
        for index, title in (('set', 'Set, p50 en ms'), ('map', 'Map, p50 en ms')):
            print('| %s | %s |' % (title, ' | '.join('%.3f' % medians[(index, key)] for key in keys)))
        print('| Map entre Set | %s |' % ' | '.join(
            '%.3fx' % (medians[('map', key)] / medians[('set', key)]) for key in keys))
        print()
    print('**La regla de la puerta:**')
    print()
    if not equal:
        print('- **STOP de corrección:** el índice sobre un Map no devuelve lo mismo que el de v5.3.')
        stop = True
    else:
        print('- **Corrección:** devuelve los mismos ids, distancias y trabajo que el índice de v5.3.')
    if ratio is None:
        known = False
    else:
        over = ratio > GATE_BYTES_BAR
        stop = stop or over
        print('- **Lo que lee el store, compactado, a 0.95:** %.3f veces lo que usa, con el límite en %.2f: %s.' % (
            ratio, GATE_BYTES_BAR, '**para**' if over else 'sigue'))
    if not medians:
        known = False
        print('- **En caliente:** sin medir todavía.')
    else:
        warm_ratio = medians[('map', (0.95, 1))] / medians[('set', (0.95, 1))]
        over = warm_ratio > GATE_WARM_BAR
        stop = stop or over
        note = ''
        if not over and warm_ratio > GATE_WARM_NOTE:
            note = ' La sesión cronometrada puede fallar la condición de en caliente.'
        print('- **En caliente, el p50 a 0.95 con 1 thread sobre el Map entre el de sobre el Set:** %.3f, %s, con el '
              'límite en %.2f: %s.%s' % (warm_ratio, percent(warm_ratio), GATE_WARM_BAR,
                                         '**para**' if over else 'sigue', note))
    print()
    if stop:
        print('**La puerta para la corrida.**')
        print()
        return True
    if not known:
        return None
    print('**La puerta deja seguir.**')
    print()
    return False


def main(out, v53, v51, only_gate):
    if only_gate:
        stop = gate(out)
        sys.exit(3 if stop else (1 if stop is None else 0))
    rounds = {}
    for round_name in ('r1', 'r2'):
        for curve, _ in CURVES:
            result = load(out, '%s-%s' % (round_name, curve))
            if result:
                rounds.setdefault(round_name, {})[curve] = result
    reading(rounds, out)
    gate(out)
    validation(out, v53, v51)
    grown(out, v53, v51)
    reorg(out)
    reads(out, v53, v51)
    if not rounds:
        print('### La sesión cronometrada')
        print()
        print('Sin cronometrar todavía.')
        return
    latency_tables(rounds)
    parts(rounds)
    stretches(out, rounds)
    ram(rounds)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], '--gate' in sys.argv[4:])
