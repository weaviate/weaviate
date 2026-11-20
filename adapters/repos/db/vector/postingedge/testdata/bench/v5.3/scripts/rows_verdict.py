#!/usr/bin/env python3
"""v5.3 (bench-report.md, "v5.3: la fila de aristas en un valor, junto a la
de miembros"), from the files run_v53.sh leaves: the index that grows with
the row of edges of a posting as one value in the bucket of postings, as it
is and laid out again, next to cell 2 of the control of v5.0, to the cell
written in batch and to the cell of the combined control of v5.1; and what a
load reads cold, row by row, next to the four indexes that control counted.

usage: rows_verdict.py <dir of the run> <dir of v5.1> > rows-verdict.md

The reading is the user's: it is the candidate format if, laid out again, its
p50 with one thread at 0.95 is at 1.20 times that of the batch or less cold,
and at 1.25 or less warm, in the first round. How near it gets as it is goes
next to it. Under 10 % between two indexes is a tie.
"""
import csv
import json
import os
import sys

LABEL = 'dbpedia-1m-rows'
V51_LABEL = 'dbpedia-1m-comb'
COLD_BAR, WARM_BAR, TIE = 1.20, 1.25, 0.10
NEW, NEW_AS_IS = 'rowso', 'rows'
CURVES = (
    ('rows', 'v5.3, tal cual'),
    ('rowso', 'v5.3, reorganizado'),
    ('grown2', 'la celda 2 de v5.0'),
    ('batch', 'escrito en lote (v4.0, 512 bits en RAM)'),
    ('k64o', 'K = 64 de v5.1, reorganizado (mía)'),
)
# The four indexes the combined control of v5.1 counted, and the two of v5.3.
READS = (
    (True, 'k64', 'K = 64 de v5.1, tal cual'),
    (True, 'grown', 'crecido de v5.0, tal cual'),
    (False, 'rows', '**v5.3, tal cual**'),
    (True, 'k64o', 'K = 64 de v5.1, reorganizado'),
    (True, 'grown2', 'la celda 2 de v5.0'),
    (False, 'rowso', '**v5.3, reorganizado**'),
)
STATES = {
    'as it is': 'v5.3, tal cual',
    'one segment': 'un segmento',
    'one segment, vectors in posting order': 'v5.3, reorganizado',
}


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


def reorg(out):
    result = load(out, 'reorg')
    if not result:
        return
    print('### El índice reorganizado: corrección y lo que lee el store, sin cronometrar')
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
            STATES.get(state['name'], state['name']), same, segments.get('postingedge_grow_postings', 0),
            files.get('postingedge_grow_postings', 0) / 1e6, state['vectorBytes'] / 1e6))
    print()
    for name, seconds in sorted(result.get('seconds', {}).items()):
        print('- **Escribir «%s»** llevó %.0f s.' % (STATES.get(name, name), seconds))
    print()
    print('| índice | recall | ef | cargas | lecturas por carga | bytes por carga | por el modelo | real entre modelo |')
    print('|---|---|---|---|---|---|---|---|')
    for state in result['states']:
        for p in state['points']:
            print('| %s | %.2f | %d | %.2f | %.2f | %.2f KB | %.2f KB | %.3f |' % (
                STATES.get(state['name'], state['name']), p['target'], p['ef'], p['loads'], p['storeReads'] / p['loads'],
                p['storeBytes'] / p['loads'] / 1e3, p['modelBytes'] / p['loads'] / 1e3, p['storeBytes'] / p['modelBytes']))
    print()


def reads(out, v51):
    results = [(label, load(v51, 'reads-' + name, V51_LABEL) if old else load(out, 'reads-' + name))
               for old, name, label in READS]
    results = [(label, r) for label, r in results if r]
    if not any(load(out, 'reads-' + name) for old, name, _ in READS if not old):
        return
    print('### Lo que lee una carga en frío, fila a fila, sin cronometrar')
    print()
    first = results[0][1]
    print('Las %d primeras queries, de una en una, con las filas de cada carga leídas una detrás de otra. Las cuatro filas '
          'de v5.0 y de v5.1 son las del control combinado, contadas el 20 por la mañana: no se vuelven a medir.'
          % first['queries'])
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


def reading(rounds):
    print('### La lectura')
    print()
    print('Es el formato candidato si, reorganizado, a 0.95 y con 1 thread, queda a %.2f veces o menos del lote en frío y '
          'a %.2f o menos en caliente. Por el p50 de la primera ronda, sobre el del lote de la misma sesión. Sin reorganizar '
          'se dan las mismas dos razones, sin umbral.' % (COLD_BAR, WARM_BAR))
    print()
    verdict = None
    print('| ronda | | v5.3, reorganizado, ms | el lote | entre el lote | entra | v5.3, tal cual | entre el lote | la '
          'celda 2 de v5.0 | entre el lote | K = 64 de v5.1, reorganizado | entre el lote |')
    print('|---|---|---|---|---|---|---|---|---|---|---|---|')
    for round_name in sorted(rounds):
        curves = rounds[round_name]
        batch = points_of(curves.get('batch')).get((0.95, 1))
        new = points_of(curves.get(NEW)).get((0.95, 1))
        if not batch or not new:
            continue
        met = []
        for state, title, bar in (('cold', 'en frío', COLD_BAR), ('warm', 'en caliente', WARM_BAR)):
            base = state_of(batch, state)['p50ms']
            mine = state_of(new, state)['p50ms']
            ok = mine / base <= bar
            met.append(ok)
            others = []
            for curve in (NEW_AS_IS, 'grown2', 'k64o'):
                point = points_of(curves.get(curve)).get((0.95, 1))
                if point:
                    other = state_of(point, state)['p50ms']
                    others += ['%.2f' % other, '%.2fx' % (other / base)]
                else:
                    others += ['', '']
            print('| %s | %s | %.2f | %.2f | %.2fx | %s | %s |' % (
                round_name, title, mine, base, mine / base, '**sí**' if ok else '**no**', ' | '.join(others)))
        if round_name == 'r1':
            verdict = all(met)
    print()
    r1 = rounds.get('r1') or {}
    new = points_of(r1.get(NEW)).get((0.95, 1))
    for other, name in (('grown2', 'la celda 2 de v5.0'), ('k64o', 'K = 64 de v5.1, reorganizado')):
        old = points_of(r1.get(other)).get((0.95, 1))
        if not new or not old:
            continue
        for state, title in (('cold', 'en frío'), ('warm', 'en caliente')):
            ratio = state_of(new, state)['p50ms'] / state_of(old, state)['p50ms']
            print('- **Reorganizado, frente a %s, %s:** %.2f veces, %s%s.' % (
                name, title, ratio, percent(ratio), ': un empate' if abs(ratio - 1) < TIE else ''))
    if verdict is not None:
        print()
        print('**%s**' % ('Es el formato candidato: cumple las dos mitades en la primera ronda.' if verdict
                          else 'No es el formato candidato, tal como se pre-registró: no cumple las dos mitades.'))
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


def grown(out, v51):
    """What the growth wrote, next to v5.0 and to K = 64 of v5.1."""
    mine = load(out, 'manifest')
    if not mine:
        return
    others = (('v5.0, un valor por cambio', os.path.join(v51, '..', 'v5.0', 'dbpedia-1m-grow-manifest.json')),
              ('K = 64 de v5.1, filas en su bucket', os.path.join(v51, 'dbpedia-1m-cons-k64-manifest.json')))
    rows = []
    for label, path in others:
        if os.path.exists(path):
            with open(path) as f:
                rows.append((label, json.load(f)))
    rows.append(('**v5.3, filas junto a los miembros**', mine))
    print('### El índice de las curvas: lo que su crecimiento escribió, sin cronometrar')
    print()
    print('| índice | postings | cobertura | 0.95: ef | 0.99: ef | el store escribió | lo que ocupa al final | entre lo '
          'que ocupa | filas escritas | lápidas de filas |')
    print('|---|---|---|---|---|---|---|---|---|---|')
    for label, m in rows:
        written = sum(v['bytes'] for v in (m.get('written') or {}).values())
        size = sum(v for k, v in (m.get('files') or {}).items() if k.startswith('postingedge_grow_'))
        efs = {round(p['target'], 2): p['ef'] for p in m.get('points') or [] if p.get('reached')}
        stats = m.get('stats') or {}
        tomb = stats.get('rowTombstones') or {}
        wrote = stats.get('rowWrites') or {}
        print('| %s | %s | %s | %s | %s | %.2f GB | %.1f MB | %.1f veces | %s | %s |' % (
            label, num(m['meta']['postings']),
            'completa' if m.get('coverage') == 'complete' else m.get('coverage', '—'), efs.get(0.95, '—'),
            efs.get(0.99, '—'), written / 1e9, size / 1e6, written / size if size else 0,
            '%s, %.2f GB' % (num(wrote['n']), wrote['bytes'] / 1e9) if wrote.get('n') else '—',
            '%s, %.2f GB' % (num(tomb['n']), tomb['bytes'] / 1e9) if tomb.get('n') else '—'))
    print()
    print('- **Los postings** cuentan los retirados: son los ids dados.')
    print('- **El crecimiento de v5.3 no se cronometra:** corre de día, con otras cosas en la máquina.')
    print()


def main(out, v51):
    rounds = {}
    for round_name in ('r1', 'r2'):
        for curve, _ in CURVES:
            result = load(out, '%s-%s' % (round_name, curve))
            if result:
                rounds.setdefault(round_name, {})[curve] = result
    grown(out, v51)
    reorg(out)
    reads(out, v51)
    if not rounds:
        print('### La sesión cronometrada')
        print()
        print('Sin cronometrar todavía.')
        return
    reading(rounds)
    latency_tables(rounds)
    parts(rounds)
    stretches(out, rounds)
    ram(rounds)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
