#!/usr/bin/env python3
"""The verdict of v5.0, the inserts on disk (bench-report.md, "v5.0: inserts
en disco"), from the files run_v50_validate.sh and run_v50_timed.sh leave.

usage: grow_verdict.py <dir of the run> > grow-verdict.md

The user's pre-registration:

1. The search over the grown index reads at recall 0.95 at most 10 % more
   than the one grown in memory.
2. Its cold latency at 0.95 is within 20 % of that of the index written in
   batch.
3. The cost of an insert is reported, with no threshold.

Read as pre-registered. 1: the bytes of posting the store reads for a query,
counted by the store, over those of the twin in memory by the byte model of
v3.0 (208 B a member, 4 B an edge), with the store compacted. 2: the p50 with
1 thread, cold, of the grown index over that of the cell with codes of 512
bits written in batch by v4.0, timed in the same session, first round.
"""
import json
import os
import sys

LABEL = 'dbpedia-1m-grow'
POSTINGS, MAP, ROUTING, DISTS = (
    'postingedge_grow_postings', 'postingedge_grow_map', 'postingedge_grow_routing', 'postingedge_grow_edgedists')
BUCKETS = ((POSTINGS, 'postings'), (MAP, 'mapa'), (ROUTING, 'códigos de ruteo'), (DISTS, 'distancias de aristas'))
CURVES = (('grown', 'crecido por inserts'), ('batch', 'escrito en lote (v4.0, 512 bits en RAM)'))
ROUNDS = ('r1', 'r2')
DRIFT = 0.10


def load(d, name):
    path = os.path.join(d, name)
    return json.load(open(path)) if os.path.exists(path) else None


def num(x, digits=0):
    s = '%.*f' % (digits, x)
    whole, _, frac = s.partition('.')
    sign = ''
    if whole.startswith('-'):
        sign, whole = '−', whole[1:]
    groups = []
    while len(whole) > 3:
        groups.insert(0, whole[-3:])
        whole = whole[:-3]
    groups.insert(0, whole)
    return sign + ' '.join(groups) + ('.' + frac if frac else '')


def mb(x):
    return '—' if x is None else '%s MB' % num(x / 1e6, 2)


def kb(x):
    return '%s KB' % num(x / 1e3, 2)


def ms(x):
    return '—' if x is None else '%.2f' % x


def signed(rel):
    return '%s%.1f %%' % ('+' if rel >= 0 else '−', abs(100 * rel))


def io(stats, *keys):
    """The reads or writes of several counters together: (n, bytes)."""
    return sum(stats[k]['n'] for k in keys), sum(stats[k]['bytes'] for k in keys)


def delta(a, b):
    """What happened between two checkpoints, whose stats add up from the
    start of the run: b minus a, counter by counter."""
    out = {}
    for key, value in b.items():
        if isinstance(value, dict):
            out[key] = {'n': value['n'] - a[key]['n'], 'bytes': value['bytes'] - a[key]['bytes']}
        elif isinstance(value, (int, float)):
            out[key] = value - a.get(key, 0)
    return out


def middle(passes):
    if not passes:
        return None
    return sorted(passes, key=lambda p: p['qps'])[len(passes) // 2]


def point_of(result, target, threads):
    for p in (result or {}).get('points', []):
        if abs(p['target'] - target) < 1e-9 and p['threads'] == threads:
            return p
    return None


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
        out[(f[0], f[1])] = {
            'start': f[2], 'end': f[3], 'seconds': int(f[4]), 'idle0': int(f[5]), 'idle1': int(f[6]),
            'events': int(f[7]), 'passed': f[8] == '1',
        }
    return out


def used(stretch):
    if stretch is None or stretch['idle1'] < 0:
        return None
    return stretch['idle1'] < stretch['seconds']


def clock(stretch):
    return '—' if stretch is None else '%s a %s' % (stretch['start'][11:16], stretch['end'][11:16])


def validation(v):
    print('### Validación: del %d %% al 100 %%, en disco, junto al gemelo en memoria' % v['start'])
    print()
    print('- **Dataset:** %s, %s vectores, %s queries, k = %d.' % (v['dataset'], num(v['vectors']), num(v['queries']), v['k']))
    print('- **Camino:** split con T = %d, ef de inserción %d, códigos de ruteo de %d bits en RAM, %d vecinos.' % (
        v['threshold'], v['insertEF'], v['routingBits'], v['config']['edges']))
    print('- **El store:** sus ciclos de volcado y compactación se llaman cada %s inserts; lee en modo pread, que es donde cuenta.' % num(v['config']['cycleEvery']))
    print()
    print('| checkpoint | vivos | postings | splits | cobertura, leyendo el store | frente al gemelo |')
    print('|---|---|---|---|---|---|')
    rows = [v['base']] + v['checkpoints']
    for cp in rows:
        print('| %d %% | %s | %s | %s | %s | %s |' % (
            cp['percent'], num(cp['live']), num(cp['postings']), num(cp['splits']),
            'completa' if cp['coverage'] == 'complete' else '**' + cp['coverage'] + '**',
            'idéntico' if cp['equal'] == 'the same as the twin' else '**' + cp['equal'] + '**'))
    if v.get('final', {}).get('live'):
        cp = v['final']
        print('| 100 %%, compactado | %s | %s | %s | %s | %s |' % (
            num(cp['live']), num(cp['postings']), num(cp['splits']),
            'completa' if cp['coverage'] == 'complete' else '**' + cp['coverage'] + '**',
            'idéntico' if cp['equal'] == 'the same as the twin' else '**' + cp['equal'] + '**'))
    print()
    ok = all(cp['coverage'] == 'complete' and cp['equal'] == 'the same as the twin' for cp in rows)
    print('- **%s**' % ('Cobertura completa e índice idéntico al gemelo en todos los checkpoints.' if ok
                        else 'STOP: algún checkpoint falla.'))
    print('- **En cada insert,** además, el mismo posting, las mismas aristas y las mismas cuentas que el gemelo: la corrida para en la primera diferencia.')
    if v.get('reopened'):
        print('- **Cerrado y abierto otra vez desde sus ficheros,** devuelve lo mismo que el gemelo en todas las queries, en los dos puntos.')
    print()

    stretches = []
    for before, after in zip(rows, rows[1:]):
        stretches.append(('del %d al %d %%' % (before['percent'], after['percent']), delta(before['stats'], after['stats'])))
    if len(rows) > 1:
        stretches.append(('toda la corrida', delta(rows[0]['stats'], rows[-1]['stats'])))

    print('### Coste por insert: lecturas')
    print()
    print('Por insert, de media en el tramo. Las del store son las que cuenta el store; lo que aún está en la memtable no es una lectura.')
    print()
    print('| tramo | cargas | rondas | búsqueda: lecturas del store | bytes | vectores del rescore | inversas: lecturas de distancias | bytes | split: lecturas del store | bytes | vectores |')
    print('|---|---|---|---|---|---|---|---|---|---|---|')
    for name, s in stretches:
        n = s['inserted']
        if n <= 0:
            continue
        print('| %s | %.1f | %.1f | %.1f | %s | %.0f | %.1f | %s | %.3f | %s | %.2f |' % (
            name, s['searchLoads'] / n, s['searchRounds'] / n, s['storeSearchReads']['n'] / n,
            mb(s['storeSearchReads']['bytes'] / n), s['searchVectors']['n'] / n,
            s['inverseDistReads']['n'] / n, kb(s['inverseDistReads']['bytes'] / n),
            s['storeSplitReads']['n'] / n, kb(s['storeSplitReads']['bytes'] / n), s['splitVectorReads']['n'] / n))
    print()
    whole = stretches[-1][1] if stretches else None
    if whole and whole['inserted'] > 0:
        n = whole['inserted']
        print('- **La búsqueda de un insert devuelve %.1f filas,** %s: las de la memtable también. El store cuenta %.1f lecturas.' % (
            whole['searchRows']['n'] / n, mb(whole['searchRows']['bytes'] / n), whole['storeSearchReads']['n'] / n))
        print('- **Las inversas no leen ninguna fila:** %s listas releídas y %s vectores exactos de más en toda la corrida.' % (
            num(whole['inverseListReads']), num(whole['inverseVectorReads']['n'])))
        print('- **Las lecturas de distancias que cuenta el store** son %.2f por insert: el resto las sirve la memtable.' % (
            whole['storeInverseReads']['n'] / n))
        print('- **Un split,** cada %.0f inserts: lee las dos filas de su posting y los vectores exactos de sus miembros, %.0f de media.' % (
            n / max(whole['splits'], 1), whole['splitVectorReads']['n'] / max(whole['splits'], 1)))
    print()

    print('### Coste por insert: escrituras')
    print()
    print('Lo que se entrega al store o a sus ficheros, por insert, en valores y en bytes de valor.')
    print()
    print('| tramo | miembro | bytes | aristas propias | bytes | inversas: aristas | en postings | valores | bytes | splits, repartidos | bytes | total |')
    print('|---|---|---|---|---|---|---|---|---|---|---|---|')
    for name, s in stretches:
        n = s['inserted']
        if n <= 0:
            continue
        member = io(s, 'memberRow', 'memberMap', 'memberRouting', 'memberVector')
        own = io(s, 'ownEdges', 'ownDists')
        inverse = io(s, 'inverseLists', 'inverseTombstones', 'inverseDistWrites')
        split = io(s, 'splitRowWrites', 'splitMapWrites', 'splitCentroidWrites')
        total = member[1] + own[1] + inverse[1] + split[1]
        print('| %s | %.0f | %s | %.0f | %s | %.2f | %.2f | %.2f | %s | %.2f | %s | %s |' % (
            name, member[0] / n, kb(member[1] / n), own[0] / n, kb(own[1] / n), s['inverseEdges'] / n,
            s['inversePostings'] / n, inverse[0] / n, kb(inverse[1] / n), split[0] / n, kb(split[1] / n), kb(total / n)))
    print()
    if whole and whole['inserted'] > 0:
        n = whole['inserted']
        print('- **Miembro:** su valor en la fila (%s), su entrada del mapa (4 B), su código de ruteo (%s) y su vector exacto (%s).' % (
            kb(whole['memberRow']['bytes'] / n), kb(whole['memberRouting']['bytes'] / n), kb(whole['memberVector']['bytes'] / n)))
        print('- **Aristas propias:** su lista (%.0f B de media) y sus distancias (%.0f B).' % (
            whole['ownEdges']['bytes'] / n, whole['ownDists']['bytes'] / n))
        inv = max(whole['inverseEdges'], 1)
        print('- **Una arista inversa** son tres valores: la lista nueva (%.0f B), la lápida de la vieja (%.0f B) y las distancias (%.0f B).' % (
            whole['inverseLists']['bytes'] / inv, whole['inverseTombstones']['bytes'] / inv, whole['inverseDistWrites']['bytes'] / inv))
        splits = max(whole['splits'], 1)
        print('- **Un split** escribe las dos filas de cada hijo, %s, y %.0f entradas del mapa.' % (
            kb(whole['splitRowWrites']['bytes'] / splits), whole['splitMapWrites']['n'] / splits))
        store = sum(whole[k]['bytes'] for k in (
            'memberRow', 'memberMap', 'memberRouting', 'ownEdges', 'ownDists', 'inverseLists', 'inverseTombstones',
            'inverseDistWrites', 'splitRowWrites', 'splitMapWrites'))
        print('- **Sin el vector exacto, un insert entrega al store %s.**' % kb(store / n))
    print()

    print('### El store: tamaño y amplificación de escritura')
    print()
    print('| | ' + ' | '.join(label for _, label in BUCKETS) + ' | total del store | segmentos del bucket de postings |')
    print('|---|' + '---|' * (len(BUCKETS) + 2))
    for cp in rows:
        files = cp['files']
        print('| %d %% | %s | %s | %d |' % (
            cp['percent'], ' | '.join(mb(files[b]) for b, _ in BUCKETS), mb(sum(files[b] for b, _ in BUCKETS)),
            cp['segments'].get(POSTINGS, 0)))
    final = v.get('final') or {}
    if final.get('files'):
        files = final['files']
        total = sum(files[b] for b, _ in BUCKETS)
        print('| **100 %%, compactado** | %s | **%s** | %d |' % (
            ' | '.join(mb(files[b]) for b, _ in BUCKETS), mb(total), final['segments'].get(POSTINGS, 0)))
        print()
        written = final['written']
        by_op = {}
        for key, value in written.items():
            op = key.split('/', 1)[1]
            by_op[op] = by_op.get(op, 0) + value['bytes']
        base_written = sum(x['bytes'] for x in v['base']['written'].values())
        all_written = sum(by_op.values())
        print('- **Compactación final:** %d rondas.' % v['compactionRounds'])
        print('- **Lo que el store escribió en sus ficheros,** en toda la corrida con su compactación final: %s. Por operación: %s.' % (
            mb(all_written - base_written),
            ', '.join('%s %s' % (op, mb(b)) for op, b in sorted(by_op.items(), key=lambda x: -x[1]))))
        print('  - Lo de antes de empezar a insertar, %s, no cuenta: es de cuando se abrió la base.' % mb(base_written))
        if whole and whole['inserted'] > 0:
            delivered = sum(whole[k]['bytes'] for k in (
                'memberRow', 'memberMap', 'memberRouting', 'ownEdges', 'ownDists', 'inverseLists', 'inverseTombstones',
                'inverseDistWrites', 'splitRowWrites', 'splitMapWrites'))
            grown = total - sum(v['base']['files'][b] for b, _ in BUCKETS)
            print('- **Amplificación de escritura:** %.2f veces lo que el store creció (%s), y %.2f veces lo que se le entregó (%s).' % (
                (all_written - base_written) / grown, mb(grown), (all_written - base_written) / delivered, mb(delivered)))
        print('- **El bucket de postings compactado:** %s, frente a los 243.0 MB de la celda de 512 bits en lote de v4.0.' % mb(files[POSTINGS]))
        print('- **El grafo de centroides:** %s de log, y %s de vectores de centroide, con los %s postings retirados.' % (
            mb(files.get('centroids', 0)), mb(files.get('centroidVectors', 0)), num(final['splits'])))
    print()

    print('### Criterio 1: lo que lee una búsqueda sobre el índice crecido')
    print()
    print('Por query, sobre las %s queries. Los puntos son los del gemelo, que es el mismo índice. El modelo son 208 B por miembro y 4 B por arista de los postings que carga.' % num(v['queries']))
    print()
    print('| store | recall | ef | recall medido | cargas | rondas | lecturas del store | bytes del store | modelo, en memoria | real entre modelo | miembros | aristas |')
    print('|---|---|---|---|---|---|---|---|---|---|---|---|')
    for label, key in (('sin compactar', 'uncompacted'), ('compactado', 'compacted')):
        for p in v.get(key) or []:
            if not p['reached']:
                print('| %s | %.2f | no llega | — | — | — | — | — | — | — | — | — |' % (label, p['target']))
                continue
            print('| %s | %.2f | %d | %.4f | %.2f | %.2f | %.1f | %s | %s | %.3f | %s | %s |' % (
                label, p['target'], p['ef'], p['recall'], p['loads'], p['rounds'], p['storeReads'], mb(p['storeBytes']),
                mb(p['modelBytes']), p['storeBytes'] / p['modelBytes'], mb(p['memberBytes']), mb(p['edgeBytes'])))
    print()
    at95 = next((p for p in (v.get('compacted') or []) if abs(p['target'] - 0.95) < 1e-9 and p['reached']), None)
    if at95:
        ratio = at95['storeBytes'] / at95['modelBytes']
        print('**Criterio 1, %s:** a 0.95, compactado, el store lee %.3f veces lo que el gemelo en memoria por el modelo; el límite es 1.10.' % (
            'se cumple' if ratio <= 1.10 else 'NO se cumple', ratio))
        print()
        print('- **En cada query, lo mismo que el gemelo:** ids, distancias y trabajo, en las %s queries de cada punto.' % num(at95['compared']))
        print('- **Sin compactar, el store cuenta menos de lo que el índice lee:** lo que está en la memtable no pasa por él.')
    print()


def timed(d, manifest):
    stretches = read_stretches(d)
    print('### Inserts por segundo')
    print()
    if manifest is None:
        print('Sin cronometrar todavía.')
        print()
    else:
        print('Un escritor, del %d %% al 100 %%, con el store leyendo por su mapping y sus ciclos dentro del tiempo. GOMAXPROCS %d.' % (
            manifest['start'], manifest['gomaxprocs']))
        print()
        print('| tramo | inserts | segundos | inserts por segundo | postings al acabar |')
        print('|---|---|---|---|---|')
        before = manifest['start']
        for s in manifest['stretches']:
            print('| del %d al %d %% | %s | %s | %s | %s |' % (
                before, s['percent'], num(s['inserts']), num(s['seconds']), num(s['perSecond']), num(s['postings'])))
            before = s['percent']
        print('| **todo** | %s | %s | **%s** | |' % (num(manifest['inserts']), num(manifest['seconds']), num(manifest['perSecond'])))
        print()
        if manifest['stretches']:
            s = manifest['stretches'][-1]['stats']
            total = manifest['seconds'] * 1e9
            parts = (('la búsqueda de aristas', 'searchNs'), ('escribir el miembro', 'writeNs'), ('las aristas inversas', 'inverseNs'),
                     ('los splits', 'splitNs'), ('los ciclos del store', 'cycleNs'), ('el grafo de centroides', 'assignNs'))
            print('- **Dónde se va el tiempo:** %s.' % ', '.join('%s %.1f %%' % (label, 100 * s[key] / total) for label, key in parts))
            print('- **Un insert son %.2f ms de media.**' % (1e3 * manifest['seconds'] / manifest['inserts']))
        st = stretches.get(('inserts', 'grow'))
        if st is not None:
            act = used(st)
            print('- **El tramo:** de %s, con la base y lo de después dentro. Teclado o trackpad durante el tramo: %s.' % (
                clock(st), 'sin registro' if act is None else ('sí' if act else 'no')))
        if manifest.get('coverage'):
            print('- **Cobertura del índice que deja:** %s.' % ('completa' if manifest['coverage'] == 'complete' else manifest['coverage']))
        if manifest.get('files'):
            print('- **Compactado después, sin cronometrar:** %d rondas en %s s; el bucket de postings pasa de %s a %s.' % (
                manifest['compactionRounds'], num(manifest['compactionSeconds']),
                mb(manifest['filesBefore'][POSTINGS]), mb(manifest['files'][POSTINGS])))
        for p in manifest.get('points') or []:
            if p['reached']:
                print('- **Su punto de %.2f:** ef %d, recall %.4f.' % (p['target'], p['ef'], p['recall']))
            else:
                print('- **No llega a %.2f:** recall máximo %.4f.' % (p['target'], manifest['maxRecall']))
        print('- **Este índice no es bit a bit el validado:** su grafo de centroides sale con otros niveles.')
        print()

    results = {c: {r: load(d, '%s-%s-%s.json' % (LABEL, r, c)) for r in ROUNDS} for c, _ in CURVES}
    names = dict(CURVES)
    print('### Latencia de búsqueda: el índice crecido frente al escrito en lote')
    print()
    if all(results[c]['r1'] is None for c, _ in CURVES):
        print('Sin cronometrar todavía.')
        print()
        return
    some = next(results[c]['r1'] for c, _ in CURVES if results[c]['r1'] is not None)
    print('Primera ronda. En caliente: la pasada de QPS mediano de las cronometradas. En frío: cada tanda, tras tirar la caché y reabrir el índice.')
    print()
    print('- **Cómo se tira la caché:** `%s`' % (some['drop'] or 'no se tira: sin medida en frío'))
    print()
    for state, key in (('en caliente', 'warm'), ('en frío', 'cold')):
        for target in (0.95, 0.99):
            rows = []
            for c, label in CURVES:
                cells = []
                ef = None
                for threads in (1, 8):
                    p = point_of(results[c]['r1'], target, threads)
                    lat = None if p is None else (middle(p['warm']) if key == 'warm' else p.get('cold'))
                    if p is not None:
                        ef = p['ef']
                    cells += ['—'] * 4 if lat is None else [ms(lat['p50ms']), ms(lat['p95ms']), ms(lat['p99ms']), num(lat['qps'])]
                if ef is not None:
                    rows.append('| %s | ef %d | %s |' % (label, ef, ' | '.join(cells)))
            if not rows or all('—' in r.split('|')[3] for r in rows):
                continue
            print('**Recall %.2f, %s:**' % (target, state))
            print()
            print('| índice | punto | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |')
            print('|---|---|---|---|---|---|---|---|---|---|')
            print('\n'.join(rows))
            print()
    print('**El crecido entre el de lote,** por el p50:')
    print()
    print('| | 0.95, 1 thread | 0.95, 8 threads | 0.99, 1 thread | 0.99, 8 threads |')
    print('|---|---|---|---|---|')
    for state, key in (('en caliente', 'warm'), ('en frío', 'cold')):
        cells = []
        for target in (0.95, 0.99):
            for threads in (1, 8):
                a, b = point_of(results['grown']['r1'], target, threads), point_of(results['batch']['r1'], target, threads)
                x = None if a is None else (middle(a['warm']) if key == 'warm' else a.get('cold'))
                y = None if b is None else (middle(b['warm']) if key == 'warm' else b.get('cold'))
                cells.append('—' if not x or not y else '%.2fx' % (x['p50ms'] / y['p50ms']))
        print('| %s | %s |' % (state, ' | '.join(cells)))
    print()

    a, b = point_of(results['grown']['r1'], 0.95, 1), point_of(results['batch']['r1'], 0.95, 1)
    if a and b and a.get('cold') and b.get('cold'):
        ratio = a['cold']['p50ms'] / b['cold']['p50ms']
        print('**Criterio 2, %s:** a 0.95, en frío y con 1 thread, el p50 del índice crecido es %s ms y el del escrito en lote %s: %.2f veces; el límite es 1.20.' % (
            'se cumple' if ratio <= 1.20 else 'NO se cumple', ms(a['cold']['p50ms']), ms(b['cold']['p50ms']), ratio))
        print()
        print('- **p95 y p99:** %s y %s ms frente a %s y %s.' % (
            ms(a['cold']['p95ms']), ms(a['cold']['p99ms']), ms(b['cold']['p95ms']), ms(b['cold']['p99ms'])))
        a8, b8 = point_of(results['grown']['r1'], 0.95, 8), point_of(results['batch']['r1'], 0.95, 8)
        if a8 and b8 and a8.get('cold') and b8.get('cold'):
            print('- **Con 8 threads:** p50 de %s ms frente a %s, %.2f veces.' % (
                ms(a8['cold']['p50ms']), ms(b8['cold']['p50ms']), a8['cold']['p50ms'] / b8['cold']['p50ms']))
        print('- **Los puntos no son el mismo ef:** cada índice va en el suyo, el menor que llega al recall.')
        for label, r in (('crecido', a), ('en lote', b)):
            cold = r['cold']
            print('  - %s: ef %d; de su latencia media en frío, bloques %s ms, scan y ruteo %s, rescore %s.' % (
                label, r['ef'], ms(cold['fetchms']), ms(cold['visitms']), ms(cold['rescorems'])))
    else:
        print('**Criterio 2:** sin medida en frío de los dos índices todavía.')
    print()

    print('### Las curvas: dos rondas, deriva y actividad')
    print()
    print('| curva | ronda 1 | ronda 2 | teclado o trackpad | mayor diferencia de p50 entre rondas | marca |')
    print('|---|---|---|---|---|---|')
    for c, label in CURVES:
        if results[c]['r1'] is None:
            continue
        seen = [used(stretches.get((r, c))) for r in ROUNDS]
        if any(s is None for s in seen):
            act = 'sin registro'
        else:
            where = [r for r, s in zip(ROUNDS, seen) if s]
            act = 'no' if not where else 'sí, en la ronda %s' % ' y la '.join(r[1:] for r in where)
        m1, m2 = medians(results[c]['r1']), medians(results[c]['r2'])
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
            names[c], clock(stretches.get(('r1', c))), clock(stretches.get(('r2', c))), act, worst,
            '**%s**' % ' y '.join(why) if why else '—'))
    print()
    print('- **Deriva:** alguna mediana de la curva se separa un 10 % o más entre las dos rondas, sobre la primera.')
    print('- **Actividad:** al acabar el tramo, el teclado y el trackpad llevaban sin usarse menos de lo que duró.')
    print()
    print('### RAM residente')
    print()
    print('| índice | heap en uso tras abrir | huella del proceso |')
    print('|---|---|---|')
    for c, label in CURVES:
        r = results[c]['r1']
        if r is not None:
            print('| %s | %s | %s |' % (label, mb(r['heapBytes']), mb(r['footprintBytes'])))
    print()


def main(d):
    v = load(d, LABEL + '-validate.json')
    manifest = load(d, LABEL + '-manifest.json')
    if v is None:
        print('### Validación')
        print()
        print('Sin datos todavía.')
        print()
    else:
        validation(v)
    timed(d, manifest)
    print('### Lo que vale y lo que no')
    print()
    print('- **Los números absolutos de esta máquina no valen para producción.** Vale la comparación entre índices, medidos igual y en la misma sesión.')
    print('- **Un escritor, y ninguna búsqueda durante los inserts.**')
    print('- **El índice validado y el cronometrado son dos corridas:** la misma secuencia de inserts, con otro grafo de centroides.')
    print('- **Sin medir:** el halo en este camino, el refresco, los deletes, la durabilidad y Linux.')


if __name__ == '__main__':
    main(sys.argv[1])
