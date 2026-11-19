#!/usr/bin/env python3
"""The verdict of v5.1, consolidated edges (bench-report.md, "v5.1: aristas
consolidadas"), from the files run_v51.sh leaves, next to those of v5.0.

usage: consolidated_verdict.py <dir of the run> <dir of v5.0> > consolidated-verdict.md

The user's pre-registration: it serves if the reads per load of the last
stretch come down to half of those of v5.0 or less, and the cold latency at
0.95 stays at 1.20 times that of the batch index or less. The inserts per
second are reported, with no threshold.

Read as pre-registered. Reads per load: those the store counts in the search
of an insert, from 80 to 100 %, in the validation, against those of the
validation of v5.0. Latency: the p50 with 1 thread, cold, at 0.95, first
round, of the index of v5.1 over that of the cell with codes of 512 bits
written in batch, timed in the same session. For every K: it serves if some K
meets both.
"""
import json
import os
import sys

LABEL = 'dbpedia-1m-cons'
V50_LABEL = 'dbpedia-1m-grow'
POSTINGS, MAP, ROUTING, DISTS, EDGES = (
    'postingedge_grow_postings', 'postingedge_grow_map', 'postingedge_grow_routing', 'postingedge_grow_edgedists',
    'postingedge_grow_edges')
BUCKETS = ((POSTINGS, 'postings'), (EDGES, 'filas de aristas'), (MAP, 'mapa'), (ROUTING, 'códigos de ruteo'),
           (DISTS, 'distancias de aristas'))
ROUNDS = ('r1', 'r2')
DRIFT = 0.10
HALF = 0.5
LIMIT = 1.20
# What an insert hands to the store, in bytes of value: every counter of
# writes but the exact vector and the centroids, which go to their files.
DELIVERED = ('memberRow', 'memberMap', 'memberRouting', 'ownEdges', 'ownDists', 'inverseLists', 'inverseTombstones',
             'inverseDistWrites', 'splitRowWrites', 'splitMapWrites', 'rowWrites', 'rowDistWrites')


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


def times(x):
    return '—' if x is None else '%.2fx' % x


def counter(stats, key):
    """A counter of reads or writes, zero where a run does not have it."""
    value = stats.get(key)
    return value if isinstance(value, dict) else {'n': 0, 'bytes': 0}


def delta(a, b):
    """What happened between two checkpoints, whose stats add up from the
    start of the run: b minus a, counter by counter. The peaks are not
    counters: they stay as b has them."""
    out = {}
    for key, value in b.items():
        if isinstance(value, dict):
            before = counter(a, key)
            out[key] = {'n': value['n'] - before['n'], 'bytes': value['bytes'] - before['bytes']}
        elif key.startswith('pendingPeak'):
            out[key] = value
        elif isinstance(value, (int, float)):
            out[key] = value - a.get(key, 0)
    return out


def stretches_of(v):
    """The stretches of a validation: (name, what happened in it)."""
    rows = [v['base']] + v['checkpoints']
    out = [('del %d al %d %%' % (a['percent'], b['percent']), delta(a['stats'], b['stats'])) for a, b in zip(rows, rows[1:])]
    if len(rows) > 1:
        out.append(('toda la corrida', delta(rows[0]['stats'], rows[-1]['stats'])))
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


def latency(result, target, threads, key):
    p = point_of(result, target, threads)
    if p is None:
        return None
    return middle(p.get('warm')) if key == 'warm' else p.get('cold')


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


def at(v, key, target):
    return next((p for p in (v.get(key) or []) if abs(p['target'] - target) < 1e-9 and p['reached']), None)


def name_of(k):
    return 'v5.0, un valor por cambio' if k == 0 else 'K = %d' % k


def correctness(runs):
    print('### Corrección: con todo volcado, lo mismo que el gemelo en memoria')
    print()
    if not runs:
        print('Sin validar todavía.')
        print()
        return
    some = next(iter(runs.values()))
    print('- **Dataset:** %s, %s vectores, %s queries, k = %d. Del %d %% al 100 %%.' % (
        some['dataset'], num(some['vectors']), num(some['queries']), some['k'], some['start']))
    print('- **Camino:** el de v5.0: split con T = %d, ef de inserción %d, códigos de ruteo de %d bits en RAM, %d vecinos.' % (
        some['threshold'], some['insertEF'], some['routingBits'], some['config']['edges']))
    print('- **Compactación mientras crece:** %s.' % (
        'forzada' if some.get('force') else 'por niveles, como en la validación de v5.0'))
    print('- **En cada checkpoint** se escribe primero lo pendiente de todos los postings. La cobertura y la comparación con el gemelo leen después el store.')
    print()
    print('| K | checkpoint | vivos | postings | splits | esperaban: miembros | en postings | cobertura, leyendo el store | frente al gemelo |')
    print('|---|---|---|---|---|---|---|---|---|')
    for k, v in sorted(runs.items()):
        rows = [v['base']] + v['checkpoints']
        if v.get('final', {}).get('live'):
            rows = rows + [dict(v['final'], percent=-1)]
        for cp in rows:
            print('| %d | %s | %s | %s | %s | %s | %s | %s | %s |' % (
                k, '100 %, compactado' if cp['percent'] < 0 else '%d %%' % cp['percent'], num(cp['live']),
                num(cp['postings']), num(cp['splits']), num(cp.get('pendingMembers', 0)), num(cp.get('pendingPostings', 0)),
                'completa' if cp['coverage'] == 'complete' else '**' + cp['coverage'] + '**',
                'idéntico' if cp['equal'] == 'the same as the twin' else '**' + cp['equal'] + '**'))
    print()
    for k, v in sorted(runs.items()):
        rows = [v['base']] + v['checkpoints']
        done = len(v['checkpoints']) > 0 and v['checkpoints'][-1]['percent'] == 100
        ok = all(cp['coverage'] == 'complete' and cp['equal'] == 'the same as the twin' for cp in rows)
        if not ok:
            state = 'STOP: algún checkpoint falla.'
        elif not done:
            state = 'sin fallos hasta el %d %%; la corrida no ha acabado.' % rows[-1]['percent']
        else:
            state = 'cobertura completa e índice idéntico al gemelo en todos los checkpoints, con todo volcado.'
        tail = ''
        if v.get('reopened'):
            tail = ' Cerrado y abierto otra vez, devuelve lo mismo que el gemelo en las %s queries, en los dos puntos.' % num(v['queries'])
        print('- **K = %d:** %s%s' % (k, state, tail))
    print('- **En cada insert,** además, el mismo posting, las mismas aristas y las mismas cuentas que el gemelo, con lo pendiente sin volcar: la corrida para en la primera diferencia.')
    print()


def reads(runs, v50):
    """The reads per load while the index grows, and whether they halve."""
    print('### Lecturas por carga mientras crece')
    print()
    cols = ([(0, v50)] if v50 else []) + sorted(runs.items())
    if not runs:
        print('Sin validar todavía.')
        print()
        return {}
    print('En la búsqueda de un insert, con el store como lo van dejando sus ciclos. Las cuenta el store: lo que está en la memtable no es una lectura. Una carga pide dos filas, la de miembros y la de aristas.')
    print()
    print('| tramo | ' + ' | '.join('%s: lecturas por carga' % name_of(k) for k, _ in cols) + ' | '
          + ' | '.join('K = %d entre v5.0' % k for k, _ in cols if k > 0) + ' |')
    print('|---|' + '---|' * (len(cols) + len(runs)))
    per = {k: dict(stretches_of(v)) for k, v in cols}
    names = [name for name, _ in stretches_of(cols[0][1])]
    for name in names:
        cells, ratios = [], []
        base = per.get(0, {}).get(name)
        for k, _ in cols:
            s = per[k].get(name)
            if s is None or s['searchLoads'] <= 0:
                cells.append('—')
                if k > 0:
                    ratios.append('—')
                continue
            x = s['storeSearchReads']['n'] / s['searchLoads']
            cells.append('%.2f' % x)
            if k > 0:
                ratios.append('—' if base is None else '%.2f' % (x / (base['storeSearchReads']['n'] / base['searchLoads'])))
        print('| %s | %s | %s |' % (name, ' | '.join(cells), ' | '.join(ratios)))
    print()
    print('| tramo | ' + ' | '.join('%s: bytes del store por carga' % name_of(k) for k, _ in cols) + ' | '
          + ' | '.join('%s: lo que la carga devuelve' % name_of(k) for k, _ in cols) + ' |')
    print('|---|' + '---|' * (2 * len(cols)))
    for name in names:
        store, back = [], []
        for k, _ in cols:
            s = per[k].get(name)
            if s is None or s['searchLoads'] <= 0:
                store.append('—')
                back.append('—')
                continue
            store.append(kb(s['storeSearchReads']['bytes'] / s['searchLoads']))
            back.append(kb(s['searchRows']['bytes'] / s['searchLoads']))
        print('| %s | %s | %s |' % (name, ' | '.join(store), ' | '.join(back)))
    print()
    print('- **El contador del store no distingue buckets.** Con aristas consolidadas la fila de aristas es un solo valor: como mucho una lectura por carga. Lo demás son lecturas de la fila de miembros.')
    print()

    last = 'del 80 al 100 %'
    verdicts = {}
    base = per.get(0, {}).get(last)
    print('**Primera mitad del criterio: las lecturas por carga del último tramo, a la mitad o menos de las de v5.0.**')
    print()
    if base is None:
        print('Sin la validación de v5.0 al lado.')
        print()
        return verdicts
    reference = base['storeSearchReads']['n'] / base['searchLoads']
    print('| | lecturas por carga, del 80 al 100 % | entre las de v5.0 | a la mitad o menos |')
    print('|---|---|---|---|')
    print('| v5.0 | %.2f | | |' % reference)
    for k, _ in sorted(runs.items()):
        s = per[k].get(last)
        if s is None or s['searchLoads'] <= 0:
            print('| K = %d | sin el último tramo todavía | | |' % k)
            continue
        x = s['storeSearchReads']['n'] / s['searchLoads']
        verdicts[k] = x / reference <= HALF
        print('| K = %d | %.2f | %.3f | %s |' % (k, x, x / reference, '**sí**' if verdicts[k] else '**no**'))
    print()
    print('- **La mitad son %.2f lecturas por carga.**' % (HALF * reference))
    print()
    return verdicts


def end_states(runs, v50):
    print('### Lecturas por carga al final, en los tres estados del store')
    print()
    cols = ([(0, v50)] if v50 else []) + sorted(runs.items())
    if not any(v.get('uncompacted') or v.get('flushed') or v.get('compacted') for _, v in sorted(runs.items())):
        print('Sin medir todavía.')
        print()
        return
    print('Por carga, sobre las queries de cada punto. Los puntos son los del gemelo de cada corrida. El modelo son 208 B por miembro y 4 B por arista de los postings que carga.')
    print()
    states = (('como lo dejan los ciclos', 'uncompacted'), ('volcado, sin compactar', 'flushed'), ('compactado', 'compacted'))
    print('| estado del store | recall | índice | ef | cargas | segmentos del bucket de postings | lecturas por carga | bytes por carga | por el modelo | real entre modelo |')
    print('|---|---|---|---|---|---|---|---|---|---|')
    for label, key in states:
        for target in (0.95, 0.99):
            for k, v in cols:
                p = at(v, key, target)
                if p is None:
                    continue
                print('| %s | %.2f | %s | %d | %.2f | %d | %.2f | %s | %s | %.3f |' % (
                    label, target, name_of(k), p['ef'], p['loads'], p.get('segments', 0), p['storeReads'] / p['loads'],
                    kb(p['storeBytes'] / p['loads']), kb(p['modelBytes'] / p['loads']), p['storeBytes'] / p['modelBytes']))
    print()
    print('- **En el primer estado el store cuenta de menos:** lo que está en la memtable no pasa por él.')
    print('- **Las corridas no comparten el grafo de centroides,** que sale con otros niveles en cada una: sus puntos y sus cargas no son exactamente los mismos.')
    for k, v in sorted(runs.items()):
        p = at(v, 'compacted', 0.95)
        if p is None:
            continue
        members = p['memberBytes'] / 208.0
        model = p['modelBytes']
        ids, frames = 5 * members, 9 * members
        rows = p['loads'] * (2 * (8 + 4 + 5) + 4)
        rest = p['storeBytes'] - model - ids - frames - rows
        print('- **K = %d, compactado, a 0.95:** sobre el modelo, el id y el grado de cada miembro en su fila de aristas %s, el marco del Set en la fila de miembros %s, cabeceras y claves %s, y lo demás %s. En total %s.' % (
            k, signed(ids / model), signed(frames / model), signed(rows / model), signed(rest / model),
            signed(p['storeBytes'] / model - 1)))
    print()


def writes(runs, v50):
    print('### Escrituras: lo que un insert entrega, y la amplificación')
    print()
    if not runs:
        print('Sin validar todavía.')
        print()
        return
    cols = ([(0, v50)] if v50 else []) + sorted(runs.items())
    print('Por insert, en toda la corrida de validación, en bytes de valor. El vector exacto, 6.14 KB, va a su fichero y no se cuenta.')
    print()
    print('| índice | miembro: fila, mapa y código de ruteo | aristas propias | inversas | filas de aristas enteras | sus distancias | splits: filas de miembros y mapa | total al store |')
    print('|---|---|---|---|---|---|---|---|')
    wholes = {}
    for k, v in cols:
        st = stretches_of(v)
        if not st or st[-1][1]['inserted'] <= 0:
            continue
        s = st[-1][1]
        wholes[k] = s
        n = s['inserted']
        member = sum(counter(s, key)['bytes'] for key in ('memberRow', 'memberMap', 'memberRouting'))
        own = sum(counter(s, key)['bytes'] for key in ('ownEdges', 'ownDists'))
        inverse = sum(counter(s, key)['bytes'] for key in ('inverseLists', 'inverseTombstones', 'inverseDistWrites'))
        rows, dists = counter(s, 'rowWrites')['bytes'], counter(s, 'rowDistWrites')['bytes']
        split = sum(counter(s, key)['bytes'] for key in ('splitRowWrites', 'splitMapWrites'))
        total = sum(counter(s, key)['bytes'] for key in DELIVERED)
        print('| %s | %s | %s | %s | %s | %s | %s | **%s** |' % (
            name_of(k), kb(member / n), kb(own / n), kb(inverse / n), kb(rows / n), kb(dists / n), kb(split / n), kb(total / n)))
    print()
    print('| K | filas de aristas escritas | por K cambios | por splits | por checkpoints | por insert | bytes de una fila | filas leídas para reescribir | lápidas de fila |')
    print('|---|---|---|---|---|---|---|---|---|')
    for k, v in sorted(runs.items()):
        s = wholes.get(k)
        if s is None:
            continue
        n = s['inserted']
        rw = counter(s, 'rowWrites')
        print('| %d | %s | %s | %s | %s | %.3f | %s | %s | %s |' % (
            k, num(rw['n']), num(s.get('rowWritesFull', 0)), num(s.get('rowWritesSplit', 0)), num(s.get('rowWritesClose', 0)),
            rw['n'] / n, kb(rw['bytes'] / max(rw['n'], 1)), num(counter(s, 'rowReads')['n']), num(s.get('rowDeletes', 0))))
    print()
    print('- **Por checkpoints:** las filas que se escriben al volcar lo pendiente en cada uno, cinco veces en la corrida. La corrida cronometrada solo las escribe al cerrar.')
    print()
    print('**El store, al acabar, compactado para siempre:**')
    print()
    present = [(b, label) for b, label in BUCKETS if any((v.get('final') or {}).get('files', {}).get(b) for _, v in cols)]
    print('| índice | ' + ' | '.join(label for _, label in present) + ' | total del store | lo que el store escribió | entre lo que creció | entre lo entregado |')
    print('|---|' + '---|' * (len(present) + 4))
    for k, v in cols:
        final = v.get('final') or {}
        if not final.get('files') or k not in wholes:
            continue
        files = final['files']
        total = sum(files.get(b, 0) for b, _ in BUCKETS)
        base_files = sum(v['base']['files'].get(b, 0) for b, _ in BUCKETS)
        wrote = sum(x['bytes'] for x in final['written'].values()) - sum(x['bytes'] for x in v['base']['written'].values())
        delivered = sum(counter(wholes[k], key)['bytes'] for key in DELIVERED)
        print('| %s | %s | %s | %s | %.2fx | %.2fx |' % (
            name_of(k), ' | '.join(mb(files.get(b, 0)) if files.get(b) else '—' for b, _ in present), mb(total), mb(wrote),
            wrote / max(total - base_files, 1), wrote / max(delivered, 1)))
    print()
    print('- **Lo que el store escribió** son su log, sus volcados y sus compactaciones, con la compactación final, sin lo de abrir la base.')
    print()


def pending(runs, manifests):
    print('### RAM de lo pendiente')
    print()
    if not runs and not manifests:
        print('Sin medir todavía.')
        print()
        return
    print('Los miembros con cambios esperando y los bytes de sus listas y de sus distancias. No incluye lo que Go gasta en guardarlos.')
    print()
    print('| K | corrida | pico: miembros | bytes | en postings | bytes por posting con algo pendiente | por posting vivo |')
    print('|---|---|---|---|---|---|---|')
    for k in sorted(set(runs) | set(manifests)):
        v = runs.get(k)
        if v and v['checkpoints']:
            s = v['checkpoints'][-1]['stats']
            live = v['checkpoints'][-1]['postings'] - v['checkpoints'][-1]['splits']
            print('| %d | validación | %s | %s | %s | %s | %s |' % (
                k, num(s.get('pendingPeakMembers', 0)), mb(s.get('pendingPeakBytes', 0)), num(s.get('pendingPeakPostings', 0)),
                kb(s.get('pendingPeakBytes', 0) / max(s.get('pendingPeakPostings', 0), 1)),
                kb(s.get('pendingPeakBytes', 0) / max(live, 1))))
        m = manifests.get(k)
        if m and m.get('stats'):
            s = m['stats']
            live = m['stretches'][-1]['postings'] - s['splits'] if m.get('stretches') else 0
            print('| %d | cronometrada | %s | %s | %s | %s | %s |' % (
                k, num(s.get('pendingPeakMembers', 0)), mb(s.get('pendingPeakBytes', 0)), num(s.get('pendingPeakPostings', 0)),
                kb(s.get('pendingPeakBytes', 0) / max(s.get('pendingPeakPostings', 0), 1)),
                kb(s.get('pendingPeakBytes', 0) / max(live, 1))))
    print()
    print('- **En la validación** cada checkpoint lo vacía: su pico es el de un tramo. **En la cronometrada** solo se vacía al cerrar.')
    rows = []
    for k, m in sorted(manifests.items()):
        for s in m.get('stretches') or []:
            rows.append('| %d | al %d %% | %s | %s | %s | %s |' % (
                k, s['percent'], num(s.get('pendingMembers', 0)), mb(s.get('pendingBytes', 0)), num(s.get('pendingPostings', 0)),
                mb(s.get('heapBytes')) if s.get('heapBytes') else '—'))
    if rows:
        print()
        print('**En la corrida cronometrada, al acabar cada tramo:**')
        print()
        print('| K | | miembros esperando | bytes | en postings | heap en uso del proceso, tras recolectar |')
        print('|---|---|---|---|---|---|')
        print('\n'.join(rows))
        print()
        print('- **El heap del proceso** tiene además los códigos de ruteo, el mapa, los centroides y su grafo.')
    print()


def inserts(d, manifests, v50_manifest):
    stretches = read_stretches(d)
    print('### Inserts por segundo')
    print()
    if not manifests:
        print('Sin cronometrar todavía.')
        print()
        return
    cols = ([(0, v50_manifest)] if v50_manifest else []) + sorted(manifests.items())
    print('Un escritor, del 10 % al 100 %, con el store leyendo por su mapping, sus ciclos dentro del tiempo y la compactación forzada. La de v5.0 es su corrida de la noche del 18 al 19: otra sesión.')
    print()
    print('| tramo | ' + ' | '.join('%s: inserts por segundo' % name_of(k) for k, _ in cols) + ' |')
    print('|---|' + '---|' * len(cols))
    some = cols[-1][1]
    before = some['start']
    for i, s in enumerate(some.get('stretches') or []):
        cells = []
        for _, m in cols:
            st = m.get('stretches') or []
            cells.append(num(st[i]['perSecond']) if i < len(st) else '—')
        print('| del %d al %d %% | %s |' % (before, s['percent'], ' | '.join(cells)))
        before = s['percent']
    print('| **todo** | %s |' % ' | '.join('**%s**' % num(m['perSecond']) if m.get('perSecond') else '—' for _, m in cols))
    print()
    parts = (('la búsqueda de aristas', 'searchNs'), ('escribir el miembro', 'writeNs'), ('las aristas inversas', 'inverseNs'),
             ('los splits', 'splitNs'), ('las filas de aristas', 'rowNs'), ('los ciclos del store', 'cycleNs'),
             ('el grafo de centroides', 'assignNs'))
    for k, m in cols:
        if not m.get('stretches'):
            continue
        s = m.get('stats') or m['stretches'][-1]['stats']
        total = m['seconds'] * 1e9
        line = ', '.join('%s %.1f %%' % (label, 100 * s.get(key, 0) / total) for label, key in parts if s.get(key))
        print('- **%s:** un insert son %.2f ms de media, repartidos así: %s.' % (name_of(k), 1e3 * m['seconds'] / m['inserts'], line))
        if k > 0:
            print('  - Al cerrar se escribieron las %s filas que esperaban, en %s s, que van en el tiempo.' % (
                num(m.get('flushRows', 0)), num(m.get('flushSeconds', 0), 1)))
            st = stretches.get(('inserts', 'k%d' % k))
            if st is not None:
                act = used(st)
                print('  - El tramo: de %s, con la base y lo de después dentro. Teclado o trackpad durante el tramo: %s.' % (
                    clock(st), 'sin registro' if act is None else ('sí' if act else 'no')))
            if m.get('coverage'):
                print('  - Cobertura del índice que deja: %s.' % ('completa' if m['coverage'] == 'complete' else m['coverage']))
        if m.get('files'):
            store = sum(m['files'].get(b, 0) for b, _ in BUCKETS)
            wrote = sum(x['bytes'] for x in m['written'].values())
            print('  - El store escribió %s, %.2f veces lo que ocupa al final (%s), con su compactación final.' % (
                mb(wrote), wrote / store, mb(store)))
        for p in m.get('points') or []:
            if p['reached']:
                print('  - Su punto de %.2f: ef %d, recall %.4f.' % (p['target'], p['ef'], p['recall']))
    print('- **Sin umbral.** Los inserts por segundo de v5.0 y de v5.1 no son de la misma sesión.')
    print()


def latencies(d, ks, read_verdicts):
    stretches = read_stretches(d)
    curves = [('k%d' % k, 'K = %d' % k) for k in ks] + [
        ('grown', 'crecido de v5.0'), ('batch', 'escrito en lote (v4.0, 512 bits en RAM)')]
    results = {c: {r: load(d, '%s-%s-%s.json' % (LABEL, r, c)) for r in ROUNDS} for c, _ in curves}
    names = dict(curves)
    print('### Latencia de búsqueda: frente al crecido de v5.0 y a la celda en lote')
    print()
    if all(results[c]['r1'] is None for c, _ in curves):
        print('Sin cronometrar todavía.')
        print()
        return
    some = next(results[c]['r1'] for c, _ in curves if results[c]['r1'] is not None)
    print('Primera ronda. En caliente: la pasada de QPS mediano de las cronometradas. En frío: cada tanda, tras tirar la caché y reabrir el índice.')
    print()
    print('- **Cómo se tira la caché:** `%s`' % (some['drop'] or 'no se tira: sin medida en frío'))
    print()
    for state, key in (('en caliente', 'warm'), ('en frío', 'cold')):
        for target in (0.95, 0.99):
            rows = []
            for c, label in curves:
                cells, ef = [], None
                for threads in (1, 8):
                    p = point_of(results[c]['r1'], target, threads)
                    lat = latency(results[c]['r1'], target, threads, key)
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
    for other, title in (('batch', 'entre la celda en lote'), ('grown', 'entre el crecido de v5.0')):
        if results[other]['r1'] is None:
            continue
        print('**Cada índice %s,** por el p50:' % title)
        print()
        print('| índice | estado | 0.95, 1 thread | 0.95, 8 threads | 0.99, 1 thread | 0.99, 8 threads |')
        print('|---|---|---|---|---|---|')
        for c, label in curves:
            if c == other or results[c]['r1'] is None:
                continue
            for state, key in (('en caliente', 'warm'), ('en frío', 'cold')):
                cells = []
                for target in (0.95, 0.99):
                    for threads in (1, 8):
                        x = latency(results[c]['r1'], target, threads, key)
                        y = latency(results[other]['r1'], target, threads, key)
                        cells.append(times(x['p50ms'] / y['p50ms'] if x and y else None))
                print('| %s | %s | %s |' % (label, state, ' | '.join(cells)))
        print()

    print('**Segunda mitad del criterio: la latencia en frío a 0.95, a 1.20 veces o menos del lote.**')
    print()
    b = latency(results['batch']['r1'], 0.95, 1, 'cold')
    lat_verdicts = {}
    if b is None:
        print('Sin medida en frío de la celda en lote todavía.')
        print()
    else:
        print('| índice | p50 en frío, 1 thread, a 0.95, ms | entre el lote | a 1.20 o menos | de su media en frío: bloques | scan y ruteo | rescore |')
        print('|---|---|---|---|---|---|---|')
        for c, label in curves:
            x = latency(results[c]['r1'], 0.95, 1, 'cold')
            if x is None:
                continue
            ratio = x['p50ms'] / b['p50ms']
            mark = ''
            if c.startswith('k'):
                lat_verdicts[int(c[1:])] = ratio <= LIMIT
                mark = '**sí**' if ratio <= LIMIT else '**no**'
            elif c == 'grown':
                mark = 'sí' if ratio <= LIMIT else 'no'
            print('| %s | %s | %s | %s | %s | %s | %s |' % (
                label, ms(x['p50ms']), '' if c == 'batch' else times(ratio), mark, ms(x['fetchms']), ms(x['visitms']),
                ms(x['rescorems'])))
        print()
        print('- **Los puntos no son el mismo ef:** cada índice va en el suyo, el menor que llega al recall.')
        print('- **Por debajo de un 10 % entre dos índices, es empate.**')
        print()

    print('### Veredicto del pre-registro')
    print()
    ks_all = sorted(set(read_verdicts) | set(lat_verdicts) | set(ks))
    print('| K | lecturas por carga a la mitad o menos | latencia en frío a 1.20 o menos | sirve |')
    print('|---|---|---|---|')
    word = {True: 'sí', False: 'no', None: 'sin medir'}
    serves = []
    for k in ks_all:
        r, lt = read_verdicts.get(k), lat_verdicts.get(k)
        both = None if r is None or lt is None else (r and lt)
        if both is False or (r is False or lt is False):
            both = False
        if both:
            serves.append(k)
        print('| %d | %s | %s | **%s** |' % (k, word[r], word[lt], word[both]))
    print()
    if any(read_verdicts.get(k) is None or lat_verdicts.get(k) is None for k in ks_all) and not serves:
        if all(read_verdicts.get(k) is False or lat_verdicts.get(k) is False for k in ks_all):
            print('**No sirve, tal como se pre-registró:** ninguna K puede cumplir ya las dos cosas.')
        else:
            print('**Sin veredicto todavía:** falta alguna de las dos medidas.')
    elif serves:
        print('**Sirve, tal como se pre-registró:** con %s se cumplen las dos cosas.' % ' y con '.join('K = %d' % k for k in serves))
    else:
        print('**No sirve, tal como se pre-registró:** ninguna K cumple las dos cosas.')
    print()

    print('### Las curvas: dos rondas, deriva y actividad')
    print()
    print('| curva | ronda 1 | ronda 2 | teclado o trackpad | mayor diferencia de p50 entre rondas | marca |')
    print('|---|---|---|---|---|---|')
    for c, label in curves:
        if results[c]['r1'] is None:
            continue
        seen = [used(stretches.get((r, c))) for r in ROUNDS]
        if any(s is None for s in seen):
            act = 'sin registro'
        else:
            where = [r for r, s in zip(ROUNDS, seen) if s]
            act = 'no' if not where else 'sí, en la ronda %s' % ' y la '.join(r[1:] for r in where)
        m1, m2 = medians(results[c]['r1']), medians(results[c]['r2'])
        both = [(key, m1[key], m2[key], m2[key] / m1[key] - 1) for key in m1 if key in m2]
        why = []
        if not both:
            worst = 'sin segunda ronda'
        else:
            key, x, y, rel = max(both, key=lambda z: abs(z[3]))
            worst = '%s (%s, %d %s, en %s: %s y %s ms)' % (
                signed(rel), key[0], key[1], 'thread' if key[1] == 1 else 'threads', key[2], ms(x), ms(y))
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
    print('### RAM residente al buscar')
    print()
    print('| índice | heap en uso tras abrir | huella del proceso |')
    print('|---|---|---|')
    for c, label in curves:
        r = results[c]['r1']
        if r is not None:
            print('| %s | %s | %s |' % (label, mb(r['heapBytes']), mb(r['footprintBytes'])))
    print()
    print('- **Tras abrir no espera nada:** lo pendiente solo existe mientras se insertan vectores.')
    print()


def main(d, v50_dir):
    runs, manifests = {}, {}
    for name in sorted(os.listdir(d)):
        if not name.startswith(LABEL + '-k'):
            continue
        k = name[len(LABEL) + 2:].split('-', 1)[0]
        if not k.isdigit():
            continue
        if name.endswith('-validate.json'):
            runs[int(k)] = load(d, name)
        elif name.endswith('-manifest.json'):
            manifests[int(k)] = load(d, name)
    v50 = load(v50_dir, V50_LABEL + '-validate.json')
    v50_manifest = load(v50_dir, V50_LABEL + '-manifest.json')
    correctness(runs)
    read_verdicts = reads(runs, v50)
    end_states(runs, v50)
    writes(runs, v50)
    pending(runs, manifests)
    inserts(d, manifests, v50_manifest)
    latencies(d, sorted(set(runs) | set(manifests)), read_verdicts)
    print('### Lo que vale y lo que no')
    print()
    print('- **Los números absolutos de esta máquina no valen para producción.** Vale la comparación entre índices, medidos igual y en la misma sesión.')
    print('- **Las lecturas por carga se comparan con la validación de v5.0,** que es otra corrida: el mismo protocolo, la misma secuencia de inserts y la misma compactación, con otro grafo de centroides.')
    print('- **Un escritor, y ninguna búsqueda durante los inserts.**')
    print('- **El índice validado y el cronometrado de cada K son dos corridas.**')
    print('- **Sin medir:** la caída con cambios pendientes y su reparación, el halo, el refresco, los deletes y Linux.')


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
