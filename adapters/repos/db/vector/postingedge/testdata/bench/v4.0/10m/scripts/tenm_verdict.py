#!/usr/bin/env python3
"""10M on disk (bench-report.md, "10M en disco: wiki-dpr 10M con el camino de
lectura de v4.0"), from the files run_10m.sh leaves: the two cells of the
posting-edge index over 10M vectors of wiki-dpr, next to the HFresh of this
branch as a node runs it, at its best configuration for 0.95 and for 0.99,
its postings read one by one and 8 at a time; and the same two cells over 1M,
which the loads are read against.

usage: tenm_verdict.py <dir of the run> <label of 10M> <label of 1M> > tenm-verdict.md

The two conditions are the user's: cold, at 0.95 with one thread, the p50 of
HFresh with its postings read 8 at a time is 3 times that of each cell or
more, in the first round; and the loads of a query at 0.95 are at 1.15 times
those of 1M or less. Under 10 % between two indexes is a tie.
"""
import csv
import json
import os
import sys

ROUNDS = ('n1', 'n2')
ADVANTAGE_BAR, LOADS_BAR, DRIFT = 3.0, 1.15, 0.10
TARGETS = (0.95, 0.99)
CURVES = (
    ('hfresh-best95', 'HFresh, postings uno a uno', 0.95),
    ('hfresh-best95-par', 'HFresh, postings de 8 en 8', 0.95),
    ('hfresh-best99', 'HFresh, postings uno a uno', 0.99),
    ('hfresh-best99-par', 'HFresh, postings de 8 en 8', 0.99),
    ('r512', 'posting-edge, códigos de ruteo en RAM', None),
    ('halo512', 'posting-edge, halo', None),
)
CELLS = (('r512', 'códigos de ruteo en RAM'), ('halo512', 'halo'))
HF = {False: 'hfresh-best%d', True: 'hfresh-best%d-par'}


def load(d, name):
    path = os.path.join(d, name)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def num(x):
    """An integer with its thousands apart."""
    s = '%.0f' % x
    groups = []
    while len(s) > 3:
        groups.insert(0, s[-3:])
        s = s[:-3]
    return ' '.join([s] + groups)


def gb(x):
    return '%.2f GB' % (x / 1e9)


def mb(x):
    return '%.1f MB' % (x / 1e6)


def signed(rel):
    return '%s%.1f %%' % ('+' if rel >= 0 else '−', abs(100 * rel))


def middle(passes):
    if not passes:
        return None
    return sorted(passes, key=lambda p: p['qps'])[len(passes) // 2]


def measure(result, target, threads, cold):
    for p in (result or {}).get('points') or []:
        if abs(p['target'] - target) < 1e-9 and p['threads'] == threads:
            return p.get('cold') if cold else middle(p.get('warm'))
    return None


def p50(m):
    return None if m is None else m['p50ms']


def cell_of(manifest, name):
    for c in (manifest or {}).get('cells', []):
        if c['spec']['name'] == name:
            return c
    return None


def point_of(cell, target):
    for p in (cell or {}).get('points', []):
        if abs(p['target'] - target) < 1e-9:
            return p
    return None


def hf_curve(target, par):
    return HF[par] % round(100 * target)


def conditions(results, big, small):
    print('### Las dos condiciones')
    print()
    print('La ventaja en frío a 0.95 frente a HFresh de 8 en 8 es de %.0f veces o más, por el p50 con 1 thread de la primera '
          'ronda, en cada una de las dos celdas; y las cargas por query a 0.95 no crecen más de un %.0f %% frente a 1M.'
          % (ADVANTAGE_BAR, 100 * (LOADS_BAR - 1)))
    print()
    print('| condición | medido | hacía falta | |')
    print('|---|---|---|---|')
    met = []
    par = p50(measure(results.get(('n1', hf_curve(0.95, True))), 0.95, 1, True))
    for name, label in CELLS:
        mine = p50(measure(results.get(('n1', name)), 0.95, 1, True))
        if par is None or mine is None:
            print('| en frío a 0.95, HFresh de 8 en 8 entre la celda de %s | sin cronometrar | %.0f o más | |' % (label, ADVANTAGE_BAR))
            met.append(None)
            continue
        ok = par / mine >= ADVANTAGE_BAR
        met.append(ok)
        print('| en frío a 0.95, HFresh de 8 en 8 entre la celda de %s | %.2f ms entre %.2f: %.2fx | %.0f o más | %s |' % (
            label, par, mine, par / mine, ADVANTAGE_BAR, '**sí**' if ok else '**no**'))
    a, b = point_of(cell_of(big, 'r512'), 0.95), point_of(cell_of(small, 'r512'), 0.95)
    if a and b and a.get('reached') and b.get('reached'):
        ratio = a['loads'] / b['loads']
        ok = ratio <= LOADS_BAR
        met.append(ok)
        print('| cargas por query a 0.95, a 10M entre las de 1M | %.2f entre %.2f: %.3f, %s | %.2f o menos | %s |' % (
            a['loads'], b['loads'], ratio, signed(ratio - 1), LOADS_BAR, '**sí**' if ok else '**no**'))
    else:
        met.append(None)
        print('| cargas por query a 0.95, a 10M entre las de 1M | sin medir | %.2f o menos | |' % LOADS_BAR)
    print()
    if None not in met:
        print('**%s**' % ('El pre-registro se cumple: las dos condiciones, en las dos celdas.' if all(met)
                          else 'El pre-registro no se cumple entero.'))
        print()


def cells_table(big, small):
    if not big and not small:
        return
    print('### Nuestras celdas: sus puntos, a 1M y a 10M, sin cronometrar')
    print()
    for name, label in CELLS:
        tried = (cell_of(big, name) or {}).get('tried') or []
        if len(tried) > 1:
            print('- **La celda de %s, a 10M, por rescore:** %s.' % (label, '; '.join(
                'con %d, el mayor recall es %.4f%s' % (t['rescoreLimit'], t['maxRecall'], '' if t['reached'] else ', y no llega a 0.99')
                for t in tried)))
    if big and not small:
        print('- **No hay celdas de 1M con el rescore de las de 10M:** las cargas no se comparan.')
    print()
    print('| celda | corpus | recall | ef | rescore | recall medido | cargas por query | rondas | lecturas del store | '
          'bytes del store | frente a 1M, cargas |')
    print('|---|---|---|---|---|---|---|---|---|---|---|')
    for name, label in CELLS:
        for target in TARGETS:
            base = point_of(cell_of(small, name), target)
            for corpus, manifest in (('1M', small), ('10M', big)):
                p = point_of(cell_of(manifest, name), target)
                if not p:
                    continue
                if not p.get('reached'):
                    print('| %s | %s | %.2f | no llega: el mayor es %.4f | | | | | | | |' % (
                        label, corpus, target, cell_of(manifest, name).get('maxRecall', 0)))
                    continue
                rel = ''
                if corpus == '10M' and base and base.get('reached'):
                    rel = '%.3f, %s' % (p['loads'] / base['loads'], signed(p['loads'] / base['loads'] - 1))
                print('| %s | %s | %.2f | %d | %d | %.4f | %.2f | %.2f | %.1f | %.2f MB | %s |' % (
                    label, corpus, target, p['ef'], p['rescoreLimit'], p['recall'], p['loads'], p['rounds'],
                    p['storeReads'], p['storeBytes'] / 1e6, rel))
    print()
    print('| celda | corpus | postings | aristas | store | vectores | residente en RAM | build, s |')
    print('|---|---|---|---|---|---|---|---|')
    for corpus, manifest in (('1M', small), ('10M', big)):
        for name, label in CELLS:
            c = cell_of(manifest, name)
            if not c:
                continue
            print('| %s | %s | %s | %s | %s | %s | %s | %s |' % (
                label, corpus, num(c['meta']['postings']), num(c['meta']['edges']), gb(c['files']['store']),
                gb(c['files']['vectors']), mb(c['files']['resident']), num(manifest['build']['seconds'])))
    print()


def hfresh_table(manifest, reads):
    if not manifest:
        return
    print('### HFresh a 10M, como en un nodo, sin cronometrar')
    print()
    cfg = manifest.get('config') or {}
    rq = cfg.get('rq') or {}
    print('- **El índice:** %s vectores de %d dimensiones, %s postings, %s entradas, %.2f por vector; postings de %d a %d '
          'entradas de %d bytes. Réplicas %s, maxPostingSizeKB %s, RQ de %s bits.' % (
              num(manifest['vectors']), manifest['dims'], num(manifest['postings']), num(manifest['entries']),
              manifest['replication'], manifest['minPostingSize'], manifest['maxPostingSize'], manifest['entryBytes'],
              cfg.get('replicas'), cfg.get('maxPostingSizeKB'), rq.get('bits')))
    if manifest.get('buildSeconds'):
        print('- **El build:** %s s de inserts y %s de mantenimiento.' % (
            num(manifest['buildSeconds']), num(manifest.get('maintenanceSeconds', 0))))
    cleaned = manifest.get('cleaned')
    if cleaned:
        print('- **El grafo de centroides, limpio:** de %s nodos, %s de ellos borrados, a %s.' % (
            num(cleaned['before']['nodes']), num(cleaned['before']['tombstones']), num(cleaned['after']['nodes'])))
    files = manifest.get('files') or {}
    if files:
        print('- **En disco:** %s. %s' % (gb(sum(files.values())), '; '.join(
            '%s, %s' % (k.replace('bucket ', ''), gb(v)) for k, v in sorted(files.items(), key=lambda kv: -kv[1]))))
    print()
    if not reads:
        return
    print('| configuración | recall | searchProbe | rescore | recall medido | postings leídos | bytes de postings | lecturas '
          'de rescore | lecturas por query | |')
    print('|---|---|---|---|---|---|---|---|---|---|')
    best = set((reads.get('best') or {}).values())
    for c in reads['counts']:
        print('| %s | %s | %d | %d | %.4f | %.1f | %.2f MB | %d | %.1f | %s |' % (
            c['name'], '%.2f' % c['target'] if c['target'] else '—', c['searchProbe'], c['rescoreLimit'], c['recall'],
            c['postingReads'], c['postingBytes'] / 1e6, c['rescoreReads'], c['reads'],
            '**la mejor**' if c['name'] in best else ('no llega' if not c.get('reached', True) else '')))
    print()


def latency(results):
    if not any(results.values()):
        return
    print('### Latencia, primera ronda')
    print()
    for target in TARGETS:
        for cold, title in ((True, 'en frío'), (False, 'en caliente')):
            rows = []
            for curve, label, only in CURVES:
                if only is not None and abs(only - target) > 1e-9:
                    continue
                r = results.get(('n1', curve))
                one, eight = measure(r, target, 1, cold), measure(r, target, 8, cold)
                if not one or not eight:
                    continue
                rows.append('| %s | %.2f | %.2f | %.2f | %s | %.2f | %.2f | %.2f | %s |' % (
                    label, one['p50ms'], one['p95ms'], one['p99ms'], num(one['qps']), eight['p50ms'], eight['p95ms'],
                    eight['p99ms'], num(eight['qps'])))
            if not rows:
                continue
            print('**Recall %.2f, %s:**' % (target, title))
            print()
            print('| índice | 1 thread: p50, ms | p95 | p99 | QPS | 8 threads: p50, ms | p95 | p99 | QPS |')
            print('|---|---|---|---|---|---|---|---|---|')
            print('\n'.join(rows))
            print()
    print('**HFresh entre cada celda,** cuántas veces el p50 de la celda:')
    print()
    print('| recall | HFresh | celda | en frío, 1 thread | en frío, 8 threads | en caliente, 1 thread | en caliente, 8 threads |')
    print('|---|---|---|---|---|---|---|')
    for target in TARGETS:
        for par in (False, True):
            hf = results.get(('n1', hf_curve(target, par)))
            for name, label in CELLS:
                mine = results.get(('n1', name))
                cells = []
                for cold in (True, False):
                    for threads in (1, 8):
                        a, b = p50(measure(hf, target, threads, cold)), p50(measure(mine, target, threads, cold))
                        cells.append('%.2fx' % (a / b) if a and b else '')
                if any(cells):
                    print('| %.2f | %s | %s | %s |' % (
                        target, 'postings de 8 en 8' if par else 'postings uno a uno', label, ' | '.join(cells)))
    print()


def stretches(d, label, results):
    path = os.path.join(d, label + '-stretches.tsv')
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
    for curve, label_, only in CURVES:
        spans, active = [], []
        for r in ROUNDS:
            row = by.get(curve, {}).get(r)
            if not row:
                spans.append('')
                continue
            spans.append('%s a %s' % (row['start'][5:16], row['end'][11:16]))
            if int(row['idle at end']) < int(row['seconds']):
                active.append(r)
        worst, where = 0.0, ''
        a, b = results.get(('n1', curve)), results.get(('n2', curve))
        for target in TARGETS:
            for threads in (1, 8):
                for cold, title in ((True, 'en frío'), (False, 'en caliente')):
                    x, y = p50(measure(a, target, threads, cold)), p50(measure(b, target, threads, cold))
                    if x and y and abs(y / x - 1) > abs(worst):
                        worst = y / x - 1
                        where = '%.2f, %d thread%s, %s: %.2f y %.2f ms' % (
                            target, threads, 's' if threads > 1 else '', title, x, y)
        marks = []
        if abs(worst) >= DRIFT:
            marks.append('**deriva**')
        if active:
            marks.append('**actividad**')
        name = label_ if only is None else '%s, a %.2f' % (label_, only)
        print('| %s | %s | %s | %s | %s | %s |' % (
            name, spans[0], spans[1] if len(spans) > 1 else '',
            'sí, en la ronda %s' % ' y '.join(r[1:] for r in active) if active else 'no',
            '%s (%s)' % (signed(worst), where) if where else '', ', '.join(marks) or '—'))
    print()
    print('- **Deriva:** alguna mediana de la curva se separa un 10 % o más entre las dos rondas, sobre la primera.')
    print('- **Actividad:** al acabar el tramo, el teclado y el trackpad llevaban sin usarse menos de lo que duró.')
    print()


def ram_and_disk(results, big, hf_manifest):
    rows = []
    for curve, label, only in CURVES:
        r = results.get(('n1', curve))
        if not r:
            continue
        name = label if only is None else '%s, a %.2f' % (label, only)
        files = r.get('files') or {}
        rows.append('| %s | %s | %s | %s |' % (name, mb(r['heapBytes']), mb(r['footprintBytes']), gb(sum(files.values()))))
    if rows:
        print('### RAM y disco de los seis procesos')
        print()
        print('| índice | heap en uso tras abrir | huella del proceso | en disco |')
        print('|---|---|---|---|')
        print('\n'.join(rows))
        print()
        print('- **El heap lleva lo del arnés:** las queries y el manifiesto.')
        print()


def main():
    d, label, label1m = sys.argv[1], sys.argv[2], sys.argv[3]
    big = load(d, label + '-manifest.json')
    # The cells of 1M that were searched with the rescore limit of those of
    # 10M: their manifest carries it in its name.
    small = None
    cell = cell_of(big, 'r512')
    if cell:
        small = load(d, '%s-r%d-manifest.json' % (label1m, cell['maxRescoreLimit']))
    if small is None:
        candidates = sorted(name for name in os.listdir(d) if name.startswith(label1m + '-r') and name.endswith('-manifest.json'))
        small = load(d, candidates[0]) if candidates and not cell else None
    hf_manifest = load(d, label + '-hfresh-manifest.json')
    reads = load(d, label + '-hfresh-reads.json')
    results = {(r, c): load(d, '%s-%s-%s.json' % (label, r, c)) for r in ROUNDS for c, _, _ in CURVES}
    conditions(results, big, small)
    cells_table(big, small)
    hfresh_table(hf_manifest, reads)
    if not any(results.values()):
        print('### La sesión cronometrada')
        print()
        print('Sin cronometrar todavía.')
        return
    latency(results)
    stretches(d, label, results)
    ram_and_disk(results, big, hf_manifest)


if __name__ == '__main__':
    main()
