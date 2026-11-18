#!/usr/bin/env python3
"""The verdict of v4.0, the first prototype that reads from disk
(bench-report.md, "v4.0: primer prototipo que lee de disco"), from the files
run_v40_disk.sh leaves.

usage: disk_verdict.py <dir of the run> > disk-verdict.md

The cells of the posting-edge index, each written to the LSM store and read
back from it, and a real HFresh with its default configuration, on the same
dataset. The user's pre-registration:

1. The bytes the store really reads are within 25 % of the byte model of
   v3.0. If not, stop.
2. The cell with codes of 512 bits in memory has a lower cold latency than
   HFresh at recall 0.95, and reaches 0.99.
3. The halo is not slower than HFresh at 0.95, cold.
4. Report up to which recall the real HFresh gets.

Read as pre-registered: 2 and 3 are decided by the p50 with 1 thread, and the
p95, the p99 and the 8 threads are given next to it.
"""
import json
import os
import sys

LABEL = 'dbpedia-1m-disk'
NAMES = {
    'full': 'código completo en RAM',
    'r512': '512 bits en RAM',
    'r384': '384 bits en RAM',
    'halo512': 'halo de 512 bits',
    'halo512p': 'halo de 512 bits, podado',
}


def load(d, name):
    path = os.path.join(d, name)
    return json.load(open(path)) if os.path.exists(path) else None


def mb(x):
    return '—' if x is None else '%.2f MB' % (x / 1e6)


def ms(x):
    return '—' if x is None else '%.2f' % x


def middle(passes):
    """The pass of the median QPS, of the timed warm passes."""
    if not passes:
        return None
    return sorted(passes, key=lambda p: p['qps'])[len(passes) // 2]


def point_of(result, target, threads):
    if result is None:
        return None
    for p in result['points']:
        if abs(p['target'] - target) < 1e-9 and p['threads'] == threads:
            return p
    return None


def more(x, base):
    if x is None or not base:
        return '—'
    pct = 100 * (x / base - 1)
    return 'un %.0f %% %s' % (abs(pct), 'más' if pct >= 0 else 'menos')


def times(x, base):
    return '—' if x is None or not base else '%.2fx' % (x / base)


def latency_row(name, param, p, lat, parts):
    if lat is None:
        return '| %s | %s | — | — | — | — | — |' % (name, param)
    return '| %s | %s | %s | %s | %s | %.0f | %s |' % (
        name, param, ms(lat['p50ms']), ms(lat['p95ms']), ms(lat['p99ms']), lat['qps'],
        ', '.join('%s %s' % (label, ms(lat[key])) for label, key in parts))


PE_PARTS = (('bloques', 'fetchms'), ('scan y ruteo', 'visitms'), ('rescore', 'rescorems'), ('centroides', 'centroidms'))
HF_PARTS = (('postings', 'readms'), ('scan', 'scanms'), ('rescore', 'rescorems'), ('centroides', 'centroidms'))


def main(d):
    manifest = load(d, LABEL + '-manifest.json')
    hf_manifest = load(d, LABEL + '-hfresh-manifest.json')
    hf_reads = load(d, LABEL + '-hfresh-reads.json')
    hf = load(d, LABEL + '-hfresh.json')
    if manifest is None:
        print('no manifest: the write stage has not run')
        return
    cells = manifest['cells']
    timed = {c['spec']['name']: load(d, '%s-%s.json' % (LABEL, c['spec']['name'])) for c in cells}
    first = cells[0]['spec']['name']
    again = load(d, '%s-%s-again.json' % (LABEL, first))
    some = next((t for t in timed.values() if t is not None), None)

    print('### Estado')
    print()
    print('- **Dataset:** %s, %d vectores de %d dimensiones, %d queries, k = %d.' % (
        manifest['dataset'], manifest['vectors'], manifest['dims'], manifest['queries'], manifest['k']))
    print('- **Celdas de posting-edge:** %s.' % ', '.join(NAMES.get(c['spec']['name'], c['spec']['name']) for c in cells))
    print('- **Cronometradas:** %s.' % (', '.join(n for n, t in timed.items() if t is not None) or 'ninguna todavía'))
    print('- **HFresh:** %s.' % ('construido%s' % (', cronometrado' if hf else ', sin cronometrar') if hf_manifest else 'sin construir'))
    if some:
        print('- **Acceso del store:** %s. **Presupuesto de concurrencia por query:** %d. **GOMAXPROCS:** %d.' % (
            some['access'], some['budget'], some['gomaxprocs']))
        print('- **Cómo se tira la caché antes de cada tanda en frío:** `%s`' % (some['drop'] or 'no se tira: sin medida en frío'))
    print()

    print('### Corrección: cada celda en disco frente a su equivalente en memoria')
    print()
    print('| celda | recall objetivo | ef | recall | queries comparadas | resultado |')
    print('|---|---|---|---|---|---|')
    for c in cells:
        for p in c['points']:
            if not p['reached']:
                print('| %s | %.2f | — | — | — | no llega: recall máximo %.4f con rescoreLimit %d |' % (
                    NAMES.get(c['spec']['name'], c['spec']['name']), p['target'], c['maxRecall'], c['maxRescoreLimit']))
                continue
            print('| %s | %.2f | %d | %.4f | %d | los mismos ids, distancias y trabajo |' % (
                NAMES.get(c['spec']['name'], c['spec']['name']), p['target'], p['ef'], p['recall'], p['compared']))
    print()
    print('La escritura para si alguna query difiere: lo que hay en el manifiesto pasó.')
    print()

    print('### Criterio 1: lecturas reales frente al modelo de v3.0')
    print()
    print('Por query, del store en modo pread, que es donde cuenta. El rescore, aparte.')
    print()
    print('| celda | recall | cargas | rondas | lecturas del store | bytes del store | modelo de v3.0 | real entre modelo | '
          'miembros | aristas | halo | rescore: lecturas | bytes |')
    print('|---|---|---|---|---|---|---|---|---|---|---|---|---|')
    worst = None
    vector_bytes = manifest['dims'] * 4
    for c in cells:
        for p in c['points']:
            if not p['reached']:
                continue
            ratio = p['realOverModel']
            if worst is None or abs(ratio - 1) > abs(worst - 1):
                worst = ratio
            print('| %s | %.2f | %.2f | %.2f | %.1f | %s | %s | %.3f | %s | %s | %s | %.0f | %s |' % (
                NAMES.get(c['spec']['name'], c['spec']['name']), p['target'], p['loads'], p['rounds'], p['storeReads'],
                mb(p['storeBytes']), mb(p['modelBytes']), ratio, mb(p['memberBytes']), mb(p['edgeBytes']),
                mb(p['haloBytes']) if p['haloBytes'] else '—', p['rescored'], mb(p['rescored'] * vector_bytes)))
    print()
    if worst is not None:
        ok = 0.75 <= worst <= 1.25
        print('**%s:** la celda que más se separa lee %.3f veces su modelo, y el margen es de 0.75 a 1.25.' % (
            'Se cumple' if ok else 'NO se cumple: stop', worst))
    print()

    print('### HFresh real')
    print()
    if hf_manifest is None:
        print('Sin construir.')
        print()
    else:
        cfg = hf_manifest['config']
        print('- **Configuración por defecto:** réplicas %d, posting máximo de %d KB (%d entradas, mínimo %d), searchProbe %d, distancia %s.' % (
            cfg['replicas'], cfg['maxPostingSizeKB'], hf_manifest['maxPostingSize'], hf_manifest['minPostingSize'],
            cfg['searchProbe'], cfg['distance']))
        print('- **Códigos:** RQ de %d bit. Una entrada de posting son %d B: id, versión y código.' % (
            cfg['rq']['bits'], hf_manifest['entryBytes']))
        print('- **Rescore:** los %d mejores candidatos, con su vector exacto del bucket `objects`.' % cfg['rq']['rescoreLimit'])
        print('- **El build:** %d postings y %d entradas, %.2f por vector. Inserts en %.0f s, mantenimiento en %.0f s, %d compactaciones.' % (
            hf_manifest['postings'], hf_manifest['entries'], hf_manifest['replication'], hf_manifest['buildSeconds'],
            hf_manifest['maintenanceSeconds'], hf_manifest['compactionRounds']))
        print('- **Segmentos por bucket:** %s.' % ', '.join(
            '%s %d' % (k.replace('bucket ', ''), v) for k, v in sorted(hf_manifest['segments'].items())))
        print()
        print('**Criterio 4: hasta qué recall llega.** Con el rescore por defecto, subiendo solo searchProbe:')
        print()
        print('| searchProbe | recall |')
        print('|---|---|')
        doubling = [s for s in hf_manifest['sweep'] if s['searchProbe'] == hf_manifest['postings'] or
                    (s['searchProbe'] & (s['searchProbe'] - 1)) == 0]
        for s in doubling:
            print('| %d%s | %.4f |' % (s['searchProbe'], ' (todos los postings)' if s['searchProbe'] == hf_manifest['postings'] else
                                       (' (por defecto)' if s['searchProbe'] == cfg['searchProbe'] else ''), s['recall']))
        print()
        print('- **Recall máximo: %.4f.**' % hf_manifest['maxRecall'])
        for p in hf_manifest['points']:
            if p['reached']:
                print('- **Llega a %.2f con searchProbe %d** (recall %.4f).' % (p['target'], p['searchProbe'], p['recall']))
            else:
                print('- **No llega a %.2f.**' % p['target'])
        print()
        if hf_reads:
            print('**Lo que lee por query,** del store en modo pread:')
            print()
            print('| recall | searchProbe | postings: lecturas | bytes | rescore: lecturas | bytes |')
            print('|---|---|---|---|---|---|')
            for r in hf_reads:
                print('| %.2f | %d | %.1f | %s | %.1f | %s |' % (
                    r['target'], r['searchProbe'], r['postingReads'], mb(r['postingBytes']), r['rescoreReads'], mb(r['rescoreBytes'])))
            print()

    for state, key in (('en caliente', 'warm'), ('en frío', 'cold')):
        print('### Latencia y QPS, %s' % state)
        print()
        if key == 'warm':
            print('Tras una pasada sin cronometrar. La pasada de QPS mediano de las cronometradas.')
        else:
            print('Cada tanda, tras tirar la caché y reabrir el índice, con el índice de los segmentos ya leído.')
        print()
        for target in (0.95, 0.99):
            for threads in (1, 8):
                print('**Recall %.2f, %d %s:**' % (target, threads, 'thread' if threads == 1 else 'threads'))
                print()
                print('| índice | punto | p50, ms | p95 | p99 | QPS | partes de la latencia media, ms |')
                print('|---|---|---|---|---|---|---|')
                for c in cells:
                    name = c['spec']['name']
                    p = point_of(timed[name], target, threads)
                    if p is None:
                        continue
                    lat = middle(p['warm']) if key == 'warm' else p.get('cold')
                    print(latency_row(NAMES.get(name, name), 'ef %d' % p['ef'], p, lat, PE_PARTS))
                p = point_of(hf, target, threads)
                if p is not None:
                    lat = middle(p['warm']) if key == 'warm' else p.get('cold')
                    print(latency_row('HFresh', 'searchProbe %d' % p['searchProbe'], p, lat, HF_PARTS))
                print()
        if key == 'cold':
            print('**Las tandas en frío:**')
            print()
            print('| índice | recall | threads | tandas | queries por tanda | cargas que repiten posting | p50 de cada mitad, ms | reapertura, ms |')
            print('|---|---|---|---|---|---|---|---|')
            for c in cells:
                name = c['spec']['name']
                for p in (timed[name] or {'points': []})['points']:
                    if not p.get('cold'):
                        continue
                    print('| %s | %.2f | %d | %d | %d | %.0f %% | %s y %s | %.0f |' % (
                        NAMES.get(name, name), p['target'], p['threads'], p['coldBatches'], p['coldBatch'],
                        100 * p['coldRepeated'], ms(p['coldHalves'][0]['p50ms']), ms(p['coldHalves'][1]['p50ms']), p['reopenms']))
            for p in (hf or {'points': []})['points']:
                if not p.get('cold'):
                    continue
                print('| HFresh | %.2f | %d | %d | %d | — | %s y %s | %.0f |' % (
                    p['target'], p['threads'], p['coldBatches'], p['coldBatch'],
                    ms(p['coldHalves'][0]['p50ms']), ms(p['coldHalves'][1]['p50ms']), p['reopenms']))
            print()
            print('**La primera query de cada tanda frente a las demás,** con 1 thread. La primera es la que menos encuentra en la caché:')
            print()
            print('| índice | recall | primera: p50, ms | media | las demás: p50, ms | media | rescore de la primera, ms | de las demás |')
            print('|---|---|---|---|---|---|---|---|')
            rows = [(NAMES.get(c['spec']['name'], c['spec']['name']), timed[c['spec']['name']]) for c in cells] + [('HFresh', hf)]
            for name, result in rows:
                for p in (result or {'points': []})['points']:
                    if not p.get('coldFirst'):
                        continue
                    a, b = p['coldFirst'], p['coldRest']
                    print('| %s | %.2f | %s | %s | %s | %s | %s | %s |' % (
                        name, p['target'], ms(a['p50ms']), ms(a['meanms']), ms(b['p50ms']), ms(b['meanms']),
                        ms(a['rescorems']), ms(b['rescorems'])))
            print()

    print('### Criterios 2 y 3: en frío, frente a HFresh, a 0.95')
    print()
    hf95 = point_of(hf, 0.95, 1)
    if hf95 is None or not hf95.get('cold'):
        if hf_manifest and not hf_manifest['points'][0]['reached']:
            print('**Sin veredicto a 0.95:** HFresh no llega a 0.95 (recall máximo %.4f).' % hf_manifest['maxRecall'])
        else:
            print('Sin medida en frío de HFresh todavía.')
        print()
    else:
        base = hf95['cold']
        spread_hf = abs(hf95['coldHalves'][0]['p50ms'] - hf95['coldHalves'][1]['p50ms'])
        print('HFresh, en frío con 1 thread: p50 %s ms, p95 %s, p99 %s. Sus dos mitades de tandas se separan %s ms.' % (
            ms(base['p50ms']), ms(base['p95ms']), ms(base['p99ms']), ms(spread_hf)))
        print()

        def against(name):
            p = point_of(timed.get(name), 0.95, 1)
            if p is None or not p.get('cold'):
                return None
            cold = p['cold']
            spread = max(spread_hf, abs(p['coldHalves'][0]['p50ms'] - p['coldHalves'][1]['p50ms']))
            apart = abs(cold['p50ms'] - base['p50ms']) > spread
            others = []
            for label, k in (('p95', 'p95ms'), ('p99', 'p99ms')):
                others.append('%s %s frente a %s' % (label, ms(cold[k]), ms(base[k])))
            p8, h8 = point_of(timed.get(name), 0.95, 8), point_of(hf, 0.95, 8)
            if p8 and h8 and p8.get('cold') and h8.get('cold'):
                others.append('con 8 threads, p50 %s frente a %s' % (ms(p8['cold']['p50ms']), ms(h8['cold']['p50ms'])))
            return cold['p50ms'], apart, '; '.join(others)

        r512 = against('r512')
        cell512 = next((c for c in cells if c['spec']['name'] == 'r512'), None)
        if r512 is None or cell512 is None:
            print('- **Criterio 2:** sin medida de la celda de 512 bits en RAM.')
        else:
            reaches = cell512['points'][-1]['reached']
            lower = r512[0] < base['p50ms']
            verdict = 'se cumple' if lower and reaches else 'NO se cumple'
            print('- **Criterio 2, la celda de 512 bits en RAM: %s.** p50 de %s ms frente a %s de HFresh (%s)%s. %s 0.99%s. Además: %s.' % (
                verdict, ms(r512[0]), ms(base['p50ms']), times(r512[0], base['p50ms']),
                '' if r512[1] else ', que no se separan de lo que se separan las mitades de las tandas',
                'Llega a' if reaches else 'No llega a', '' if not reaches else ' con ef %d' % cell512['points'][-1]['ef'], r512[2]))
        halos = [(n, against(n)) for n in ('halo512', 'halo512p')]
        halos = [(n, a) for n, a in halos if a is not None]
        if not halos:
            print('- **Criterio 3:** sin medida de las celdas de halo.')
        else:
            ok = [n for n, a in halos if a[0] <= base['p50ms']]
            print('- **Criterio 3, el halo: %s.**' % ('se cumple, con %s' % ' y '.join(NAMES[n] for n in ok) if ok else 'NO se cumple'))
            for n, a in halos:
                print('  - %s: p50 de %s ms frente a %s (%s)%s. Además: %s.' % (
                    NAMES[n].capitalize(), ms(a[0]), ms(base['p50ms']), times(a[0], base['p50ms']),
                    '' if a[1] else ', que no se separan', a[2]))
        print()

    print('### RAM residente y disco')
    print()
    print('| índice | heap en uso tras abrir | huella del proceso | disco: store | vectores exactos | lo residente, en ficheros | total |')
    print('|---|---|---|---|---|---|---|')
    for c in cells:
        name = c['spec']['name']
        t = timed[name]
        files = c['files']
        total = sum(files.values())
        print('| %s | %s | %s | %s | %s | %s | %s |' % (
            NAMES.get(name, name), mb(t['heapBytes']) if t else '—', mb(t['footprintBytes']) if t else '—',
            mb(files['store']), mb(files['vectors']), mb(files['resident']), mb(total)))
    if hf_manifest:
        files = hf_manifest['files']
        vectors = files.get('bucket objects', 0)
        store = sum(v for k, v in files.items() if k.startswith('bucket ') and k != 'bucket objects')
        rest = sum(v for k, v in files.items() if not k.startswith('bucket '))
        print('| HFresh | %s | %s | %s | %s | %s | %s |' % (
            mb(hf['heapBytes']) if hf else '—', mb(hf['footprintBytes']) if hf else '—', mb(store), mb(vectors), mb(rest),
            mb(sum(files.values()))))
    print()
    print('- **Las celdas de posting-edge comparten un solo fichero de vectores,** enlazado: en disco está una vez.')
    print('- **El heap es el de Go tras abrir el índice y recolectar.** La caché del sistema no cuenta.')
    print('- **En HFresh, «vectores exactos» es su bucket `objects`,** y «store» sus otros buckets.')
    print()

    if again is not None and timed.get(first) is not None:
        print('### Deriva: la primera celda otra vez, al final')
        print()
        print('| recall | threads | p50 al principio, ms | al final | QPS al principio | al final |')
        print('|---|---|---|---|---|---|')
        for p in timed[first]['points']:
            q = point_of(again, p['target'], p['threads'])
            if q is None:
                continue
            a, b = middle(p['warm']), middle(q['warm'])
            print('| %.2f | %d | %s | %s (%s) | %.0f | %.0f (%s) |' % (
                p['target'], p['threads'], ms(a['p50ms']), ms(b['p50ms']), more(b['p50ms'], a['p50ms']),
                a['qps'], b['qps'], more(b['qps'], a['qps'])))
        print()

    print('### Lo que vale y lo que no')
    print()
    print('- **Los números absolutos de esta máquina no valen para producción.** Vale la comparación entre índices, medidos igual.')
    print('- **Es una sesión,** sin repetir.')
    print('- **El p99 en frío sale de las 5 queries más lentas de 500.**')


if __name__ == '__main__':
    main(sys.argv[1])
