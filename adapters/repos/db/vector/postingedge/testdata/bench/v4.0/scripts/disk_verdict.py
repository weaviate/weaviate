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
p95, the p99 and the 8 threads are given next to it. By the user's amendment
of the 18th HFresh is measured twice, with its default configuration and with
the searchProbe and rescore limit that read the least for each recall, and
the criteria are read against the second.

By the fifth amendment the session has two rounds. A curve is one index timed
in a process of its own: a cell of posting-edge, or one configuration of
HFresh. The first round is the measure and the second tells drift: a curve
drifts when any of its medians moves by 10 % or more between the two. A curve
coincides with user activity when the keyboard or the trackpad were used
during its stretch in either round. A curve marked for either reason is
repeated at night (round "night"), and the repeat then stands for it. Under
10 % between two indexes is a tie: it does not meet "lower than" (criterion
2) and it meets "not slower than" (criterion 3).
"""
import json
import os
import sys

LABEL = 'dbpedia-1m-disk'
# Under this, two indexes are tied and two rounds agree. Between two indexes
# it is taken over HFresh, and between two rounds over the first.
TIE = 0.10
HF_CURVES = (
    ('hfresh-best95', 'HFresh, la mejor a 0.95'),
    ('hfresh-best99', 'HFresh, la mejor a 0.99'),
    ('hfresh-default', 'HFresh por defecto'),
)
ROUND_NAMES = {'r1': 'ronda 1', 'r2': 'ronda 2', 'night': 'repetición de noche'}
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


def signed(rel):
    return '%s%.1f %%' % ('+' if rel >= 0 else '−', abs(100 * rel))


def times(x, base):
    return '—' if x is None or not base else '%.2fx' % (x / base)


def versus(x, base):
    """How x stands to base: 'menos', 'más', or 'empate' under TIE."""
    rel = x / base - 1
    if abs(rel) < TIE:
        return 'empate'
    return 'menos' if rel < 0 else 'más'


def medians(result):
    """The medians of a curve: (point, threads, state) -> p50 in ms."""
    out = {}
    for p in (result or {}).get('points', []):
        point = '%.2f' % p['target'] if 'ef' in p else 'recall %.4f' % p['recall']
        warm = middle(p.get('warm'))
        if warm:
            out[(point, p['threads'], 'caliente')] = warm['p50ms']
        if p.get('cold'):
            out[(point, p['threads'], 'frío')] = p['cold']['p50ms']
    return out


def read_stretches(d):
    """What run_v40_disk.sh left about every stretch: (round, curve) -> its
    line, the last one when a stretch was run again."""
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
    """Keyboard or trackpad during the stretch: at its end they had been idle
    for less than it lasted. None when there is no record."""
    if stretch is None or stretch['idle1'] < 0:
        return None
    return stretch['idle1'] < stretch['seconds']


def clock(stretch):
    return '—' if stretch is None else '%s a %s' % (stretch['start'][11:16], stretch['end'][11:16])


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
    if manifest is None:
        print('no manifest: the write stage has not run')
        return
    cells = manifest['cells']
    # The curves, and what every round left of each.
    curves = [(c['spec']['name'], NAMES.get(c['spec']['name'], c['spec']['name'])) for c in cells] + list(HF_CURVES)
    rounds = {name: {r: load(d, '%s-%s-%s.json' % (LABEL, r, name)) for r in ROUND_NAMES} for name, _ in curves}
    stretches = read_stretches(d)

    def measure(name):
        """The round a curve is read from: its repeat at night when there is
        one, the first round otherwise."""
        return 'night' if rounds[name]['night'] is not None else 'r1'

    def drift(name):
        """The medians of the curve in the two rounds, and whether any moved
        by TIE or more. None when a round is missing."""
        a, b = medians(rounds[name]['r1']), medians(rounds[name]['r2'])
        both = [(k, a[k], b[k], b[k] / a[k] - 1) for k in a if k in b]
        if not both:
            return None
        return both, any(abs(rel) >= TIE for _, _, _, rel in both)

    def activity(name):
        """Whether the keyboard or the trackpad were used in a stretch of the
        curve in either round: the rounds where they were, or None when a
        round has no record."""
        seen = [used(stretches.get((r, name))) for r in ('r1', 'r2')]
        if any(s is None for s in seen):
            return None
        return [r for r, s in zip(('r1', 'r2'), seen) if s]

    def marked(name):
        d_, a_ = drift(name), activity(name)
        why = []
        if d_ is not None and d_[1]:
            why.append('deriva')
        if a_:
            why.append('actividad')
        return why

    def tag(name):
        """What a row of a table says of where its curve comes from."""
        if measure(name) == 'night':
            return ' (repetida de noche)'
        return ' (marcada)' if marked(name) else ''

    timed = {name: rounds[name][measure(name)] for name, _ in curves}
    some = next((t for t in timed.values() if t is not None), None)
    hf_any = next((timed[name] for name, _ in HF_CURVES if timed[name] is not None), None)

    print('### Estado')
    print()
    print('- **Dataset:** %s, %d vectores de %d dimensiones, %d queries, k = %d.' % (
        manifest['dataset'], manifest['vectors'], manifest['dims'], manifest['queries'], manifest['k']))
    print('- **Celdas de posting-edge:** %s.' % ', '.join(NAMES.get(c['spec']['name'], c['spec']['name']) for c in cells))
    for r, label in ROUND_NAMES.items():
        done = [name for name, _ in curves if rounds[name][r] is not None]
        spans = [stretches[(r, name)] for name in done if (r, name) in stretches]
        if done:
            print('- **Cronometradas en la %s:** %s%s.' % (
                label, ', '.join(done),
                '' if not spans else ', de %s a %s' % (min(s['start'] for s in spans)[:16], max(s['end'] for s in spans)[11:16])))
    if not any(t is not None for t in timed.values()):
        print('- **Cronometradas:** ninguna todavía.')
    print('- **HFresh:** %s.' % ('construido%s' % (', cronometrado' if hf_any else ', sin cronometrar') if hf_manifest else 'sin construir'))
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

    # HFresh: its default configuration, and for every recall the candidate
    # that reads the least.
    counts = {c['name']: c for c in (hf_reads or {}).get('counts', [])}
    best = (hf_reads or {}).get('best', {})

    def hf_point(curve, threads):
        for p in (timed.get(curve) or {'points': []})['points']:
            if p['threads'] == threads:
                return p
        return None

    hf_names = dict(HF_CURVES)

    print('### HFresh real')
    print()
    if hf_manifest is None:
        print('Sin construir.')
        print()
    else:
        cfg = hf_manifest['config']
        default = hf_manifest['default']
        print('- **Configuración por defecto:** réplicas %d, posting máximo de %d KB (%d entradas, mínimo %d), searchProbe %d, rescoreLimit %d, distancia %s.' % (
            cfg['replicas'], cfg['maxPostingSizeKB'], hf_manifest['maxPostingSize'], hf_manifest['minPostingSize'],
            cfg['searchProbe'], cfg['rq']['rescoreLimit'], cfg['distance']))
        print('- **Códigos:** RQ de %d bit. Una entrada de posting son %d B: id, versión y código.' % (
            cfg['rq']['bits'], hf_manifest['entryBytes']))
        print('- **Rescore:** los mejores candidatos, %d por defecto, con su vector exacto del bucket `objects`.' % cfg['rq']['rescoreLimit'])
        print('- **El build:** %d postings y %d entradas, %.2f por vector. Inserts en %.0f s, mantenimiento en %.0f s, %d compactaciones.' % (
            hf_manifest['postings'], hf_manifest['entries'], hf_manifest['replication'], hf_manifest['buildSeconds'],
            hf_manifest['maintenanceSeconds'], hf_manifest['compactionRounds']))
        print('- **Segmentos por bucket:** %s.' % ', '.join(
            '%s %d' % (k.replace('bucket ', ''), v) for k, v in sorted(hf_manifest['segments'].items())))
        print('- **Con su configuración por defecto da un recall de %.4f.**' % default['recall'])
        print()
        print('**Criterio 4: hasta qué recall llega.** Recall por searchProbe, con cada rescoreLimit; el de por defecto es %d:' % cfg['rq']['rescoreLimit'])
        print()
        rescores = sorted({s['rescoreLimit'] for s in hf_manifest['sweep']})
        print('| searchProbe | ' + ' | '.join('rescoreLimit %d' % r for r in rescores) + ' | queries |')
        print('|---|' + '---|' * len(rescores) + '---|')
        by = {(s['searchProbe'], s['rescoreLimit']): s for s in hf_manifest['sweep']}
        probes = sorted({s['searchProbe'] for s in hf_manifest['sweep'] if s['searchProbe'] == hf_manifest['postings'] or
                         (s['searchProbe'] & (s['searchProbe'] - 1)) == 0})
        for probe in probes:
            row = ['%.4f' % by[(probe, r)]['recall'] if (probe, r) in by else '—' for r in rescores]
            queries = next(s['queries'] for s in hf_manifest['sweep'] if s['searchProbe'] == probe)
            note = ' (todos los postings)' if probe == hf_manifest['postings'] else (' (por defecto)' if probe == cfg['searchProbe'] else '')
            print('| %d%s | %s | %d |' % (probe, note, ' | '.join(row), queries))
        print()
        print('- **Recall máximo con el rescoreLimit por defecto: %.4f.**' % hf_manifest['maxRecall'])
        print('- Un hueco es un searchProbe que no hizo falta medir: el barrido de un rescoreLimit para cuando llega a 0.99.')
        print()
        print('**Las candidatas: el menor searchProbe que llega a cada recall, por rescoreLimit.** Se elige la que menos lecturas hace.')
        print()
        print('| recall | rescoreLimit | searchProbe | recall medido | lecturas por query | postings: lecturas | bytes | rescore: lecturas | bytes | |')
        print('|---|---|---|---|---|---|---|---|---|---|')
        for c in hf_manifest['candidates']:
            if not c['reached']:
                print('| %.2f | %d | no llega | — | — | — | — | — | — | |' % (c['target'], c['rescoreLimit']))
                continue
            r = counts.get(c['name'])
            mark = '**la mejor**' if best.get('%.2f' % c['target']) == c['name'] else ''
            if r is None:
                print('| %.2f | %d | %d | %.4f | sin contar | — | — | — | — | |' % (c['target'], c['rescoreLimit'], c['searchProbe'], c['recall']))
                continue
            print('| %.2f | %d | %d | %.4f | %.1f | %.1f | %s | %.1f | %s | %s |' % (
                c['target'], c['rescoreLimit'], c['searchProbe'], c['recall'], r['reads'], r['postingReads'], mb(r['postingBytes']),
                r['rescoreReads'], mb(r['rescoreBytes']), mark))
        r = counts.get('default')
        if r is not None:
            print('| por defecto: %.4f | %d | %d | %.4f | %.1f | %.1f | %s | %.1f | %s | |' % (
                default['recall'], default['rescoreLimit'], default['searchProbe'], default['recall'], r['reads'], r['postingReads'],
                mb(r['postingBytes']), r['rescoreReads'], mb(r['rescoreBytes'])))
        print()
        for target in (0.95, 0.99):
            if '%.2f' % target not in best and hf_reads is not None:
                print('- **A %.2f no llega con ningún rescoreLimit.**' % target)
        print()

    print('### Las curvas: dos rondas, deriva y actividad de usuario')
    print()
    print('Una curva es un índice cronometrado en un proceso propio. La medida es la primera ronda; la segunda detecta deriva.')
    print()
    print('| curva | ronda 1 | ronda 2 | teclado o trackpad | mayor diferencia de p50 entre rondas | marca | repetición de noche |')
    print('|---|---|---|---|---|---|---|')
    any_marked = []
    for name, label in curves:
        if rounds[name]['r1'] is None and rounds[name]['r2'] is None:
            continue
        seen = activity(name)
        if seen is None:
            act = 'sin registro'
        elif not seen:
            act = 'no'
        else:
            act = 'sí, en la %s' % ' y la '.join(ROUND_NAMES[r] for r in seen)
        dr = drift(name)
        if dr is None:
            worst = 'sin segunda ronda'
        else:
            k, a, b, rel = max(dr[0], key=lambda x: abs(x[3]))
            worst = '%s (%s, %d %s, en %s: %s y %s ms)' % (
                signed(rel), k[0], k[1], 'thread' if k[1] == 1 else 'threads', k[2], ms(a), ms(b))
        why = marked(name)
        if why:
            any_marked.append(name)
        night = stretches.get(('night', name))
        night_text = '—'
        if rounds[name]['night'] is not None:
            night_used = used(night)
            night_text = '%s%s' % (clock(night), '' if not night_used else ', con teclado o trackpad')
        print('| %s | %s | %s | %s | %s | %s | %s |' % (
            label, clock(stretches.get(('r1', name))), clock(stretches.get(('r2', name))), act, worst,
            '**%s**' % ' y '.join(why) if why else '—', night_text))
    print()
    print('- **Deriva:** alguna mediana de la curva se separa un 10 % o más entre las dos rondas, sobre la primera.')
    print('- **Actividad:** al acabar el tramo, el teclado y el trackpad llevaban sin usarse menos de lo que duró.')
    if any_marked:
        pending = [n for n in any_marked if rounds[n]['night'] is None]
        print('- **Curvas marcadas: %s.**%s' % (
            ', '.join(dict(curves)[n] for n in any_marked),
            ' Por repetir esta noche: %s.' % ', '.join(dict(curves)[n] for n in pending) if pending else ' Todas repetidas.'))
    elif any(rounds[name]['r2'] is not None for name, _ in curves):
        print('- **Ninguna curva marcada.**')
    print('- **Una curva repetida de noche se lee de su repetición,** que viene de otra sesión que las curvas con las que se compara.')
    print()
    print('**Las medianas de cada curva en cada ronda, en ms:**')
    print()
    print('| curva | punto | threads | estado | ronda 1 | ronda 2 | diferencia | repetición de noche |')
    print('|---|---|---|---|---|---|---|---|')
    for name, label in curves:
        a, b, n = medians(rounds[name]['r1']), medians(rounds[name]['r2']), medians(rounds[name]['night'])
        for k in a or b or n:
            rel = None if k not in a or k not in b else b[k] / a[k] - 1
            diff = '—' if rel is None else ('**%s**' if abs(rel) >= TIE else '%s') % signed(rel)
            print('| %s | %s | %d | %s | %s | %s | %s | %s |' % (
                label, k[0], k[1], k[2], ms(a.get(k)), ms(b.get(k)), diff, ms(n.get(k))))
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
                    print(latency_row(NAMES.get(name, name) + tag(name), 'ef %d' % p['ef'], p, lat, PE_PARTS))
                for curve in ('hfresh-best%d' % round(100 * target), 'hfresh-default'):
                    p = hf_point(curve, threads)
                    if p is None:
                        continue
                    lat = middle(p['warm']) if key == 'warm' else p.get('cold')
                    label = hf_names[curve] + ('' if curve != 'hfresh-default' else ', recall %.4f' % p['recall']) + tag(curve)
                    print(latency_row(label, 'searchProbe %d, rescoreLimit %d' % (p['searchProbe'], p['rescoreLimit']), p, lat, HF_PARTS))
                print()
        # every cell over the best HFresh of each recall
        print('**Cada celda frente a la mejor configuración de HFresh para ese recall, %s:** su p50 entre el de HFresh. Menos de un 10 %% de diferencia es un empate.' % state)
        print()
        print('| celda | 0.95, 1 thread | 0.95, 8 threads | 0.99, 1 thread | 0.99, 8 threads |')
        print('|---|---|---|---|---|')
        for c in cells:
            name = c['spec']['name']
            row = []
            for target in (0.95, 0.99):
                for threads in (1, 8):
                    p, h = point_of(timed[name], target, threads), hf_point('hfresh-best%d' % round(100 * target), threads)
                    a = None if p is None else (middle(p['warm']) if key == 'warm' else p.get('cold'))
                    b = None if h is None else (middle(h['warm']) if key == 'warm' else h.get('cold'))
                    row.append('—' if not a or not b else '%s, %s' % (times(a['p50ms'], b['p50ms']), versus(a['p50ms'], b['p50ms'])))
            print('| %s | %s |' % (NAMES.get(name, name) + tag(name), ' | '.join(row)))
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
            for curve, label in HF_CURVES:
                for p in (timed[curve] or {'points': []})['points']:
                    if not p.get('cold'):
                        continue
                    print('| %s | %.4f | %d | %d | %d | — | %s y %s | %.0f |' % (
                        label, p['recall'], p['threads'], p['coldBatches'], p['coldBatch'],
                        ms(p['coldHalves'][0]['p50ms']), ms(p['coldHalves'][1]['p50ms']), p['reopenms']))
            print()
            print('**La primera query de cada tanda frente a las demás,** con 1 thread. La primera es la que menos encuentra en la caché:')
            print()
            print('| índice | recall | primera: p50, ms | media | las demás: p50, ms | media | rescore de la primera, ms | de las demás |')
            print('|---|---|---|---|---|---|---|---|')
            rows = [(NAMES.get(c['spec']['name'], c['spec']['name']), timed[c['spec']['name']]) for c in cells]
            for name, result in rows:
                for p in (result or {'points': []})['points']:
                    if not p.get('coldFirst'):
                        continue
                    a, b = p['coldFirst'], p['coldRest']
                    print('| %s | %.2f | %s | %s | %s | %s | %s | %s |' % (
                        name, p['target'], ms(a['p50ms']), ms(a['meanms']), ms(b['p50ms']), ms(b['meanms']),
                        ms(a['rescorems']), ms(b['rescorems'])))
            for curve, label in HF_CURVES:
                for p in (timed[curve] or {'points': []})['points']:
                    if not p.get('coldFirst'):
                        continue
                    a, b = p['coldFirst'], p['coldRest']
                    print('| %s | %.4f | %s | %s | %s | %s | %s | %s |' % (
                        label, p['recall'], ms(a['p50ms']), ms(a['meanms']), ms(b['p50ms']), ms(b['meanms']),
                        ms(a['rescorems']), ms(b['rescorems'])))
            print()

    print('### Criterios 2 y 3: en frío, frente a HFresh, a 0.95')
    print()
    print('Se leen contra la mejor configuración de HFresh a 0.95 (enmienda 4), por el p50 con 1 thread. '
          'Menos de un 10 % de diferencia, sobre HFresh, es un empate (enmienda 5): no cumple «menos latencia» y sí cumple «no más lento».')
    print()
    hf95 = hf_point('hfresh-best95', 1)
    if hf95 is None or not hf95.get('cold'):
        if hf_reads is not None and '0.95' not in best:
            print('**Sin veredicto a 0.95:** HFresh no llega a 0.95 con ningún rescoreLimit.')
        else:
            print('Sin medida en frío de HFresh todavía.')
        print()
    else:
        base = hf95['cold']
        print('- **HFresh, la mejor a 0.95**%s (searchProbe %d, rescoreLimit %d, recall %.4f), en frío con 1 thread: p50 %s ms, p95 %s, p99 %s. Sus dos mitades de tandas: %s y %s ms.' % (
            tag('hfresh-best95'), hf95['searchProbe'], hf95['rescoreLimit'], hf95['recall'], ms(base['p50ms']), ms(base['p95ms']), ms(base['p99ms']),
            ms(hf95['coldHalves'][0]['p50ms']), ms(hf95['coldHalves'][1]['p50ms'])))
        dflt = hf_point('hfresh-default', 1)
        if dflt is not None and dflt.get('cold'):
            print('- **HFresh por defecto**%s (searchProbe %d, rescoreLimit %d, recall %.4f): p50 %s ms, p95 %s, p99 %s.' % (
                tag('hfresh-default'), dflt['searchProbe'], dflt['rescoreLimit'], dflt['recall'], ms(dflt['cold']['p50ms']),
                ms(dflt['cold']['p95ms']), ms(dflt['cold']['p99ms'])))
        print()

        def against(name):
            p = point_of(timed.get(name), 0.95, 1)
            if p is None or not p.get('cold'):
                return None
            cold = p['cold']
            others = ['sus mitades de tandas, %s y %s ms' % (ms(p['coldHalves'][0]['p50ms']), ms(p['coldHalves'][1]['p50ms']))]
            for label, k in (('p95', 'p95ms'), ('p99', 'p99ms')):
                others.append('%s %s frente a %s' % (label, ms(cold[k]), ms(base[k])))
            p8, h8 = point_of(timed.get(name), 0.95, 8), hf_point('hfresh-best95', 8)
            if p8 and h8 and p8.get('cold') and h8.get('cold'):
                others.append('con 8 threads, p50 %s frente a %s (%s, %s)' % (
                    ms(p8['cold']['p50ms']), ms(h8['cold']['p50ms']), times(p8['cold']['p50ms'], h8['cold']['p50ms']),
                    versus(p8['cold']['p50ms'], h8['cold']['p50ms'])))
            notes = []
            if measure(name) != measure('hfresh-best95'):
                notes.append('las dos curvas vienen de sesiones distintas (%s y %s)' % (
                    ROUND_NAMES[measure(name)], ROUND_NAMES[measure('hfresh-best95')]))
            pending = [dict(curves)[n] for n in (name, 'hfresh-best95') if marked(n) and measure(n) != 'night']
            if pending:
                notes.append('curva marcada y sin repetir: %s' % ', '.join(pending))
            return cold['p50ms'], versus(cold['p50ms'], base['p50ms']), '; '.join(others), notes

        def said(a):
            return '%s ms frente a %s de HFresh (%s): %s' % (ms(a[0]), ms(base['p50ms']), times(a[0], base['p50ms']), {
                'menos': 'menos latencia', 'más': 'más latencia', 'empate': 'empate'}[a[1]])

        r512 = against('r512')
        cell512 = next((c for c in cells if c['spec']['name'] == 'r512'), None)
        if r512 is None or cell512 is None:
            print('- **Criterio 2:** sin medida de la celda de 512 bits en RAM.')
        else:
            reaches = cell512['points'][-1]['reached']
            verdict = 'se cumple' if r512[1] == 'menos' and reaches else 'NO se cumple'
            print('- **Criterio 2, la celda de 512 bits en RAM: %s.** p50 de %s. %s 0.99%s. Además: %s.%s' % (
                verdict, said(r512), 'Llega a' if reaches else 'No llega a',
                '' if not reaches else ' con ef %d' % cell512['points'][-1]['ef'], r512[2],
                ''.join(' **Ojo:** %s.' % n for n in r512[3])))
        halos = [(n, against(n)) for n in ('halo512', 'halo512p')]
        halos = [(n, a) for n, a in halos if a is not None]
        if not halos:
            print('- **Criterio 3:** sin medida de las celdas de halo.')
        else:
            ok = [n for n, a in halos if a[1] in ('menos', 'empate')]
            print('- **Criterio 3, el halo: %s.**' % ('se cumple, con %s' % ' y '.join(NAMES[n] for n in ok) if ok else 'NO se cumple'))
            for n, a in halos:
                print('  - %s: p50 de %s. Además: %s.%s' % (
                    NAMES[n].capitalize(), said(a), a[2], ''.join(' **Ojo:** %s.' % x for x in a[3])))
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
            mb(hf_any['heapBytes']) if hf_any else '—', mb(hf_any['footprintBytes']) if hf_any else '—', mb(store), mb(vectors), mb(rest),
            mb(sum(files.values()))))
    print()
    print('- **Las celdas de posting-edge comparten un solo fichero de vectores,** enlazado: en disco está una vez.')
    print('- **El heap es el de Go tras abrir el índice y recolectar.** La caché del sistema no cuenta.')
    print('- **En HFresh, «vectores exactos» es su bucket `objects`,** y «store» sus otros buckets.')
    print()

    print('### Lo que vale y lo que no')
    print()
    print('- **Los números absolutos de esta máquina no valen para producción.** Vale la comparación entre índices, medidos igual.')
    print('- **Es una sesión de dos rondas.** La medida es la primera; la segunda dice qué curvas derivaron.')
    print('- **Menos de un 10 % entre dos índices es un empate.**')
    print('- **El p99 en frío sale de las 5 queries más lentas de 500.**')


if __name__ == '__main__':
    main(sys.argv[1])
