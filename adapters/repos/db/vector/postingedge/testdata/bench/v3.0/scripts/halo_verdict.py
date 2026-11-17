#!/usr/bin/env python3
"""The verdict of v3.0, the operating point of the halo (bench-report.md,
"v3.0: punto de operación del halo"), from the files of run_v30_halo.sh.

usage: halo_verdict.py <dir of the run> > halo-verdict.md

dbpedia 1M built in batch with two posting sizes. Over each build, the
reference ranks the endpoints of the edges with the full code, as the resident
sidecar does, and each cell ranks them with a sketch, which a posting would
carry for every distinct endpoint of its edges. The scan keeps the full code.

The user's gate: a cell that does not reach recall 0.99 by raising ef and the
rescore limit is documented and out. The user's criterion: the halo is viable
if a cell reaches 0.99 with at most 3 times the loads of the reference. Every
cell is compared with the reference of its own build, inside the run.

The byte model, as pre-registered: a member is its id (8 B) and its full
code; an edge is the id of its endpoint (4 B); a halo entry is the id of a
distinct endpoint (4 B) and its sketch. No page alignment, no posting header.
"""
import json
import math
import os
import sys

GATE = 0.99
MAX_LOADS = 3.0
ID_BYTES = 8
EDGE_BYTES = 4
ENTRY_ID_BYTES = 4
SIZES = (236, 1024)


def load(path):
    return json.load(open(path)) if os.path.exists(path) else None


def crossing(pts, target):
    """The two points the curve crosses the recall target between."""
    prev = None
    for p in pts:
        if p['recall'] >= target:
            return prev, p
        prev = p
    return prev, None


def at(pts, target, value):
    """A value at the recall target, interpolated between the two points around it."""
    prev, p = crossing(pts, target)
    if p is None:
        return None
    if prev is None:
        return value(p)
    f = (target - prev['recall']) / (p['recall'] - prev['recall'])
    a, b = value(prev), value(p)
    if a <= 0 or b <= 0:
        return a + f * (b - a)
    return math.exp(math.log(a) + f * (math.log(b) - math.log(a)))


def fmt(x, spec='%.2f'):
    return '—' if x is None else spec % x


def times(x, base):
    return '—' if x is None or not base else '%.2fx' % (x / base)


def mb(x):
    return '—' if x is None else '%.2f MB' % (x / 1e6)


def kb(x):
    return '—' if x is None else '%.1f KB' % (x / 1e3)


def pct_of(x, total):
    return '—' if not total else '%.0f %%' % (100 * x / total)


def name_of(cell):
    return 'referencia: código completo' if cell['routingBits'] == 0 else 'sketch de %d bits' % cell['routingBits']


def between(pts, target=GATE):
    prev, p = crossing(pts, target)
    if p is not None and prev is not None:
        return 'ef %d y %d' % (prev['ef'], p['ef']), '%.4f y %.4f' % (prev['recall'], p['recall'])
    if p is not None:
        return 'por debajo de ef %d' % p['ef'], '%.4f' % p['recall']
    return 'no llega', '—'


def percentile(sorted_values, q):
    return sorted_values[min(len(sorted_values) - 1, int(q * len(sorted_values)))]


class Cell:
    """One cell of a build, with the curve its numbers are read on."""

    def __init__(self, run, raw):
        self.raw = raw
        self.bits = raw['routingBits']
        self.code = raw['codeBytes']
        self.member = ID_BYTES + run['fullCodeBytes']
        self.entry = ENTRY_ID_BYTES + self.code
        self.best = max(p['recall'] for p in raw['points'])
        self.points = raw['points']
        self.rescore = raw['points'][0]['rescoreLimit']
        self.fallback_best = None
        self.passes = self.best >= GATE
        if raw.get('fallback'):
            self.fallback_best = max(p['recall'] for p in raw['fallback'])
            if not self.passes and self.fallback_best >= GATE:
                # The gate allows raising the rescore limit: the cell is read
                # on that curve.
                self.passes = True
                self.points = raw['fallback']
                self.rescore = raw['fallback'][0]['rescoreLimit']

    def loads(self, target):
        return at(self.points, target, lambda p: p['loadsMean'])

    def read(self, target, halo):
        """The bytes of each block a query reads at the recall target."""
        members = at(self.points, target, lambda p: p['membersMean'] * self.member)
        edges = at(self.points, target, lambda p: p['edgesMean'] * EDGE_BYTES)
        entries = at(self.points, target, lambda p: p['entriesMean'] * self.entry) if halo else 0.0
        if members is None:
            return None
        return members, edges, entries, members + edges + entries


def main(d):
    runs = {s: load(os.path.join(d, 'dbpedia-1m-halo-s%d.json' % s)) for s in SIZES}
    runs = {s: r for s, r in runs.items() if r is not None}
    if not runs:
        print('no run')
        return
    cells = {s: [Cell(r, c) for c in r['cells']] for s, r in runs.items()}
    refs = {s: next((c for c in cs if c.bits == 0), None) for s, cs in cells.items()}

    print('### Estado')
    print()
    print('| S | ruteo de endpoints | bytes del código | recall máximo, con rescoreLimit %s | gate de 0.99 | cargas a 0.95 | cargas a 0.99 | '
          'frente a su referencia a 0.99 | cruza 0.99 entre | recall en esos ef | endpoints puntuados a 0.99 |' % '/'.join(
              sorted({str(c.raw['points'][0]['rescoreLimit']) for cs in cells.values() for c in cs})))
    print('|---|---|---|---|---|---|---|---|---|---|---|')
    for s, cs in cells.items():
        ref = refs[s]
        for c in cs:
            gate = 'pasa' if c.best >= GATE else ('pasa con rescoreLimit %d' % c.rescore if c.passes else '**no pasa: fuera**')
            best = '%.4f' % c.best
            if c.fallback_best is not None:
                best += ' (%.4f con %d)' % (c.fallback_best, c.raw['fallbackRescore'])
            b, r = between(c.points)
            l95, l99 = c.loads(0.95), c.loads(0.99)
            print('| %d | %s | %d | %s | %s | %s | %s | %s | %s | %s | %s |' % (
                s, name_of(c.raw), c.code, best, gate, fmt(l95), fmt(l99),
                times(l99, ref.loads(0.99)) if ref and c.passes else '—', b, r,
                fmt(at(c.points, 0.99, lambda p: p['endpointsMean']), '%.0f')))
    print()

    print('### El criterio')
    print()
    print('**El halo es viable si alguna celda alcanza 0.99 con %.0f× o menos las cargas de la referencia.**' % MAX_LOADS)
    print()
    viable = []
    for s, cs in cells.items():
        ref = refs[s]
        r99 = ref.loads(0.99) if ref else None
        if r99 is None:
            print('- **S = %d:** la referencia no llega a 0.99: sin veredicto.' % s)
            continue
        print('- **S = %d:** la referencia carga %.2f postings a 0.99, y %.0f× son %.2f.' % (s, r99, MAX_LOADS, MAX_LOADS * r99))
        for c in cs:
            if c.bits == 0:
                continue
            if not c.passes:
                print('  - **Sketch de %d bits: fuera por el gate.** Su recall máximo es %.4f%s.' % (
                    c.bits, c.best, '' if c.fallback_best is None else ', y %.4f con rescoreLimit %d' % (c.fallback_best, c.raw['fallbackRescore'])))
                continue
            l99 = c.loads(0.99)
            ok = l99 <= MAX_LOADS * r99
            if ok:
                viable.append((s, c.bits, l99 / r99))
            print('  - **Sketch de %d bits:** %.2f cargas a 0.99, %.2fx las de la referencia: %s.' % (
                c.bits, l99, l99 / r99, '**cumple**' if ok else '**no cumple**'))
    print()
    if viable:
        print('**Veredicto: el halo es viable.** Cumplen %s.' % ', '.join(
            'S = %d con %d bits (%.2fx)' % v for v in viable))
    else:
        print('**Veredicto: el halo no es viable** por el criterio: ninguna celda alcanza 0.99 con %.0f× o menos las cargas de su referencia.' % MAX_LOADS)
    print()

    print('### El posting, bloque a bloque')
    print()
    for s, run in runs.items():
        lay = run['layout']
        counts = lay['counts']
        member = ID_BYTES + run['fullCodeBytes']
        edges_per_member = sum(c[1] for c in counts) / sum(c[0] for c in counts)
        print('**S = %d:** %d postings. %.2f aristas por miembro, y %.3f endpoints distintos por arista.' % (
            s, lay['postings'], edges_per_member, lay['distinct']['mean'] / lay['edges']['mean']))
        print()
        print('| bloque | unidades por posting: media | p95 | bytes por unidad | bytes por posting: media | p95 |')
        print('|---|---|---|---|---|---|')
        print('| miembros | %.1f | %d | %d | %s | %s |' % (
            lay['members']['mean'], lay['members']['p95'], member, kb(lay['members']['mean'] * member), kb(lay['members']['p95'] * member)))
        print('| aristas | %.1f | %d | %d | %s | %s |' % (
            lay['edges']['mean'], lay['edges']['p95'], EDGE_BYTES, kb(lay['edges']['mean'] * EDGE_BYTES), kb(lay['edges']['p95'] * EDGE_BYTES)))
        for c in cells[s]:
            if c.bits == 0:
                continue
            print('| halo, sketch de %d bits | %.1f endpoints distintos | %d | %d | %s | %s |' % (
                c.bits, lay['distinct']['mean'], lay['distinct']['p95'], c.entry, kb(lay['distinct']['mean'] * c.entry),
                kb(lay['distinct']['p95'] * c.entry)))
        full_entry = ENTRY_ID_BYTES + run['fullCodeBytes']
        print('| halo con el código completo, informativo | %.1f endpoints distintos | %d | %d | %s | %s |' % (
            lay['distinct']['mean'], lay['distinct']['p95'], full_entry, kb(lay['distinct']['mean'] * full_entry),
            kb(lay['distinct']['p95'] * full_entry)))
        print()
        print('| el posting entero | media | p95 | frente al posting sin halo |')
        print('|---|---|---|---|')
        base = sorted(c[0] * member + c[1] * EDGE_BYTES for c in counts)
        base_mean = sum(base) / len(base)
        print('| miembros y aristas, con sidecar residente | %s | %s | 1.00x |' % (kb(base_mean), kb(percentile(base, 0.95))))
        for c in cells[s]:
            if c.bits == 0:
                continue
            total = sorted(x[0] * member + x[1] * EDGE_BYTES + x[2] * c.entry for x in counts)
            mean = sum(total) / len(total)
            print('| con halo de %d bits | %s | %s | %.2fx |' % (c.bits, kb(mean), kb(percentile(total, 0.95)), mean / base_mean))
        print()

    print('### Resultado principal: bytes leídos por query')
    print()
    print('De los postings que carga cada query. El rescore va aparte: %d lecturas de %.1f KB en los dos modos.' % (
        round(next(iter(refs.values())).points[0]['rescoredMean']) if refs else 0,
        next(iter(runs.values()))['dims'] * 4 / 1e3))
    print()
    base236 = refs.get(236)
    for target in (0.95, 0.99):
        print('**A recall %.2f:**' % target)
        print()
        print('| S | celda | modo | cargas | miembros | aristas | halo | total | frente al sidecar residente de su S | frente al de S = 236 |')
        print('|---|---|---|---|---|---|---|---|---|---|')
        for s, cs in cells.items():
            ref = refs[s]
            rread = ref.read(target, False) if ref else None
            b236 = base236.read(target, False) if base236 else None

            def row(cell, mode, read, loads):
                if read is None:
                    print('| %d | %s | %s | — | — | — | — | — | — | — |' % (s, name_of(cell.raw), mode))
                    return
                print('| %d | %s | %s | %s | %s | %s | %s | %s | %s | %s |' % (
                    s, name_of(cell.raw), mode, fmt(loads), mb(read[0]), mb(read[1]), mb(read[2]) if read[2] else '—', mb(read[3]),
                    times(read[3], rread[3]) if rread else '—', times(read[3], b236[3]) if b236 else '—'))

            if ref:
                row(ref, 'sidecar residente', rread, ref.loads(target))
            for c in cs:
                if c.bits == 0:
                    continue
                if not c.passes and target >= GATE:
                    print('| %d | %s | todo en disco | fuera por el gate | — | — | — | — | — | — |' % (s, name_of(c.raw)))
                    continue
                row(c, 'todo en disco' + ('' if c.rescore == ref.rescore else ', rescoreLimit %d' % c.rescore), c.read(target, True),
                    c.loads(target))
        print()

    print('### Recall y cargas a igual ef, frente a la referencia')
    print()
    for s, cs in cells.items():
        ref = refs[s]
        if ref is None:
            continue
        others = [c for c in cs if c.bits != 0]
        by_ef = [{p['ef']: p for p in c.raw['points']} for c in others]
        print('**S = %d,** con rescoreLimit %d:' % (s, ref.raw['points'][0]['rescoreLimit']))
        print()
        print('| ef | referencia: recall | cargas | ' + ' | '.join(
            '%d bits: recall | cargas' % c.bits for c in others) + ' |')
        print('|---|---|---' + '|---|---' * len(others) + '|')
        for p in ref.raw['points']:
            row = []
            for pts in by_ef:
                q = pts.get(p['ef'])
                if q is None:
                    row += ['—', '—']
                    continue
                row += ['%.4f (%+.4f)' % (q['recall'], q['recall'] - p['recall']),
                        '%.2f (%s)' % (q['loadsMean'], times(q['loadsMean'], p['loadsMean']))]
            print('| %d | %.4f | %.2f | %s |' % (p['ef'], p['recall'], p['loadsMean'], ' | '.join(row)))
        print()

    print('### Lo que ocupa cada modo')
    print()
    print('| S | modo | disco: miembros | aristas | halo | total | RAM: sidecar | mapa vector→posting |')
    print('|---|---|---|---|---|---|---|---|')
    for s, run in runs.items():
        counts = run['layout']['counts']
        n = run['vectors']
        member = ID_BYTES + run['fullCodeBytes']
        members = sum(c[0] for c in counts) * member
        edges = sum(c[1] for c in counts) * EDGE_BYTES
        distinct = sum(c[2] for c in counts)
        print('| %d | sidecar residente | %s | %s | — | %s | %s | %s |' % (
            s, mb(members), mb(edges), mb(members + edges), mb(n * run['fullCodeBytes']), mb(n * 4)))
        for c in cells[s]:
            if c.bits == 0:
                continue
            halo = distinct * c.entry
            print('| %d | todo en disco, sketch de %d bits | %s | %s | %s | %s | — | %s |' % (
                s, c.bits, mb(members), mb(edges), mb(halo), mb(members + edges + halo), mb(n * 4)))
    print()
    print('Los centroides quedan residentes en los dos modos, y no se cuentan.')
    print()

    print('### La regla de ruido')
    print()
    print('- **Cada celda se compara con la referencia de su build,** dentro de la corrida, sobre el mismo índice y con las mismas queries.')
    print('- **La columna frente a S = 236 compara dos particiones distintas.** Lleva lo que se separan dos builds, sin medir aquí.')
    print('- **Nada a 0.99 se compara con otra corrida.**')


if __name__ == '__main__':
    main(sys.argv[1])
