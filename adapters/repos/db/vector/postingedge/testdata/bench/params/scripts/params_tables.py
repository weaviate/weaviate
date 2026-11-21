#!/usr/bin/env python3
"""The two measures of the parameters (bench-report.md, "Parámetros: el `ef`
dinámico y la regla del rescore"), from the files run_params.sh leaves: for
every k, the recall and the loads of the reference cell and of the cell that
routes with codes of 512 bits over the mesh of ef, and the ef each crosses
0.95 and 0.99 at; and, at 768d, the same for every rescore limit, with the
ceiling each leaves.

usage: params_tables.py <dir of the run> > params-tables.md

A crossing is interpolated between the two points of the mesh around it, in
the logarithm of ef, and so are its loads.
"""
import json
import math
import os
import sys

CELLS = ((0, 'referencia, código completo'), (512, 'ruteo con 512 bits'))
TARGETS = (0.95, 0.99)


def load(out, name):
    path = os.path.join(out, name + '.json')
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def cell_of(result, bits):
    for cell in result['cells']:
        if cell['routingBits'] == bits:
            return cell
    return None


def sweeps(cell):
    """rescore limit -> points, for the sweep with the default and for the fallback one."""
    out = {}
    if cell.get('points'):
        out[cell['points'][0]['rescoreLimit']] = cell['points']
    if cell.get('fallback'):
        out[cell['fallback'][0]['rescoreLimit']] = cell['fallback']
    return out


def crossing(points, target):
    """(ef, loads) where the recall first gets to target, None if it never does."""
    prev = None
    for p in points:
        if p['recall'] >= target:
            if prev is None:
                return float(p['ef']), p['loadsMean'], True
            span = p['recall'] - prev['recall']
            share = (target - prev['recall']) / span if span > 0 else 1.0
            ef = math.exp(math.log(prev['ef']) + share * (math.log(p['ef']) - math.log(prev['ef'])))
            return ef, prev['loadsMean'] + share * (p['loadsMean'] - prev['loadsMean']), False
        prev = p
    return None


def cross_text(points, target):
    c = crossing(points, target)
    if c is None:
        return 'no llega', ''
    ef, loads, first = c
    return ('ef %s%.0f' % ('≤ ' if first else '', ef)), '%.1f' % loads


def apart(sweeps_):
    """How far the same sweep falls in several runs: the largest difference in recall and in loads, in percent."""
    recall, loads = 0.0, 0.0
    first = {p['ef']: p for p in sweeps_[0]}
    for other in sweeps_[1:]:
        for p in other:
            q = first.get(p['ef'])
            if q is None:
                continue
            recall = max(recall, abs(p['recall'] - q['recall']))
            if q['loadsMean']:
                loads = max(loads, abs(p['loadsMean'] / q['loadsMean'] - 1))
    return recall, 100 * loads


def mesh_table(title, runs):
    """runs: (label, points of the reference, points of the 512-bit cell)."""
    print(title)
    print()
    efs = sorted({p['ef'] for _, ref, sk in runs for pts in (ref, sk) for p in pts or []})
    print('| ef | %s |' % ' | '.join('%s: recall, referencia | cargas | recall, 512 bits | cargas' % label for label, _, _ in runs))
    print('|---|%s' % ('---|---|---|---|' * len(runs)))
    for ef in efs:
        cells = []
        for _, ref, sk in runs:
            for pts in (ref, sk):
                p = next((p for p in pts or [] if p['ef'] == ef), None)
                cells += ['%.4f' % p['recall'], '%.2f' % p['loadsMean']] if p else ['', '']
        print('| %d | %s |' % (ef, ' | '.join(cells)))
    print()


def crossings_table(rows):
    """rows: (label, points)."""
    print('| | 0.95: ef | cargas | 0.99: ef | cargas | el mayor recall de la malla | en ef | cargas p95 a 0.95 |')
    print('|---|---|---|---|---|---|---|---|')
    for label, pts in rows:
        if not pts:
            continue
        a, la = cross_text(pts, 0.95)
        b, lb = cross_text(pts, 0.99)
        best = max(pts, key=lambda p: p['recall'])
        c = crossing(pts, 0.95)
        p95 = ''
        if c is not None:
            near = min(pts, key=lambda p: abs(math.log(p['ef']) - math.log(c[0])))
            p95 = '%d (en ef %d)' % (near['loadsP95'], near['ef'])
        print('| %s | %s | %s | %s | %s | %.4f | %d | %s |' % (label, a, la, b, lb, best['recall'], best['ef'], p95))
    print()


def dynamic_ef(out):
    runs = (('k = 1', load(out, 'dbpedia-1m-k1')), ('k = 10', load(out, 'dbpedia-1m-k10')),
            ('k = 100', load(out, 'dbpedia-1m-k100-r200')), ('k = 100 (otra corrida)', load(out, 'dbpedia-1m-k100-r400')))
    if not any(r for _, r in runs):
        return
    print('## El `ef` dinámico: dbpedia 1M, S = 236')
    print()
    first = next(r for _, r in runs if r)
    print('%s vectores de %d dimensiones, %s queries. Sin cronometrar, en memoria.' % (
        first['vectors'], first['dims'], first['queries']))
    print()
    base = []
    for label, r in runs[:3]:
        if not r:
            continue
        ref, sk = cell_of(r, 0), cell_of(r, 512)
        base.append((label, ref['points'] if ref else None, sk['points'] if sk else None))
    if base:
        mesh_table('**Recall a k y cargas por query, con el rescore por defecto** (64, o k si es mayor):', base)
    print('**Dónde cruza cada k,** con el rescore por defecto:')
    print()
    rows = []
    for label, r in runs[:3]:
        if not r:
            continue
        for bits, name in CELLS:
            cell = cell_of(r, bits)
            if cell and cell.get('points'):
                rows.append(('%s, %s' % (label, name), cell['points']))
    crossings_table(rows)
    wide = []
    for label, r in runs[2:]:
        if not r:
            continue
        for bits, name in CELLS:
            cell = cell_of(r, bits)
            if not cell:
                continue
            for limit, pts in sorted(sweeps(cell).items()):
                wide.append(('k = 100, %s, rescore %d' % (name, max(limit, 100)), pts))
    if wide:
        print('**k = 100, con más rescore.** El de 64 corre con 100, que es k:')
        print()
        seen, rows = set(), []
        for label, pts in wide:
            if label not in seen:
                seen.add(label)
                rows.append((label, pts))
        crossings_table(rows)
        a, b = runs[2][1], runs[3][1]
        if a and b:
            for bits, name in CELLS:
                recall, loads = apart([cell_of(a, bits)['points'], cell_of(b, bits)['points']])
                print('- **Las dos corridas de k = 100, en su barrido por defecto, celda de %s:** se separan como mucho '
                      '%.4f de recall y un %.2f %% de cargas.' % (name, recall, loads))
            print()


def rescore_rule(out):
    runs = [(r, load(out, 'wikidpr-1m-r%d' % r)) for r in (100, 150, 200)]
    runs = [(r, d) for r, d in runs if d]
    if not runs:
        return
    first = runs[0][1]
    print('## La regla del rescore: wiki-dpr 1M a 768d, S = 236')
    print()
    print('%s vectores de %d dimensiones, %s queries, k = %d. Sin cronometrar, en memoria.' % (
        first['vectors'], first['dims'], first['queries'], first['k']))
    print()
    for bits, name in CELLS:
        by = {}
        defaults = []
        for r, d in runs:
            cell = cell_of(d, bits)
            if not cell:
                continue
            for limit, pts in sweeps(cell).items():
                if limit == 64:
                    defaults.append(pts)
                by.setdefault(limit, pts)
        if not by:
            continue
        print('**Celda de %s:**' % name)
        print()
        efs = sorted({p['ef'] for pts in by.values() for p in pts})
        limits = sorted(by)
        print('| ef | %s |' % ' | '.join('rescore %d: recall | cargas' % limit for limit in limits))
        print('|---|%s' % ('---|---|' * len(limits)))
        for ef in efs:
            cells = []
            for limit in limits:
                p = next((p for p in by[limit] if p['ef'] == ef), None)
                cells += ['%.4f' % p['recall'], '%.2f' % p['loadsMean']] if p else ['', '']
            print('| %d | %s |' % (ef, ' | '.join(cells)))
        print()
        crossings_table([('rescore %d' % limit, by[limit]) for limit in limits])
        if len(defaults) > 1:
            recall, loads = apart(defaults)
            print('- **Los %d barridos con rescore 64** se separan como mucho %.4f de recall y un %.2f %% de cargas.' % (
                len(defaults), recall, loads))
            print()


def main(out):
    dynamic_ef(out)
    rescore_rule(out)


if __name__ == '__main__':
    main(sys.argv[1])
