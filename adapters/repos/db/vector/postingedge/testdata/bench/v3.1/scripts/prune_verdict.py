#!/usr/bin/env python3
"""The verdict of v3.1, pruning the halo (bench-report.md, "v3.1: podar el
halo"), from the file of run_v31_prune.sh.

usage: prune_verdict.py <dir of the run> [<dir of the run of v3.0>] > prune-verdict.md

dbpedia 1M built in batch with S = 236, the build of v3.0. For each sketch
width, a grid of cells: four sets of edges (all, one per target posting,
capped, and both) by three source margins. The cell with all the edges and
margin 0.06 is the base, the cell of v3.0 measured again in this run, and
every other cell is compared with the base of its width.

The user's gate: a cell that does not reach recall 0.99 by raising ef and the
rescore limit is documented and out. The user's criterion: the pruning works
if a cell reads 2.46 MB or less per query at recall 0.95 and keeps 0.99
reachable; how much it drops at 0.99 is reported too.

The byte model, as pre-registered and as in v3.0: a member is its id (8 B)
and its full code; an edge is the id of its endpoint (4 B); a halo entry is
the id of a distinct endpoint (4 B) and its sketch. No page alignment, no
posting header, no rescore.
"""
import json
import math
import os
import sys

GATE = 0.99
BAR = 2.46e6  # bytes per query at recall 0.95: the HFresh partition without replicas
BASE_MARGIN = 0.06
ID_BYTES = 8
EDGE_BYTES = 4
ENTRY_ID_BYTES = 4
TARGETS = (0.95, 0.99)


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


def mb(x):
    return '—' if x is None else '%.2f MB' % (x / 1e6)


def kb(x):
    return '—' if x is None else '%.1f KB' % (x / 1e3)


def signed(x, spec='%.1f'):
    """A signed number with the minus sign of the report."""
    return ('+' if x >= 0 else '−') + spec % abs(x)


def change(x, base):
    """How much x is above or under base, in percent."""
    if x is None or not base:
        return '—'
    return signed(100 * (x / base - 1)) + ' %'


def more(x, base):
    """How much x is above or under base, in words."""
    if x is None or not base:
        return '—'
    pct = 100 * (x / base - 1)
    return 'un %.1f %% %s' % (abs(pct), 'más' if pct >= 0 else 'menos')


def times(x, base):
    return '—' if x is None or not base else '%.2fx' % (x / base)


def between(pts, target):
    prev, p = crossing(pts, target)
    if p is not None and prev is not None:
        return 'ef %d y %d (%.4f y %.4f)' % (prev['ef'], p['ef'], prev['recall'], p['recall'])
    if p is not None:
        return 'por debajo de ef %d (%.4f)' % (p['ef'], p['recall'])
    return 'no llega'


def edges_name(name, sets):
    s = sets[name]
    if not s['onePerTarget'] and not s['degreeCap']:
        return 'todas'
    if s['onePerTarget'] and s['degreeCap']:
        return 'una por posting de destino y cap %d' % s['degreeCap']
    if s['onePerTarget']:
        return 'una por posting de destino'
    return 'cap %d' % s['degreeCap']


class Cell:
    """One cell of the grid, with the curve its numbers are read on."""

    def __init__(self, run, raw, sets):
        self.raw = raw
        self.bits = raw['routingBits']
        self.edges = raw.get('edges', 'all')
        self.margin = raw.get('margin', BASE_MARGIN)
        self.set = sets[self.edges]
        self.member = ID_BYTES + run['fullCodeBytes']
        self.entry = ENTRY_ID_BYTES + raw['codeBytes']
        self.best = max(p['recall'] for p in raw['points'])
        self.points = raw['points']
        self.rescore = raw['points'][0]['rescoreLimit']
        self.fallback_best = None
        self.passes = self.best >= GATE
        if raw.get('fallback'):
            self.fallback_best = max(p['recall'] for p in raw['fallback'])
            if not self.passes and self.fallback_best >= GATE:
                # The gate allows raising the rescore limit: the cell is read
                # on that curve, at 0.95 too.
                self.passes = True
                self.points = raw['fallback']
                self.rescore = raw['fallback'][0]['rescoreLimit']
        self.pruned_edges = self.set['onePerTarget'] or self.set['degreeCap'] > 0
        self.other_margin = abs(self.margin - BASE_MARGIN) > 1e-9
        # The levers of the cell: each pruning of the edges is one, and a
        # margin other than that of the base is another.
        self.levers = int(self.set['onePerTarget']) + int(self.set['degreeCap'] > 0) + int(self.other_margin)
        self.is_base = self.levers == 0

    def name(self, sets):
        return '%s, M = %.2f' % (edges_name(self.edges, sets), self.margin)

    def value(self, target, key):
        return at(self.points, target, lambda p: p[key])

    def read(self, target, halo=True):
        """The bytes of each block a query reads at the recall target."""
        members = at(self.points, target, lambda p: p['membersMean'] * self.member)
        edges = at(self.points, target, lambda p: p['edgesMean'] * EDGE_BYTES)
        entries = at(self.points, target, lambda p: p['entriesMean'] * self.entry) if halo else 0.0
        if members is None:
            return None
        return members, edges, entries, members + edges + entries

    def total(self, target, halo=True):
        r = self.read(target, halo)
        return None if r is None else r[3]

    def disk(self, halo=True):
        counts = self.set['layout']['counts']
        return (sum(c[0] for c in counts) * self.member + sum(c[1] for c in counts) * EDGE_BYTES +
                (sum(c[2] for c in counts) * self.entry if halo else 0))

    def gate(self):
        if self.best >= GATE:
            return 'pasa'
        if self.passes:
            return 'pasa con rescoreLimit %d' % self.rescore
        best = '%.4f' % self.best
        if self.fallback_best is not None:
            best += ', y %.4f con rescoreLimit %d' % (self.fallback_best, self.raw['fallbackRescore'])
        return '**no pasa: fuera** (%s)' % best


def lowest(cells, target):
    """The cell that reads fewest bytes at the target, of those that pass the gate."""
    ok = [c for c in cells if c.passes and c.total(target) is not None]
    return min(ok, key=lambda c: c.total(target)) if ok else None


def main(d, v30_dir):
    run = load(os.path.join(d, 'dbpedia-1m-haloprune-s236.json'))
    if run is None:
        print('no run')
        return
    sets = {s['name']: s for s in run['edgeSets']}
    cells = [Cell(run, c, sets) for c in run['cells']]
    widths = sorted({c.bits for c in cells})
    by_width = {w: [c for c in cells if c.bits == w] for w in widths}
    bases = {w: next((c for c in cs if c.is_base), None) for w, cs in by_width.items()}
    ref = Cell(run, run['reference'], sets) if run.get('reference') else None
    n = run['vectors']

    print('### Estado')
    print()
    print('- **La corrida:** %d vectores, %d queries, k = %d. %d celdas de %d: %s.' % (
        n, run['queries'], run['k'], len(cells), len(widths) * len(sets) * len({c.margin for c in cells}),
        ', '.join('%d con sketch de %d bits' % (len(cs), w) for w, cs in by_width.items())))
    print('- **La malla de ef:** %s.' % ', '.join(str(e) for e in run['efs']))
    v30 = load(os.path.join(v30_dir, 'dbpedia-1m-halo-s236.json')) if v30_dir else None
    if v30 is None:
        print('- **El build no se comprueba contra v3.0:** no hay corrida de v3.0 que leer.')
    elif v30['vectors'] != n or v30['queries'] != run['queries']:
        print('- **El build no es el de v3.0:** aquella corrida tiene %d vectores y %d queries.' % (v30['vectors'], v30['queries']))
    else:
        same = v30['layout']['counts'] == sets['all']['layout']['counts']
        print('- **El build %s el de v3.0:** los miembros, las aristas y los endpoints distintos de sus %d postings %s los de `v3.0/dbpedia-1m-halo-s236.json`.' % (
            'es' if same else '**no es**', sets['all']['layout']['postings'], 'son' if same else '**no son**'))
        for w in widths:
            old = next((c for c in v30['cells'] if c['routingBits'] == w), None)
            base = bases[w]
            if old is None or base is None:
                continue
            mine = {p['ef']: p for p in base.raw['points']}
            common = [p for p in old['points'] if p['ef'] in mine]
            equal = sum(1 for p in common if all(
                abs(p[k] - mine[p['ef']][k]) < 1e-9 for k in ('recall', 'loadsMean', 'roundsMean', 'entriesMean')))
            worst = max((abs(p['loadsMean'] - mine[p['ef']]['loadsMean']) for p in common), default=0.0)
            print('  - Base de %d bits, a igual ef: %d de los %d puntos de v3.0 salen idénticos en recall, cargas, rondas y entradas de halo%s.' % (
                w, equal, len(common), '' if equal == len(common) else '; la mayor diferencia en cargas es de %.2f' % worst))
        if ref is not None:
            old = next((c for c in v30['cells'] if c['routingBits'] == 0), None)
            mine = {p['ef']: p for p in ref.raw['points']}
            common = [p for p in old['points'] if p['ef'] in mine] if old else []
            equal = sum(1 for p in common if all(
                abs(p[k] - mine[p['ef']][k]) < 1e-9 for k in ('recall', 'loadsMean', 'roundsMean')))
            print('  - Referencia con código completo, a igual ef: %d de %d puntos idénticos.' % (equal, len(common)))
    print()

    print('### Las aristas de cada poda')
    print()
    full = sets['all']
    print('| aristas | por vector | máximo por vector | postings de destino por vector | aristas repetidas hacia un posting | '
          'frente a todas | por posting: media | p95 | endpoints distintos por posting: media | p95 | por arista | frente a todas |')
    print('|---|---|---|---|---|---|---|---|---|---|---|---|')
    for name, s in sets.items():
        lay = s['layout']
        print('| %s | %.2f | %d | %.2f | %d | %s | %.1f | %d | %.1f | %d | %.3f | %s |' % (
            edges_name(name, sets), s['edges'] / n, s['maxDegree'], s['targets'] / n, s['shared'],
            change(s['edges'], full['edges']) if name != 'all' else '—', lay['edges']['mean'], lay['edges']['p95'],
            lay['distinct']['mean'], lay['distinct']['p95'], lay['distinct']['mean'] / lay['edges']['mean'],
            change(lay['distinct']['mean'], full['layout']['distinct']['mean']) if name != 'all' else '—'))
    print()
    print('**La comprobación de cada poda,** hecha en el paso, antes de medir: cada fila se rehace desde la fila completa, arista a arista, y tiene que coincidir.')
    print()
    for name, s in sets.items():
        if name == 'all':
            continue
        notes = []
        if s['onePerTarget']:
            notes.append('%d aristas repetidas hacia un mismo posting' % s['shared'])
        if s['onePerTarget'] and not s['degreeCap']:
            notes.append('%d postings de destino, los %d de todas las aristas: %s' % (
                s['targets'], full['targets'], 'ningún vector pierde un destino' if s['targets'] == full['targets'] else '**se pierden destinos**'))
        if s['degreeCap']:
            notes.append('como mucho %d aristas por vector, con el cap en %d' % (s['maxDegree'], s['degreeCap']))
        if s['onePerTarget'] and s['degreeCap']:
            notes.append('%d postings de destino de %d: con el cap se pierde el %.1f %%' % (
                s['targets'], full['targets'], 100 * (1 - s['targets'] / full['targets'])))
        print('- **%s:** %s.' % (edges_name(name, sets).capitalize(), '; '.join(notes)))
    print()

    print('**El posting, bloque a bloque,** de media:')
    print()
    print('| aristas | miembros | aristas | ' + ' | '.join(
        'halo de %d bits | el posting entero | frente a todas' % w for w in widths) + ' |')
    print('|---|---|---' + '|---|---|---' * len(widths) + '|')
    member = ID_BYTES + run['fullCodeBytes']
    for name, s in sets.items():
        lay = s['layout']
        row = []
        for w in widths:
            entry = bases[w].entry
            whole = lay['members']['mean'] * member + lay['edges']['mean'] * EDGE_BYTES + lay['distinct']['mean'] * entry
            whole_full = (full['layout']['members']['mean'] * member + full['layout']['edges']['mean'] * EDGE_BYTES +
                          full['layout']['distinct']['mean'] * entry)
            row += [kb(lay['distinct']['mean'] * entry), kb(whole), change(whole, whole_full) if name != 'all' else '—']
        print('| %s | %s | %s | %s |' % (
            edges_name(name, sets), kb(lay['members']['mean'] * member), kb(lay['edges']['mean'] * EDGE_BYTES), ' | '.join(row)))
    print()

    print('### Las celdas')
    print()
    print('Bytes de posting por query, de los postings que carga cada una, con sus tres bloques y sin el rescore. '
          'Entre paréntesis, la parte que es halo. El listón son %.2f MB a 0.95.' % (BAR / 1e6))
    print()
    for w, cs in by_width.items():
        base = bases[w]
        print('**Sketch de %d bits:**' % w)
        print()
        print('| aristas | M | celda | gate de 0.99 | a 0.95: cargas | rondas | bytes (halo) | frente a la base | ¿%.2f MB o menos? | '
              'a 0.99: cargas | rondas | bytes (halo) | frente a la base | disco total |' % (BAR / 1e6))
        print('|---|---|---|---|---|---|---|---|---|---|---|---|---|---|')
        for c in cs:
            kind = '**base**' if c.is_base else ('combinación' if c.levers >= 2 else (
                'celda 3' if c.other_margin else ('celda 1' if c.set['onePerTarget'] else 'celda 2')))
            r95, r99 = c.read(0.95), c.read(0.99)
            ok = '—' if c.is_base or r95 is None else ('**sí**' if r95[3] <= BAR and c.passes else 'no')
            print('| %s | %.2f | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |' % (
                edges_name(c.edges, sets), c.margin, kind, c.gate(),
                fmt(c.value(0.95, 'loadsMean')), fmt(c.value(0.95, 'roundsMean')),
                '—' if r95 is None else '%s (%s)' % (mb(r95[3]), mb(r95[2])),
                '—' if c.is_base else change(c.total(0.95), base.total(0.95)), ok,
                fmt(c.value(0.99, 'loadsMean')) if c.passes else '—', fmt(c.value(0.99, 'roundsMean')) if c.passes else '—',
                '—' if r99 is None or not c.passes else '%s (%s)' % (mb(r99[3]), mb(r99[2])),
                '—' if c.is_base or not c.passes else change(c.total(0.99), base.total(0.99)), mb(c.disk())))
        print()
    print('Dónde cruza cada celda, en su curva de ef:')
    print()
    print('| sketch | aristas | M | cruza 0.95 entre | cruza 0.99 entre |')
    print('|---|---|---|---|---|')
    for w, cs in by_width.items():
        for c in cs:
            print('| %d bits | %s | %.2f | %s | %s |' % (
                w, edges_name(c.edges, sets), c.margin, between(c.points, 0.95), between(c.points, 0.99)))
    print()

    print('### El margen (celda 3)')
    print()
    print('Con todas las aristas. Se elige, por ancho, el M que menos bytes lee a 0.95.')
    print()
    print('| sketch | M | a 0.95: cargas | bytes | frente a M = %.2f | a 0.99: cargas | bytes | frente a M = %.2f |' % (BASE_MARGIN, BASE_MARGIN))
    print('|---|---|---|---|---|---|---|---|')
    chosen = {}
    for w, cs in by_width.items():
        margins = [c for c in cs if not c.pruned_edges]
        base = bases[w]
        for c in margins:
            print('| %d bits | %.2f%s | %s | %s | %s | %s | %s | %s |' % (
                w, c.margin, ' (base)' if c.is_base else '', fmt(c.value(0.95, 'loadsMean')), mb(c.total(0.95)),
                '—' if c.is_base else change(c.total(0.95), base.total(0.95)),
                fmt(c.value(0.99, 'loadsMean')) if c.passes else '—', mb(c.total(0.99)) if c.passes else '—',
                '—' if c.is_base or not c.passes else change(c.total(0.99), base.total(0.99))))
        chosen[w] = (lowest(margins, 0.95), lowest(margins, 0.99))
    print()
    for w, (m95, m99) in chosen.items():
        if m95 is None:
            print('- **Sketch de %d bits:** ningún margen pasa el gate.' % w)
            continue
        base = bases[w]
        line = '- **Sketch de %d bits: M = %.2f,** con %s a 0.95' % (w, m95.margin, mb(m95.total(0.95)))
        if m95.is_base:
            line += '. Es el de la base: ningún otro margen lee menos.'
        else:
            line += ', %s que con M = %.2f.' % (more(m95.total(0.95), base.total(0.95)), BASE_MARGIN)
        if m99 is not None and m99.margin != m95.margin:
            line += ' A 0.99 lee menos M = %.2f: %s, frente a %s con M = %.2f.' % (
                m99.margin, mb(m99.total(0.99)), mb(m95.total(0.99)), m95.margin)
        elif m99 is not None:
            line += ' A 0.99 es también el que menos lee: %s.' % mb(m99.total(0.99))
        print(line)
    print()

    print('### La mejor combinación (celda 4)')
    print()
    print('De las celdas con dos palancas o más, la que menos bytes lee a 0.95 entre las que pasan el gate.')
    print()
    best_combo = {}
    best_single = {}
    for w, cs in by_width.items():
        base = bases[w]
        combos = [c for c in cs if c.levers >= 2]
        singles = [c for c in cs if c.levers == 1]
        combo, single = lowest(combos, 0.95), lowest(singles, 0.95)
        best_combo[w], best_single[w] = combo, single
        if combo is None:
            print('- **Sketch de %d bits:** ninguna combinación pasa el gate.' % w)
            continue
        print('- **Sketch de %d bits: %s.** Lee %s a 0.95 (%s que la base) y %s a 0.99 (%s).' % (
            w, combo.name(sets), mb(combo.total(0.95)), more(combo.total(0.95), base.total(0.95)),
            mb(combo.total(0.99)), more(combo.total(0.99), base.total(0.99))))
        if single is not None:
            better = combo.total(0.95) < single.total(0.95)
            print('  - La mejor palanca sola es %s, con %s a 0.95: la combinación %s.' % (
                single.name(sets), mb(single.total(0.95)),
                'lee %s' % more(combo.total(0.95), single.total(0.95)) if better else '**no lee menos que ella**'))
        at99 = lowest(combos, 0.99)
        if at99 is not None and at99 is not combo:
            print('  - A 0.99 la combinación que menos lee es otra: %s, con %s.' % (at99.name(sets), mb(at99.total(0.99))))
    print()

    print('### El criterio')
    print()
    print('**La poda sirve si alguna celda lee %.2f MB o menos a 0.95 y mantiene 0.99 alcanzable.**' % (BAR / 1e6))
    print()
    meets = []
    for w, cs in by_width.items():
        base = bases[w]
        pruned = [c for c in cs if not c.is_base]
        low = lowest(pruned, 0.95)
        out = [c for c in pruned if not c.passes]
        ok = [c for c in pruned if c.passes and c.total(0.95) is not None and c.total(0.95) <= BAR]
        meets += ok
        print('- **Sketch de %d bits.** La base lee %s a 0.95 y %s a 0.99. %s' % (
            w, mb(base.total(0.95)), mb(base.total(0.99)),
            'Ya está por debajo del listón.' if base.total(0.95) <= BAR else
            'Para llegar al listón hay que leer %s.' % more(BAR, base.total(0.95))))
        if out:
            print('  - Fuera por el gate: %s.' % '; '.join(c.name(sets) for c in out))
        else:
            print('  - Las %d celdas podadas pasan el gate.' % len(pruned))
        if low is not None:
            print('  - La que menos lee a 0.95 es %s: %s, %s que la base, y %s del listón.' % (
                low.name(sets), mb(low.total(0.95)), more(low.total(0.95), base.total(0.95)),
                'por debajo' if low.total(0.95) <= BAR else 'a %s' % mb(low.total(0.95) - BAR)))
        print('  - **Cumplen %d de %d%s' % (len(ok), len(pruned), '.**' if not ok else ':** ' + '; '.join(
            '%s (%s)' % (c.name(sets), mb(c.total(0.95))) for c in sorted(ok, key=lambda c: c.total(0.95))) + '.'))
    print()
    if meets:
        print('**Veredicto: la poda sirve.** %d celdas leen %.2f MB o menos a 0.95 y pasan el gate de 0.99.' % (len(meets), BAR / 1e6))
    else:
        print('**Veredicto: la poda no sirve,** por el criterio: ninguna celda lee %.2f MB o menos a 0.95 pasando el gate de 0.99.' % (BAR / 1e6))
    print()
    print('**Cuánto baja a 0.99:**')
    print()
    print('| sketch | celda | bytes a 0.99 | frente a la base | en MB |')
    print('|---|---|---|---|---|')
    for w, cs in by_width.items():
        base = bases[w]
        print('| %d bits | base | %s | — | — |' % (w, mb(base.total(0.99))))
        shown = []
        for c in (lowest([x for x in cs if not x.is_base], 0.99), lowest([x for x in cs if not x.is_base], 0.95),
                  best_single[w], best_combo[w]):
            if c is not None and c not in shown:
                shown.append(c)
        low99 = shown[0] if shown else None
        for c in shown:
            tags = []
            if c is low99:
                tags.append('la que menos lee a 0.99')
            if c is lowest([x for x in cs if not x.is_base], 0.95):
                tags.append('la que menos lee a 0.95')
            print('| %d bits | %s: %s | %s | %s | %s MB |' % (
                w, c.name(sets), ', '.join(tags) if tags else ('la mejor palanca sola a 0.95' if c is best_single[w] else 'la mejor combinación a 0.95'),
                mb(c.total(0.99)), change(c.total(0.99), base.total(0.99)), signed((c.total(0.99) - base.total(0.99)) / 1e6, '%.2f')))
    print()

    print('### Lo que quita cada palanca sola a 0.95, y lo que añade la combinación')
    print()
    print('| sketch | celda | bytes a 0.95 | frente a la base | cargas | frente a la base | bytes por carga | frente a la base |')
    print('|---|---|---|---|---|---|---|---|')
    for w, cs in by_width.items():
        base = bases[w]
        per_load = lambda c: c.total(0.95) / c.value(0.95, 'loadsMean')
        print('| %d bits | base | %s | — | %s | — | %s | — |' % (
            w, mb(base.total(0.95)), fmt(base.value(0.95, 'loadsMean')), kb(per_load(base))))
        rows = [c for c in cs if c.levers == 1 and c.passes]
        if best_combo[w] is not None:
            rows.append(best_combo[w])
        for c in rows:
            print('| %d bits | %s%s | %s | %s | %s | %s | %s | %s |' % (
                w, c.name(sets), '' if c.levers == 1 else ' (la mejor combinación)', mb(c.total(0.95)),
                change(c.total(0.95), base.total(0.95)), fmt(c.value(0.95, 'loadsMean')),
                change(c.value(0.95, 'loadsMean'), base.value(0.95, 'loadsMean')), kb(per_load(c)),
                change(per_load(c), per_load(base))))
    print()

    print('### Recall, cargas y rondas a igual ef, frente a la base')
    print()
    for w, cs in by_width.items():
        base = bases[w]
        others = [c for c in cs if c.levers == 1]
        if best_combo[w] is not None:
            others.append(best_combo[w])
        by_ef = [{p['ef']: p for p in c.raw['points']} for c in others]
        print('**Sketch de %d bits,** con rescoreLimit %d:' % (w, base.raw['points'][0]['rescoreLimit']))
        print()
        print('| ef | base: recall | cargas | rondas | ' + ' | '.join(
            '%s: recall | cargas' % c.name(sets) for c in others) + ' |')
        print('|---|---|---|---' + '|---|---' * len(others) + '|')
        for p in base.raw['points']:
            row = []
            for pts in by_ef:
                q = pts.get(p['ef'])
                if q is None:
                    row += ['—', '—']
                    continue
                row += ['%.4f (%s)' % (q['recall'], signed(q['recall'] - p['recall'], '%.4f')),
                        '%.2f (%s)' % (q['loadsMean'], times(q['loadsMean'], p['loadsMean']))]
            print('| %d | %.4f | %.2f | %.2f | %s |' % (p['ef'], p['recall'], p['loadsMean'], p['roundsMean'], ' | '.join(row)))
        print()

    print('### Endpoints puntuados, de las entradas de halo leídas')
    print()
    print('| sketch | aristas | M | a 0.95: entradas leídas | endpoints puntuados | parte | a 0.99: entradas leídas | endpoints puntuados | parte |')
    print('|---|---|---|---|---|---|---|---|---|')
    for w, cs in by_width.items():
        for c in cs:
            row = []
            for target in TARGETS:
                entries, scored = c.value(target, 'entriesMean'), c.value(target, 'endpointsMean')
                if entries is None or (target >= GATE and not c.passes):
                    row += ['—', '—', '—']
                else:
                    row += ['%.0f' % entries, '%.0f' % scored, '%.0f %%' % (100 * scored / entries)]
            print('| %d bits | %s | %.2f | %s |' % (w, edges_name(c.edges, sets), c.margin, ' | '.join(row)))
    print()

    if ref is not None:
        print('### La referencia con sidecar residente, en la misma corrida')
        print()
        print('Ruteo con el código completo, todas las aristas y M = %.2f. Lee los miembros y las aristas de los postings que carga; el sidecar, %s, está en RAM. No entra en el criterio.' % (
            BASE_MARGIN, mb(n * run['fullCodeBytes'])))
        print()
        print('| | a 0.95: cargas | rondas | bytes | a 0.99: cargas | rondas | bytes | disco total |')
        print('|---|---|---|---|---|---|---|---|')
        print('| sidecar residente | %s | %s | %s | %s | %s | %s | %s |' % (
            fmt(ref.value(0.95, 'loadsMean')), fmt(ref.value(0.95, 'roundsMean')), mb(ref.total(0.95, False)),
            fmt(ref.value(0.99, 'loadsMean')), fmt(ref.value(0.99, 'roundsMean')), mb(ref.total(0.99, False)), mb(ref.disk(False))))
        for w, cs in by_width.items():
            shown = [bases[w]]
            for c in (lowest([x for x in cs if not x.is_base], 0.95), lowest([x for x in cs if not x.is_base], 0.99)):
                if c is not None and c not in shown:
                    shown.append(c)
            for c in shown:
                print('| %d bits, %s | %s (%s) | %s | %s (%s) | %s (%s) | %s | %s (%s) | %s (%s) |' % (
                    w, 'base' if c.is_base else c.name(sets),
                    fmt(c.value(0.95, 'loadsMean')), times(c.value(0.95, 'loadsMean'), ref.value(0.95, 'loadsMean')),
                    fmt(c.value(0.95, 'roundsMean')), mb(c.total(0.95)), times(c.total(0.95), ref.total(0.95, False)),
                    fmt(c.value(0.99, 'loadsMean')), times(c.value(0.99, 'loadsMean'), ref.value(0.99, 'loadsMean')),
                    fmt(c.value(0.99, 'roundsMean')), mb(c.total(0.99)), times(c.total(0.99), ref.total(0.99, False)),
                    mb(c.disk()), times(c.disk(), ref.disk(False))))
        print()

    print('### La regla de ruido')
    print()
    print('- **Cada celda se compara con la base de su ancho,** dentro de la corrida, sobre el mismo build y con las mismas queries.')
    print('- **Nada a 0.99 se compara con otra corrida.** Las cifras de v3.0 no entran en el veredicto.')
    print('- **El listón de %.2f MB es de otra fuente,** una simulación con otra contabilidad (`disk-design.md`, «Contexto: frente a HFresh»): se usa tal cual.' % (BAR / 1e6))


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)
