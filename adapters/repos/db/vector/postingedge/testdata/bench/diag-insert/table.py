# The table of the diagnostic of the inserts: by size and by how many callers
# add at once, where the time of a caller goes, in ms per insert and as a
# share of it. From the TSV summary.py writes of the passes of every size
# (the two passes of phases of every number of callers, averaged).
#
# The time of a caller per insert is callers x wall / inserts: every caller
# is in every round from its start to the end of its flush.
#
# Usage: python3 -I -B table.py <100k.tsv> <10m.tsv>
import csv, sys


def load(path):
    rows = {}
    for r in csv.DictReader(open(path), delimiter="\t"):
        rows[r["pass"]] = {k: (float(v) if v not in ("", "yes", "no") and k not in ("pass", "kind", "segments") else v)
                           for k, v in r.items()}
    return rows


def mean(rows, names, key):
    vals = [rows[n][key] for n in names if n in rows and rows[n].get(key, "") != ""]
    return sum(vals) / len(vals) if vals else 0.0


# The parts of the time of a caller, each from the columns of summary.py.
PARTS = [
    ("plan: centroides", ["assign ms"]),
    ("plan: filas (pedir al store y decodificar)", ["fetch ms"]),
    ("plan: recorrido", ["visit ms"]),
    ("plan: vectores exactos", ["rescore ms"]),
    ("plan: resto de la búsqueda", ["search ms", "-fetch ms", "-visit ms", "-rescore ms"]),
    ("plan: resto (fuera de centroides y búsqueda)", ["c plans", "-assign ms", "-search ms"]),
    ("espera a entrar a buscar mientras se aplica un lote", ["c enterWait", "c readWait"]),
    ("dejar los planes y esperar a que su lote se aplique", ["c leaveWait", "c appliedWait"]),
    ("quien aplica: espera a que acaben las búsquedas", ["c drainWait"]),
    ("quien aplica: espera el cerrojo de escritura", ["c applyLockWait"]),
    ("quien aplica: aplicar bajo el cerrojo", ["c apply"]),
    ("quien aplica: después de aplicar", ["c applyAfter"]),
    ("admisión del lote (espera y comprobación)", ["c admitWait", "c admit"]),
    ("AddBatch, sin nombre", ["c unnamed"]),
    ("fuera de los relojes de AddBatch (lanzar y recoger llamadores)", ["add ms", "-c total"]),
    ("Flush de la ronda", ["flush ms"]),
    ("entre rondas", ["between ms"]),
]
# What applying is made of, by the clocks of the index (inside "aplicar").
APPLY = [("escribir el miembro", "write ms"), ("aristas inversas", "inverse ms"), ("split", "split ms"),
         ("filas reescritas", "row ms"), ("registro del diario", "journal ms"), ("entregar sus escrituras", "hand ms"),
         ("sync", "sync ms")]
COUNTS = ["ins/s", "caller ms", "cores", "user ms", "sys ms", "Minstr", "Mcyc", "IPC", "mutex wait ms", "sched lat ms",
          "gc cpu ms", "go idle cores", "ready", "rd KB", "wr KB", "disk rd ops/ins", "disk rd ms/op", "minor faults",
          "major faults", "vol csw", "invol csw", "thr R", "thr U", "loads", "rounds", "rows", "rows MB", "members",
          "vectors", "inv checks", "inv edges", "splits/1000", "store reads", "seg held/read", "seg found/read",
          "alloc MB", "journal KB", "row writes", "plans/window", "plans/batch"]

sizes = [("100k", load(sys.argv[1])), ("10M", load(sys.argv[2]))]
cells = [(size, c, rows, ["phases-c%d-a" % c, "phases-c%d-b" % c]) for size, rows in sizes for c in (1, 2, 4, 8)]


def part(rows, names, cols):
    total = 0.0
    for col in cols:
        sign = -1.0 if col.startswith("-") else 1.0
        total += sign * mean(rows, names, col.lstrip("-"))
    return total


head = ["parte"] + ["%s, %d" % (size, c) for size, c, _, _ in cells]
print("| " + " | ".join(head) + " |")
print("|---|" + "---:|" * len(cells))
for label, cols in PARTS:
    line = [label]
    for size, c, rows, names in cells:
        t = mean(rows, names, "caller ms")
        v = part(rows, names, cols)
        line.append("%.2f (%.0f %%)" % (v, 100 * v / t if t else 0))
    print("| " + " | ".join(line) + " |")
line = ["**tiempo del llamador por insert, ms**"]
for size, c, rows, names in cells:
    line.append("**%.2f**" % mean(rows, names, "caller ms"))
print("| " + " | ".join(line) + " |")
print()
print("| dentro de aplicar, ms por insert | " + " | ".join(head[1:]) + " |")
print("|---|" + "---:|" * len(cells))
for label, col in APPLY:
    print("| " + label + " | " + " | ".join("%.3f" % mean(rows, names, col) for _, _, rows, names in cells) + " |")
print()
print("| por insert | " + " | ".join(head[1:]) + " |")
print("|---|" + "---:|" * len(cells))
for col in COUNTS:
    vals = [mean(rows, names, col) for _, _, rows, names in cells]
    print("| " + col + " | " + " | ".join(("%.1f" % v if abs(v) >= 100 else "%.2f" % v) for v in vals) + " |")
print()
# The two passes of every cell, one next to the other: how far apart they are.
print("| pasadas iguales | " + " | ".join(head[1:]) + " |")
print("|---|" + "---:|" * len(cells))
print("| inserts/s, a y b | " + " | ".join(
    "%.0f y %.0f" % (rows.get(names[0], {}).get("ins/s", 0) or 0, rows.get(names[1], {}).get("ins/s", 0) or 0)
    for _, _, rows, names in cells) + " |")
