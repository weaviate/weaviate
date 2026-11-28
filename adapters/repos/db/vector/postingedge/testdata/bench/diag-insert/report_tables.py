# The tables of the report of the diagnostic of the inserts, from the TSVs of
# summary.py. Usage: python3 -I -B report_tables.py <100k.tsv> <10m.tsv>
import csv, sys

def load(path):
    rows = {}
    for r in csv.DictReader(open(path), delimiter="\t"):
        rows[r["pass"]] = {k: (float(v) if v not in ("", "yes", "no") and k not in ("pass", "kind", "segments") else v) for k, v in r.items()}
    return rows

R = {"100k": load(sys.argv[1]), "10M": load(sys.argv[2])}

def sp(x, d=0):
    s = ("%." + str(d) + "f") % x
    a, _, b = s.partition(".")
    neg = a.startswith("-"); a = a.lstrip("-")
    out = ""
    while len(a) > 3:
        out = " " + a[-3:] + out; a = a[:-3]
    return ("−" if neg else "") + a + out + ("." + b if b else "")

def mean(size, names, key):
    vals = [R[size][n][key] for n in names if n in R[size] and R[size][n].get(key, "") != ""]
    return sum(vals) / len(vals) if vals else 0.0

cells = [(size, c, ["phases-c%d-a" % c, "phases-c%d-b" % c]) for size in ("100k", "10M") for c in (1, 2, 4, 8)]
head = " | ".join("%s, %d" % (s, c) for s, c, _ in cells)

def part(size, names, cols):
    return sum((-1 if c.startswith("-") else 1) * mean(size, names, c.lstrip("-")) for c in cols)

PARTS = [
    ("plan: centroides", ["assign ms"]),
    ("plan: filas, pedirlas al store y decodificarlas", ["fetch ms"]),
    ("plan: recorrido", ["visit ms"]),
    ("plan: vectores exactos", ["rescore ms"]),
    ("plan: resto", ["c plans", "-assign ms", "-fetch ms", "-visit ms", "-rescore ms"]),
    ("espera a empezar a buscar mientras un lote se aplica", ["c enterWait", "c readWait"]),
    ("quien aplica: espera a que acaben las búsquedas en curso", ["c drainWait"]),
    ("quien aplica: aplicar bajo el cerrojo", ["c apply", "c applyLockWait", "c applyAfter"]),
    ("espera a que su lote se aplique", ["c leaveWait", "c appliedWait"]),
    ("admisión del lote y `Flush` de la ronda", ["c admitWait", "c admit", "c unnamed", "flush ms", "between ms"]),
    ("fuera de `AddBatch`: la ronda del arnés espera a su llamador más lento", ["add ms", "-c total"]),
]
print("| ms por insert, y parte del tiempo del llamador | " + head + " |")
print("|---|" + "---:|" * len(cells))
for label, cols in PARTS:
    line = []
    for size, c, names in cells:
        t = mean(size, names, "caller ms"); v = part(size, names, cols)
        line.append("%.2f (%.0f %%)" % (v, 100 * v / t))
    print("| " + label + " | " + " | ".join(line) + " |")
print("| **tiempo del llamador por insert** | " + " | ".join("**%.2f**" % mean(s, n, "caller ms") for s, c, n in cells) + " |")
print("| inserts por segundo, las dos pasadas | " + " | ".join("%.0f y %.0f" % (R[s][n[0]]["ins/s"], R[s][n[1]]["ins/s"]) for s, c, n in cells) + " |")
print()
ROWS = [("inserts por segundo", "ins/s", 0), ("cores ocupados", "cores", 2), ("ms de CPU por insert", None, 2),
        ("millones de instrucciones por insert", "Minstr", 1), ("instrucciones por ciclo", "IPC", 2),
        ("Ps ociosos, según Go", "go idle cores", 2), ("hilos listos sin core", "ready", 2),
        ("espera en mutex, ms por insert", "mutex wait ms", 2), ("CPU del GC, ms por insert", "gc cpu ms", 2),
        ("reservado, MB por insert", "alloc MB", 2),
        ("leído de disco, KB por insert", "rd KB", 0), ("lecturas de disco por insert", "disk rd ops/ins", 1),
        ("ms por lectura de disco", "disk rd ms/op", 2), ("hilos esperando al disco", "thr U", 2),
        ("escrito a disco, KB por insert", "wr KB", 1), ("fallos de página por insert", None, 1),
        ("planes por lote aplicado", "plans/batch", 1)]
print("| por insert | " + head + " |")
print("|---|" + "---:|" * len(cells))
for label, key, d in ROWS:
    vals = []
    for s, c, n in cells:
        if label.startswith("ms de CPU"):
            v = mean(s, n, "user ms") + mean(s, n, "sys ms")
        elif label.startswith("fallos"):
            v = mean(s, n, "minor faults") + mean(s, n, "major faults")
        else:
            v = mean(s, n, key)
        vals.append(sp(v, d))
    print("| " + label + " | " + " | ".join(vals) + " |")
print()
# The counters, which do not depend on the callers: the pass of counters.
C = [("cargas de posting", "loads", 1), ("rondas", "rounds", 1), ("filas leídas del store", "rows", 1),
     ("MB de filas", "rows MB", 2), ("segmentos consultados por fila", "seg held/read", 2),
     ("segmentos donde estaba la fila", "seg found/read", 2), ("miembros recorridos", "w scanned", 0),
     ("aristas seguidas", "w edges", 0), ("extremos dentro de un posting ya cargado", "w in loaded", 0),
     ("extremos puntuados", "w endpoints", 0), ("vectores exactos", "vectors", 1),
     ("vecinos mirados para aristas inversas", "inv checks", 2), ("aristas inversas entradas", "inv edges", 2),
     ("splits por mil inserts", "splits/1000", 2), ("filas escritas", "row writes", 3),
     ("de ellas, por el presupuesto", "row writes budget", 3), ("KB de diario", "journal KB", 2)]
print("| por insert, 8 llamadores | 100k | 10M | veces |")
print("|---|---:|---:|---:|")
for label, key, d in C:
    a, b = R["100k"]["counters-c8"].get(key, 0) or 0, R["10M"]["counters-c8"].get(key, 0) or 0
    print("| %s | %s | %s | %s |" % (label, sp(a, d), sp(b, d), ("%.2f" % (b / a)) if a else "—"))
print()
print("| pasada con 8 llamadores | 100k: inserts/s | cargas | 10M: inserts/s | cargas | lecturas de disco por insert |")
print("|---|---:|---:|---:|---:|---:|")
for label, p in [("`m2` sin tocar", "control-m2"), ("diagnóstico, relojes apagados", "control-diag-off"),
                 ("diagnóstico, relojes puestos", "control-diag-on"), ("fases, primera", "phases-c8-a"),
                 ("fases, segunda", "phases-c8-b"), ("con los contadores de trabajo", "counters-c8"),
                 ("sin compactación", "nocompaction-c8"), ("las 1 000 queries", "queries-c8"),
                 ("filas 100 000 a 119 999", "fresh-c8")]:
    a, b = R["100k"].get(p), R["10M"].get(p)
    print("| %s | %s | %s | %s | %s | %s |" % (label, sp(a["ins/s"], 0) if a else "—", sp(a["loads"], 1) if a else "—",
          sp(b["ins/s"], 0) if b else "—", sp(b["loads"], 1) if b else "—", sp(b["disk rd ops/ins"], 0) if b and b.get("disk rd ops/ins", "") != "" else "—"))
print()
print("| la búsqueda del insert, ms | 100k: sola, 1 hilo | insert, 1 llamador | sola, 8 hilos | insert, 8 llamadores | 10M: sola, 1 hilo | insert, 1 llamador | sola, 8 hilos | insert, 8 llamadores |")
print("|---|" + "---:|" * 8)
def col(size, kind, c):
    if kind == "s":
        r = R[size]["search-t%d" % c]; return lambda k: r[k]
    names = ["phases-c%d-a" % c, "phases-c%d-b" % c]; return lambda k: mean(size, names, k)
cols = [col(s, k, c) for s in ("100k", "10M") for c in (1, 8) for k in ("s", "i")]
for label, f in [("por segundo", lambda g: sp(g("ins/s"), 0)),
                 ("plan, ms", lambda g: "%.2f" % (g("assign ms") + g("search ms"))),
                 ("filas", lambda g: "%.2f" % g("fetch ms")), ("recorrido", lambda g: "%.2f" % g("visit ms")),
                 ("vectores exactos", lambda g: "%.2f" % g("rescore ms")),
                 ("cores ocupados", lambda g: "%.2f" % g("cores")),
                 ("ms de CPU", lambda g: "%.2f" % (g("user ms") + g("sys ms"))),
                 ("millones de instrucciones", lambda g: "%.1f" % g("Minstr")),
                 ("leído de disco, KB", lambda g: sp(g("rd KB"), 0))]:
    print("| " + label + " | " + " | ".join(f(g) for g in cols) + " |")
print()
print("| por insert, 8 llamadores | 100k | 100k sin compactación | 10M | 10M sin compactación |")
print("|---|---:|---:|---:|---:|")
IO = [("inserts por segundo", "ins/s", 0), ("escrito a disco por el proceso, KB", "wr KB", 1),
      ("vectores, KB", "KB exact vectors of the inserts", 2), ("WAL del store, KB", "KB lsmkv WAL", 2),
      ("volcados de memtable, KB", "KB lsmkv flush (level 0 segments)", 2),
      ("compactación, KB", "KB lsmkv compaction (level 1 and up)", 1), ("diario, KB, por su contador", "journal KB", 2),
      ("escrituras de disco", "disk wr ops/ins", 2), ("leído de disco, KB", "rd KB", 0),
      ("lecturas de disco", "disk rd ops/ins", 1), ("fallos de página", None, 1),
      ("segmentos consultados por fila", "seg held/read", 2), ("segmentos donde estaba la fila", "seg found/read", 2),
      ("plan: filas, ms", "fetch ms", 2), ("reservado, MB", "alloc MB", 2)]
passes = [("100k", ["phases-c8-a", "phases-c8-b"]), ("100k", ["nocompaction-c8"]), ("10M", ["phases-c8-a", "phases-c8-b"]), ("10M", ["nocompaction-c8"])]
for label, key, d in IO:
    vals = []
    for s, n in passes:
        v = (mean(s, n, "minor faults") + mean(s, n, "major faults")) if key is None else mean(s, n, key)
        vals.append(sp(v, d))
    print("| " + label + " | " + " | ".join(vals) + " |")
for s, p in (("100k", "phases-c8-a"), ("100k", "nocompaction-c8"), ("10M", "phases-c8-a"), ("10M", "nocompaction-c8")):
    print(s, p, R[s][p]["segments"])
