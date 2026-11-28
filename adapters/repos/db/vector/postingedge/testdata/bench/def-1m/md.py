# The Markdown tables of the report from the TSV lines stretches.py prints
# (the groups and the whole run). Usage: python3 -I -B md.py <groups.tsv>
import csv, sys
rows = list(csv.reader(open(sys.argv[1]), delimiter="\t"))
head, body = rows[0], [r for r in rows[1:] if len(r) == len(rows[0])]
ix = {c: i for i, c in enumerate(head)}
def sp(v):
    if v == "": return "—"
    if "." in v:
        a, b = v.split("."); return sp(a) + "." + b
    s = ""
    while len(v) > 3: s = " " + v[-3:] + s; v = v[:-3]
    return v + s
cols = [("at", "hora"), ("k in", "miles dentro"), ("ins/s", "inserts/s"), ("user", "cores usuario"), ("sys", "cores sistema"),
        ("ms CPU/ins", "ms de CPU por insert"), ("wr KB/ins", "escrito, KB por insert"), ("WA", "veces el vector"),
        ("journal KB/ins", "diario, KB"), ("rows KB/ins", "filas de aristas y distancias, KB"),
        ("postings", "postings vivos"), ("disk GB", "GB en disco")]
print("| " + " | ".join(n for _, n in cols) + " |")
print("|---|" + "---:|" * (len(cols) - 1))
for r in body:
    label = r[0]
    cells = [("**todo**" if label == "all" else "todo menos el primer tramo" if label.startswith("all but") else label)]
    cells += [sp(r[ix[c]]) for c, _ in cols[1:]]
    print("| " + " | ".join(cells) + " |")
