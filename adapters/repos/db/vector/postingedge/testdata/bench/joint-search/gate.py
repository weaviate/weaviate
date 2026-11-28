# The estimate of an event by joint search and its gate, by the formula
# pre-registered in bench-report.md ("El evento por búsqueda conjunta, y su
# puerta", and the user's amendment of 2026-11-28), written before any timed
# data of it existed. For one size of index:
#
#   today, a vector:   assign + rows + members + endpoints + rescore + rest + write
#   the event, with m pending of a posting, a vector:
#     rows       x distinct loads / loads
#     members    whole (the estimate as it was)  |  x together/apart of the
#                member micro-benchmark with m queries (the amended one)
#     endpoints  x together/apart of the endpoint micro-benchmark with m queries
#     rescore    x distinct exact vectors / exact vectors
#     assign, rest and write whole
#
# The traversal is split in members and endpoints by the members a plan scans
# times the ns a member of the member micro-benchmark apart, with the table
# of this size; the endpoints are the rest. The second reference is today's
# insert with the delete marks by posting, measured (the passes of the lever
# binary), not subtracted.
#
# Usage: python3 -I -B gate.py --keys 10000000 --joint joint.json
#   --endpoints jointscore.json [--members jointmembers.json]
#   --today a.json,b.json [--bmember a.json,b.json --bposting a.json,b.json]
#   --write inserts.json [--census-member-ms 2.07]
import argparse, json

ap = argparse.ArgumentParser()
ap.add_argument("--keys", type=int, required=True)
ap.add_argument("--joint", required=True)
ap.add_argument("--endpoints", required=True)
ap.add_argument("--members")
ap.add_argument("--today", required=True)
ap.add_argument("--bmember")
ap.add_argument("--bposting")
ap.add_argument("--write", required=True)
ap.add_argument("--census-member-ms", type=float)
a = ap.parse_args()


def parts(paths):
    """The parts of the search of an insert, ms a plan, the mean of the passes."""
    out, n = {}, 0
    for path in paths.split(","):
        p = json.load(open(path))["plans"]
        k = p["plans"]
        one = {
            "assign": p["assignNs"] / k / 1e6, "rows": p["searchFetchNs"] / k / 1e6,
            "visit": p["searchVisitNs"] / k / 1e6, "rescore": p["searchRescoreNs"] / k / 1e6,
            "search": p["searchNs"] / k / 1e6, "loads": p["searchLoads"] / k, "members": p["searchMembers"] / k,
            "rowsRead": p["searchRows"] / k, "exact": p["searchVectorReads"] / k,
        }
        for key, v in one.items():
            out[key] = out.get(key, 0) + v
        n += 1
    out = {key: v / n for key, v in out.items()}
    out["rest"] = out["search"] - out["rows"] - out["visit"] - out["rescore"]
    out["passes"] = n
    return out


def micro(path, size_key, ratio, apart):
    """By m: the mean over the rounds of together/apart and of the ns apart."""
    by = {}
    for c in json.load(open(path))["cases"]:
        if c["keys"] != a.keys:
            continue
        by.setdefault(c["queries"], []).append((c[ratio], c[apart]))
    return {m: (sum(r for r, _ in v) / len(v), sum(x for _, x in v) / len(v), len(v)) for m, v in by.items()}


w = json.load(open(a.write))
plans = w["clocksAfter"]["plans"] - w["clocksBefore"]["plans"]
write = (w["clocksAfter"]["applyNs"] - w["clocksBefore"]["applyNs"]) / plans / 1e6

today = parts(a.today)
ends = micro(a.endpoints, "endpoints", "togetherOverApart", "apartNsAScore")
mem = micro(a.members, "members", "togetherOverApart", "apartNsAScore") if a.members else {}
bare = micro(a.members, "members", "togetherOverApartNoMarks", "apartNoMarksNsAScore") if a.members else {}
joint = json.load(open(a.joint))

print("index of %d keys | today, ms a vector (%d passes): assign %.3f, rows %.3f, traversal %.3f, rescore %.3f, rest %.3f, "
      "write %.3f | %.1f loads, %.0f members, %.0f rows read, %.0f exact vectors" % (
          a.keys, today["passes"], today["assign"], today["rows"], today["visit"], today["rescore"], today["rest"], write,
          today["loads"], today["members"], today["rowsRead"], today["exact"]))


def split(ref, ns_member):
    members = ref["members"] * ns_member / 1e6
    return members, ref["visit"] - members


def total(ref, members, endpoints):
    return ref["assign"] + ref["rows"] + members + endpoints + ref["rescore"] + ref["rest"] + write


refb = None
if a.bmember and a.bposting:
    bm, refb = parts(a.bmember), parts(a.bposting)
    print("the lever binary: marks by member, search %.3f ms (traversal %.3f); by posting, %.3f (traversal %.3f): %+.1f %% | "
          "by member against the diagnostic binary: %+.1f %% of the search" % (
              bm["search"], bm["visit"], refb["search"], refb["visit"], 100 * (refb["search"] / bm["search"] - 1),
              100 * (bm["search"] / today["search"] - 1)))

for cell in joint["cells"]:
    m = cell["pending"]
    searches = cell["searches"]
    loads_today = cell["loadsTotal"] / searches
    loads_event = cell["loadsDistinct"] / searches
    share_rows = cell["loadsDistinct"] / cell["loadsTotal"]
    share_exact = cell["exactDistinct"] / cell["exactTotal"]
    rows_a_load = today["rowsRead"] / today["loads"]
    reads_today = rows_a_load * loads_today + cell["exactTotal"] / searches
    reads_event = rows_a_load * loads_event + cell["exactDistinct"] / searches
    print("\n%d pending a posting (%d postings, %d searches)" % (m, cell["postings"], searches))
    print("  loads a vector: %.2f today, %.2f the event: %.3f (the gate: 0.200 or less) | members scored %.3f distinct, "
          "endpoints %.3f, exact vectors %.3f" % (
              loads_today, loads_event, loads_event / loads_today, cell["membersDistinct"] / cell["membersTotal"],
              cell["endpointsDistinct"] / cell["endpointsTotal"], share_exact))
    print("  reads a vector (rows and exact vectors): %.1f today, %.1f the event: %.3f" % (
        reads_today, reads_event, reads_event / reads_today))
    if m not in ends:
        print("  no endpoint micro-benchmark with %d queries and %d keys" % (m, a.keys))
        continue
    r_ends = ends[m][0]
    if m in mem:
        ns_member, r_mem = mem[m][1], mem[m][0]
        # The ns apart do not depend on m: the mean over every m steadies it.
        ns_member = sum(v[1] for v in mem.values()) / len(mem)
        origin = "the member micro-benchmark, %.1f ns a member apart" % ns_member
    elif a.census_member_ms:
        ns_member, r_mem = 1e6 * a.census_member_ms / today["members"], None
        origin = "the census, %.2f ms a plan" % a.census_member_ms
    else:
        print("  no member micro-benchmark and no census: the traversal cannot be split")
        continue
    members, endpoints = split(today, ns_member)
    t_today = total(today, members, endpoints)
    rows_e = today["rows"] * share_rows
    resc_e = today["rescore"] * share_exact
    ends_e = endpoints * r_ends
    fixed = today["assign"] + today["rest"] + write
    as_it_was = fixed + rows_e + members + ends_e + resc_e
    print("  the traversal of today: members %.3f ms, endpoints %.3f (%s)%s" % (
        members, endpoints, origin,
        "" if not a.census_member_ms else " | the census: members %.2f ms" % a.census_member_ms))
    print("  the event, ms a vector: rows %.3f, endpoints %.3f (x %.2f), rescore %.3f, assign, rest and write %.3f" % (
        rows_e, ends_e, r_ends, resc_e, fixed))
    print("  CPU, members not shared (as it was): %.3f ms against %.3f today: %.3f (the gate: 0.333 or less)" % (
        as_it_was, t_today, as_it_was / t_today))
    if r_mem is None:
        continue
    amended = fixed + rows_e + members * r_mem + ends_e + resc_e
    print("  CPU, amended (members x %.2f): %.3f ms against %.3f today: %.3f (the gate: 0.333 or less)" % (
        r_mem, amended, t_today, amended / t_today))
    if refb is None:
        continue
    ns_bare = sum(v[1] for v in bare.values()) / len(bare)
    members_b, endpoints_b = split(refb, ns_bare)
    t_b = total(refb, members_b, endpoints_b)
    print("  against today with the marks by posting (%.3f ms): as it was %.3f, amended %.3f" % (
        t_b, as_it_was / t_b, amended / t_b))
    fixed_b = refb["assign"] + refb["rest"] + write
    event_b = (fixed_b + refb["rows"] * share_rows + members_b * bare[m][0] + endpoints_b * r_ends
               + refb["rescore"] * share_exact)
    print("  the event with the marks by posting too (does not decide): %.3f ms: %.3f of today with them" % (
        event_b, event_b / t_b))
