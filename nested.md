Nested Property Filtering — Design Summary
  
  ---
  Reference Documents

  doc123:
  {"owner":"Marsha","addresses":[{"city":"Berlin","postcode":"10115"}],
   "tags":["german","premium"],
   "cars":[{"make":"BMW","tires":[{"width":225}]}]}
  doc124:
  {"owner":"Justin","addresses":[{"city":"Madrid","postcode":"28001"},{"city":"London","postcode":"SW1"}],
   "tags":["german","japanese","sedan"],
   "cars":[{"make":"Audi","tires":[{"width":205},{"width":225}]},{"make":"Kia","tires":[{"width":195}]}]}
  doc125:
  {"owner":"Anna","addresses":[{"city":"Paris","postcode":"75001"}],
   "tags":["electric"],
   "cars":[{"make":"Tesla","tires":[{"width":245}],"accessories":[{"type":"charger"},{"type":"mats"}]}]}

 ---
  1. Bitmap Value Encoding

  To be able to filter within the same object/element of objects/elements array, its position in the array has to be stored aside document id.
  Document ids in filterable and rangeable indexes are stored in roaring bitmaps. Bitmaps can store uint64 values, same type that weaviate use for document ids.
  That means positions and document ids have to be combined into single uint64.

  Every value stored in the inverted index is a 64-bit integer encoding three components:

  | root_idx (16 bits) | leaf_idx (16 bits) | docID (32 bits) |
  | bits 63-48         | bits 47-32         | bits 31-0       |

  - root_idx: 1-based. For standalone object always 1 (document = implicit 1-element array). For object[] property = element index.
  - leaf_idx: 1-based contiguous counter per root across all leaf arrays in the document.
  - docID: 32-bit internal document identifier.

  Limits: 65,535 (2^16) roots × 65,535 (2^16) leaves per root × ~4B (2^32) docIDs
  Notation: r{root}|l{leaf}|d{docID} — e.g., r1|l3|d124 = (root_idx=1 << 48) + (leaf_idx=3 << 32) + docId=124 = 281_487_861_612_668

  RoaringSet mapping: high 32 bits (root|leaf) = container key; low 32 bits (docID) = value within container. Operations on container keys are O(containers).

  Why positions, not just docIDs? Positions encode element identity within arrays, enabling:
  - Same-element filtering (city=X AND postcode=Y on the same address)
  - Cross-sibling correlation (same parent element)
  - Positional access (addresses[0])
  - ALL/NONE operators & per-element negation

  NOTE: 32 bits to store docID limits max docID in the bitmap to 4,294,967,296. If number of supported roots and leaves per root would be decreased to 16,384 (2^14), there will be 36 bits left for docID bringing max supported docID to 68,719,476,736.
        If 68B (2^36) is still too low, sroar library can be modified to use additional 16 bits of containers' keys (keys are uin64 values with lowest 16 bits set to 0). That change would allow to use 16 bits for root_idx (65,535), 16 bits for leaf_idx (65,535) and 48 bits for docID (281,474,976,710,656).


  ---
  2. Two LSM Buckets

  All documents and properties share exactly 2 buckets (StrategyRoaringSet):
  To avoid unbounded number of buckets created per each object subproperty, one common Values bucket per filter type will be created for property.
  Initially only filterable index will be supported for nested objects, therefore Values bucket is of RoaringSet strategy.
  Each value stored in Values bucket will be prefixed with hashed subproperty path.
  To support rangeable index another bucket of RoaringSetRange strategy will be introduced in the future to hold Values of indexed subproperties.

  NOTE: bucket of RoaringSetRange currently supports just keys of uin8 type indicating bits of indexed values. It has to be modified to support prefixes in order to store multiple subproperties in single bucket.

  ┌──────────┬───────────────────────────────────────────┬─────────────────────────────────────────────────────┐
  │  Bucket  │                Key Format                 │                 Value                               │
  ├──────────┼───────────────────────────────────────────┼─────────────────────────────────────────────────────┤
  │ Values   │ hash8(prop_path) + encoded_value          │ RoaringSet bitmap of composite positions and docIds │
  ├──────────┼───────────────────────────────────────────┼─────────────────────────────────────────────────────┤
  │ Metadata │ hash8("_idx." + array_path) + BE16(index) │ RoaringSet bitmap of composite positions and docIds │
  │          ├───────────────────────────────────────────┼─────────────────────────────────────────────────────┤
  │          │ hash8("_exists." + path)                  │ RoaringSet bitmap of composite positions and docIds │
  └──────────┴───────────────────────────────────────────┴─────────────────────────────────────────────────────┘

  - hash8(path) — 8-byte hash of dot-notation property path → constant prefix size regardless of nesting depth
  - encoded_value — standard per-type encoding (text, number, bool, etc.)
  - BE16(index) — big-endian uint16, 0-based array element index. uint16 is sufficient as no more than 2^16 roots or leaves will be supported

  Hash function choice for 8 bytes:
  - xxHash64 — excellent distribution, extremely fast, commonly used in Go (github.com/cespare/xxhash/v2). Likely already in Weaviate's dependency tree.
  - FNV-64a — in Go's standard library (hash/fnv), zero external dependency, good enough distribution for short strings.
  8 bytes should be enough for short paths and close to 0 collision probability.


  ---
  3. Position Assignment

  Rules (depth-first walk):
  1. Document itself = implicit 1-element array → root=1 always
  2. Leaf arrays (no sub-arrays within elements, including scalar arrays): each element gets the next leaf_idx
  3. Intermediate arrays (elements contain sub-arrays): elements don't get their own leaf — they inherit all descendant leaf positions
  4. Scalars at any level: inherit all descendant leaf positions from that level
  5. Object treated as 1-element object[] (single code path)

  doc123 (Marsha) — root=1, 4 leaves

  ┌──────────────────┬──────┐
  │     Element      │ Leaf │
  ├──────────────────┼──────┤
  │ addresses[0]     │ l1   │
  ├──────────────────┼──────┤
  │ tags[0]          │ l2   │
  ├──────────────────┼──────┤
  │ tags[1]          │ l3   │
  ├──────────────────┼──────┤
  │ cars[0].tires[0] │ l4   │
  └──────────────────┴──────┘

  Intermediate: cars[0] → {l4}. Doc-level scalar: owner → {l1, l2, l3, l4}

  doc124 (Justin) — root=1, 8 leaves

  ┌──────────────────┬──────┐
  │     Element      │ Leaf │
  ├──────────────────┼──────┤
  │ addresses[0]     │ l1   │
  ├──────────────────┼──────┤
  │ addresses[1]     │ l2   │
  ├──────────────────┼──────┤
  │ tags[0]          │ l3   │
  ├──────────────────┼──────┤
  │ tags[1]          │ l4   │
  ├──────────────────┼──────┤
  │ tags[2]          │ l5   │
  ├──────────────────┼──────┤
  │ cars[0].tires[0] │ l6   │
  ├──────────────────┼──────┤
  │ cars[0].tires[1] │ l7   │
  ├──────────────────┼──────┤
  │ cars[1].tires[0] │ l8   │
  └──────────────────┴──────┘

  Intermediate: cars[0] → {l6, l7}, cars[1] → {l8}. Doc-level scalar: owner → {l1..l8}

  doc125 (Anna) — root=1, 5 leaves

  ┌────────────────────────┬──────┐
  │        Element         │ Leaf │
  ├────────────────────────┼──────┤
  │ addresses[0]           │ l1   │
  ├────────────────────────┼──────┤
  │ tags[0]                │ l2   │
  ├────────────────────────┼──────┤
  │ cars[0].tires[0]       │ l3   │
  ├────────────────────────┼──────┤
  │ cars[0].accessories[0] │ l4   │
  ├────────────────────────┼──────┤
  │ cars[0].accessories[1] │ l5   │
  └────────────────────────┴──────┘

  Intermediate: cars[0] → {l3, l4, l5}. Doc-level scalar: owner → {l1..l5}

  ---
  4. Bitmap Operations

  ┌──────────────┬──────────────────────────────────────────────┬───────────────────────────────────────────────────────────┐
  │  Operation   │                  Mechanics                   │                        When to use                        │
  ├──────────────┼──────────────────────────────────────────────┼───────────────────────────────────────────────────────────┤
  │ Direct AND   │ Positions match exactly (root+leaf+docID)    │ Same-element; ancestor-descendant; doc-level scalar + any │
  ├──────────────┼──────────────────────────────────────────────┼───────────────────────────────────────────────────────────┤
  │ MaskPosition │ Zeros bits 47-32 (leaf), keeps root+docID    │ Cross-sibling subtrees under same level                   │
  ├──────────────┼──────────────────────────────────────────────┼───────────────────────────────────────────────────────────┤
  │ Strip        │ Zeros bits 63-32, keeps docID only           │ Final result extraction                                   │
  ├──────────────┼──────────────────────────────────────────────┼───────────────────────────────────────────────────────────┤
  │ ANDNOT       │ Raw position subtraction                     │ Negation (preserves per-element precision)                │
  ├──────────────┼──────────────────────────────────────────────┼───────────────────────────────────────────────────────────┤
  │ _idx loop    │ Iterate element positions via _idx per index │ Cross-sibling under intermediate array                    │
  └──────────────┴──────────────────────────────────────────────┴───────────────────────────────────────────────────────────┘

  NOTE: 
    - MaskPosition() and Strip() methods have to be added to sroar library
    - "Direct AND" and "ANDNOT" are respectively sroar's And() and AndNot()
    - "_idx loop" process of traversing through _idx entries to verify whether filtered values belong to the same element of nested (intermediate, not root) array.
  

  Decision table:

  ┌──────────────────────────────────────┬────────────────────────────────────────────────────────────────┐
  │             Relationship             │                           Operation                            │
  ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
  │ Same property, same element          │ Direct AND                                                     │
  ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
  │ Ancestor → descendant (same subtree) │ Direct AND                                                     │
  ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
  │ Doc-level scalar + any nested        │ Direct AND (full propagation)                                  │
  ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
  │ Sibling subtrees under root          │ MaskPosition + AND                                             │
  ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
  │ Sibling arrays under intermediate    │ MaskPosition pre-filter + _idx loop                            │
  ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
  │ Negation                             │ ANDNOT on raw positions                                        │
  ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
  │ ANY                                  │ Default — filter + Strip                                       │
  ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
  │ ALL                                  │ allElements ANDNOT passing → failDocs; allDocs ANDNOT failDocs │
  ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
  │ NONE                                 │ Strip(matching) → matchDocs; allDocs ANDNOT matchDocs          │
  ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
  │ Position access arr[N]               │ filter AND _idx[N]                                             │
  └──────────────────────────────────────┴────────────────────────────────────────────────────────────────┘


  ---
  5. Complete Index

  Values Bucket

  // key = hash8(path) + encoded_value → RoaringSet bitmap

  hash8("owner") + "Marsha"              → {r1|l1..l4|d123}
  hash8("owner") + "Justin"              → {r1|l1..l8|d124}
  hash8("owner") + "Anna"                → {r1|l1..l5|d125}

  hash8("addresses.city") + "Berlin"     → {r1|l1|d123}
  hash8("addresses.city") + "Madrid"     → {r1|l1|d124}
  hash8("addresses.city") + "London"     → {r1|l2|d124}
  hash8("addresses.city") + "Paris"      → {r1|l1|d125}

  hash8("addresses.postcode") + "10115"  → {r1|l1|d123}
  hash8("addresses.postcode") + "28001"  → {r1|l1|d124}
  hash8("addresses.postcode") + "SW1"    → {r1|l2|d124}
  hash8("addresses.postcode") + "75001"  → {r1|l1|d125}

  hash8("tags") + "german"               → {r1|l2|d123, r1|l3|d124}
  hash8("tags") + "premium"              → {r1|l3|d123}
  hash8("tags") + "japanese"             → {r1|l4|d124}
  hash8("tags") + "sedan"                → {r1|l5|d124}
  hash8("tags") + "electric"             → {r1|l2|d125}

  hash8("cars.make") + "BMW"             → {r1|l4|d123}
  hash8("cars.make") + "Audi"            → {r1|l6|d124, r1|l7|d124}
  hash8("cars.make") + "Kia"             → {r1|l8|d124}
  hash8("cars.make") + "Tesla"           → {r1|l3..l5|d125}

  hash8("cars.tires.width") + 225        → {r1|l4|d123, r1|l7|d124}
  hash8("cars.tires.width") + 205        → {r1|l6|d124}
  hash8("cars.tires.width") + 195        → {r1|l8|d124}
  hash8("cars.tires.width") + 245        → {r1|l3|d125}

  hash8("cars.accessories.type") + "charger" → {r1|l4|d125}
  hash8("cars.accessories.type") + "mats"    → {r1|l5|d125}

  Metadata Bucket

  // _idx entries (key = hash8("_idx." + path) + BE16(N)):

  hash8("_idx.addresses") + BE16(0)          → {r1|l1|d123, r1|l1|d124, r1|l1|d125}
  hash8("_idx.addresses") + BE16(1)          → {r1|l2|d124}

  hash8("_idx.tags") + BE16(0)               → {r1|l2|d123, r1|l3|d124, r1|l2|d125}
  hash8("_idx.tags") + BE16(1)               → {r1|l3|d123, r1|l4|d124}
  hash8("_idx.tags") + BE16(2)               → {r1|l5|d124}

  hash8("_idx.cars") + BE16(0)               → {r1|l4|d123, r1|l6|d124, r1|l7|d124, r1|l3..l5|d125}
  hash8("_idx.cars") + BE16(1)               → {r1|l8|d124}

  hash8("_idx.cars.tires") + BE16(0)         → {r1|l4|d123, r1|l6|d124, r1|l8|d124, r1|l3|d125}
  hash8("_idx.cars.tires") + BE16(1)         → {r1|l7|d124}

  hash8("_idx.cars.accessories") + BE16(0)   → {r1|l4|d125}
  hash8("_idx.cars.accessories") + BE16(1)   → {r1|l5|d125}

  // _exists entries (key = hash8("_exists." + path)):

  hash8("_exists.owner")                  → {r1|l1..l4|d123, r1|l1..l8|d124, r1|l1..l5|d125}
  hash8("_exists.addresses")              → {r1|l1|d123, r1|l1|d124, r1|l2|d124, r1|l1|d125}
  hash8("_exists.addresses.city")         → {r1|l1|d123, r1|l1|d124, r1|l2|d124, r1|l1|d125}
  hash8("_exists.addresses.postcode")     → {r1|l1|d123, r1|l1|d124, r1|l2|d124, r1|l1|d125}
  hash8("_exists.tags")                   → {r1|l2|d123, r1|l3|d123, r1|l3..l5|d124, r1|l2|d125}
  hash8("_exists.cars")                   → {r1|l4|d123, r1|l6..l8|d124, r1|l3..l5|d125}
  hash8("_exists.cars.make")              → {r1|l4|d123, r1|l6..l8|d124, r1|l3..l5|d125}
  hash8("_exists.cars.tires")             → {r1|l4|d123, r1|l6..l8|d124, r1|l3|d125}
  hash8("_exists.cars.tires.width")       → {r1|l4|d123, r1|l6..l8|d124, r1|l3|d125}
  hash8("_exists.cars.accessories")       → {r1|l4|d125, r1|l5|d125}
  hash8("_exists.cars.accessories.type")  → {r1|l4|d125, r1|l5|d125}


  ---
  6. Filtering Examples — User Story 1: Scalar Arrays

  1.1 Exact match on scalar array values

  a) tags = "german"
  → Lookup hash8("tags")+"german" → {r1|l2|d123, r1|l3|d124} → Strip → {d123, d124}

  b) tags = "electric"
  → Lookup → {r1|l2|d125} → Strip → {d125}

  c) tags = "sedan"
  → Lookup → {r1|l5|d124} → Strip → {d124}

  1.2 Contains semantics

  a) tags ContainsAll ["german", "premium"]
  → A=tags="german" → MaskPos → {r1|d123, r1|d124}
  → B=tags="premium" → MaskPos → {r1|d123}
  → AND → {r1|d123} → Strip → {d123}

  b) tags ContainsAny ["japanese", "electric"]
  → A = tags="japanese" ∪ tags="electric" → {r1|l4|d124, r1|l2|d125}
  → Strip → {d124, d125}

  c) tags ContainsAll ["german", "japanese"]
  → MaskPos(tags="german") AND MaskPos(tags="japanese") → {r1|d124}
  → Strip → {d124}

  NOTE: ContainsAny only needs at least one value to exist — so we union all matching bitmaps and Strip directly to docIDs. No correlation between values needed.
        ContainsAll needs every specified value present in the same document. Since different values sit at different leaf positions (different array elements), Direct AND would give empty (l2 ≠ l3). MaskPosition zeros out the leaf bits so we can AND on just root+docID, confirming both values belong to the same document.

  1.3 Position within array

  a) tags[0] = "german" (0-indexed → _idx.tags BE16(0))
  → A=tags="german" AND _idx.tags[0] → {r1|l2|d123, r1|l3|d124}
  → Strip → {d123, d124}

  b) tags[1] = "premium" (→ _idx.tags BE16(1))
  → A=tags="premium" AND _idx.tags[1] → {r1|l3|d123}
  → Strip → {d123}

  c) tags[2] = "sedan" (→ _idx.tags BE16(2))
  → A=tags="sedan" AND _idx.tags[2] → {r1|l5|d124}
  → Strip → {d124}

  1.4 Multiple conditions on same array field

  a) tags = "german" AND tags = "premium" (same array, different elements)
  → MaskPos(tags="german") AND MaskPos(tags="premium") → {r1|d123}
  → Strip → {d123}

  b) tags = "german" AND tags = "japanese"
  → MaskPos(tags="german") AND MaskPos(tags="japanese") → {r1|d124}
  → Strip → {d124}

  c) tags[0] = "german" AND tags[1] = "japanese"
  → A = tags="german" AND _idx.tags[0] → {r1|l2|d123, r1|l3|d124}
  → B = tags="japanese" AND _idx.tags[1] → {r1|l4|d124}
  → MaskPos(A) AND MaskPos(B) → {r1|d124}
  → Strip → {d124}

  1.5 Presence check (JSON Any)

  a) HAS(tags)
  → _exists.tags → Strip → {d123, d124, d125}

  b) HAS(cars.accessories)
  → _exists.cars.accessories → Strip → {d125}

  c) HAS(addresses.postcode)
  → _exists.addresses.postcode → Strip → {d123, d124, d125}

  1.6 Negation

  a) tags != "german" (doc-level: documents where NO tag is "german")
  → failDocs = Strip(tags="german") → {d123, d124}
  → allDocs ANDNOT failDocs → {d125}

  b) tags NOT IN ["japanese", "sedan"]
  → failDocs = Strip(tags="japanese" ∪ tags="sedan") → {d124}
  → allDocs ANDNOT failDocs → {d123, d125}

  c) Per-element: tags != "german" (tag elements that are NOT "german")
  → all = _exists.tags, match = tags="german"
  → all ANDNOT match → {r1|l3|d123, r1|l4|d124, r1|l5|d124, r1|l2|d125}
  (premium, japanese, sedan, electric remain)

  1.7 Null state

  a) cars.accessories IS NULL (documents with no accessories)
  → hasProp = Strip(_exists.cars.accessories) → {d125}
  → allDocs ANDNOT hasProp → {d123, d124}

  b) tags IS NULL (documents with no tags — none in our data)
  → hasProp = Strip(_exists.tags) → {d123, d124, d125}
  → allDocs ANDNOT hasProp → {}

  c) Per-element: which car elements lack accessories?
  → MaskPos(_exists.cars) ANDNOT MaskPos(_exists.cars.accessories) → {r1|d123, r1|d124}
  → Strip → {d123, d124} (BMW, Audi, Kia — none have accessories)

  ---
  7. Filtering Examples — User Story 2: Nested Objects

  2.1 Exact match on scalar values in a path

  a) addresses.city = "Berlin"
  → Lookup → {r1|l1|d123} → Strip → {d123}

  b) cars.tires.width = 225
  → Lookup → {r1|l4|d123, r1|l7|d124} → Strip → {d123, d124}

  c) cars.accessories.type = "charger"
  → Lookup → {r1|l4|d125} → Strip → {d125}

  2.2 Multiple conditions on scalar values in a path (same element)

  a) addresses.city = "Madrid" AND addresses.postcode = "28001"
  → A={r1|l1|d124}, B={r1|l1|d124}
  → Direct AND → {r1|l1|d124} → Strip → {d124} ✓ same address element

  b) cars.make = "Audi" AND cars.tires.width = 205 (ancestor-descendant)
  → A=cars.make="Audi"→{r1|l6|d124, r1|l7|d124}, B=tires.width=205→{r1|l6|d124}
  → Direct AND → {r1|l6|d124} → Strip → {d124} ✓ Audi's first tire

  c) cars.make = "Audi" AND cars.tires.width = 195 (different car elements)
  → A={r1|l6|d124, r1|l7|d124}, B={r1|l8|d124}
  → Direct AND → {} ✓ correct: 195 belongs to Kia, not Audi

  2.3 Multiple conditions on a document in a path (cross-subtree)

  a) addresses.city = "Madrid" AND cars.make = "Audi" (sibling subtrees)
  → MaskPos(addresses.city="Madrid") AND MaskPos(cars.make="Audi") → {r1|d124}
  → Strip → {d124}

  b) owner = "Anna" AND cars.accessories.type = "charger" (doc-level + nested)
  → A=owner="Anna"→{r1|l1..l5|d125}, B={r1|l4|d125}
  → Direct AND → {r1|l4|d125} → Strip → {d125} (no MaskPos needed — owner has full propagation)

  c) addresses.city = "Berlin" AND cars.tires.width = 225
  → MaskPos({r1|l1|d123}) AND MaskPos({r1|l4|d123, r1|l7|d124})
  → {r1|d123} AND {r1|d123, r1|d124} → {r1|d123}
  → Strip → {d123}

  2.4 Contains semantics (partial or complete document match)

  a) addresses contains {city:"Madrid", postcode:"28001"} (exact element)
  → A=city="Madrid"→{r1|l1|d124}, B=postcode="28001"→{r1|l1|d124}
  → Direct AND → {r1|l1|d124} → Strip → {d124} ✓ same address

  b) cars contains {make:"Tesla"} (partial match)
  → cars.make="Tesla" → {r1|l3..l5|d125} → Strip → {d125}

  c) cars contains {make:"Audi", tires:[{width:225}]} (nested partial)
  → A=cars.make="Audi"→{r1|l6|d124, r1|l7|d124}, B=tires.width=225→{r1|l4|d123, r1|l7|d124}
  → Direct AND → {r1|l7|d124} → Strip → {d124} ✓ Audi's 2nd tire is 225

  2.5 Negation

  a) addresses.city != "Berlin" (doc-level: no address has Berlin)
  → failDocs = Strip(addresses.city="Berlin") → {d123}
  → allDocs ANDNOT failDocs → {d124, d125}

  b) cars.make NOT IN ["BMW", "Kia"]
  → failDocs = Strip(cars.make="BMW" ∪ cars.make="Kia") → {d123, d124}
  → allDocs ANDNOT failDocs → {d125}

  c) Per-element: cars.make != "Audi" (car elements that are NOT Audi)
  → all = _exists.cars.make, match = cars.make="Audi"
  → all ANDNOT match → {r1|l4|d123, r1|l8|d124, r1|l3..l5|d125}
  (BMW, Kia, Tesla elements remain) → Strip → {d123, d124, d125}

  ---
  8. Filtering Examples — User Story 3: Arrays of Objects

  (all requirements from stories 1 & 2 apply; these are the additional requirements)

  3.1 Filtering at any arbitrary level

  a) cars.tires.width > 200 (2 levels deep)
  → Range scan → {r1|l4|d123(225), r1|l6|d124(205), r1|l7|d124(225), r1|l3|d125(245)}
  → Strip → {d123, d124, d125} (195 excluded — Kia's tire)

  b) cars.accessories.type = "mats" (3 levels through intermediate)
  → Lookup → {r1|l5|d125} → Strip → {d125}

  c) cars.tires.width = 245 AND cars.accessories.type = "mats" (cross-sibling under intermediate)
  → A=tires.width=245→{r1|l3|d125}, B=acc.type="mats"→{r1|l5|d125}
  → MaskPos pre-filter: {r1|d125} → candidate d125
  → _idx.cars loop: _idx.cars[0]={r1|l3..l5|d125}
    → A AND cars[0] = {r1|l3|d125} ✓, B AND cars[0] = {r1|l5|d125} ✓ → same car → MATCH
  → Strip → {d125}

  3.2 Any/All/None

  a) ANY(cars.tires.width > 200) — at least one tire wider than 200
  → match = tires.width > 200 → {r1|l4|d123, r1|l6|d124, r1|l7|d124, r1|l3|d125}
  → Strip → {d123, d124, d125} (ANY is default — just filter & strip)

  b) ALL(cars.tires.width > 200) — every tire must be wider than 200
  → passing = tires.width > 200 → {r1|l4|d123, r1|l6|d124, r1|l7|d124, r1|l3|d125}
  → allTires = _exists.cars.tires.width → {r1|l4|d123, r1|l6..l8|d124, r1|l3|d125}
  → failing = allTires ANDNOT passing → {r1|l8|d124} (Kia's 195 tire)
  → failDocs = Strip(failing) → {d124}
  → allDocs ANDNOT failDocs → {d123, d125}

  c) NONE(cars.make = "BMW") — no car is BMW
  → matchDocs = Strip(cars.make="BMW") → {d123}
  → allDocs ANDNOT matchDocs → {d124, d125}
  

