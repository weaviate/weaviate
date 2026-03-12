Nested Property Filtering — Design Summary
    
---
1. Bitmap Value Encoding

    To be able to filter within the same object/element of objects/elements array, its position in the array has to be stored aside document id.
    Document ids in filterable and rangeable indexes are stored in roaring bitmaps. Bitmaps can store uint64 values, same type that weaviate use for document ids.
    That means positions and document ids have to be combined into single uint64.

    Every value stored in the inverted index is a 64-bit integer encoding three components:

    | root_idx (16 bits) | leaf_idx (16 bits) | docID (32 bits) |
    | bits 63-48                 | bits 47-32                 | bits 31-0             |

    - root_idx: 1-based. For standalone object always 1 (document = implicit 1-element array). For object[] property = element index.
    - leaf_idx: 1-based contiguous counter per root across all leaf arrays in the document.
    - docID: 32-bit internal document identifier.

    Limits: 65,535 (2^16) roots × 65,535 (2^16) leaves per root × ~4B (2^32) docIDs
    Notation: r{root}|l{leaf}|d{docID} — e.g., r1|l3|d124 = (root_idx=1 << 48) + (leaf_idx=3 << 32) + docId=124 = 281_487_861_612_668

    RoaringSet mapping: high 32 bits (root|leaf) = container key; low 32 bits (docID) = value within container (lowest 16 bits) and container key (highest 16 bits). Operations on container keys are O(containers).

    Why positions, not just docIDs? Positions encode element identity within arrays, enabling:
    - Same-element filtering (city=X AND postcode=Y on the same address)
    - Cross-sibling correlation (same parent element)
    - Positional access (addresses[0])
    - ALL/NONE operators & per-element negation

    NOTE: 
        32 bits to store docID limits max docID in the bitmap to 4,294,967,296 (4B). By decreasing number of supported roots and leaves per root numbers, max docID can be increased:
        - root_idx and leaf_idx = 16,384 (2^14) => max docID = 68,719,476,736 (2^36 = 68B)
        - root_idx and leaf_idx = 4,096 (2^12) => max docID = 1,099,511,627,776 (2^40 = 1T)
        If those values are still to strict, sroar library can be modified to use additional 16 bits of containers' keys (keys are uin64 values with lowest 16 bits set to 0). Such change would allow to use 16 bits for root_idx (65,535), 16 bits for leaf_idx (65,535) and 48 bits for docID (281,474,976,710,656).


---
2. Two LSM Buckets

    All documents and properties share exactly 2 buckets (StrategyRoaringSet):
    To avoid unbounded number of buckets created per each object subproperty, one common Values bucket per filter type will be created for property.
    Initially only filterable index will be supported for nested objects, therefore Values bucket is of RoaringSet strategy.
    Each value stored in Values bucket will be prefixed with hashed subproperty path.
    To support rangeable index another bucket of RoaringSetRange strategy will be introduced in the future to hold Values of indexed subproperties.

    NOTE: bucket of RoaringSetRange currently supports just keys of uin8 type indicating bits of indexed values. It has to be modified to support prefixes in order to store multiple subproperties in single bucket.

    ┌──────────┬───────────────────────────────────────────┬─────────────────────────────────────────────────────┐
    │  Bucket  │             Key Format                    │                    Value                            │
    ├──────────┼───────────────────────────────────────────┼─────────────────────────────────────────────────────┤
    │ Values   │ hash8(prop_path) + encoded_value          │ RoaringSet bitmap of composite positions and docIds │
    ├──────────┼───────────────────────────────────────────┼─────────────────────────────────────────────────────┤
    │ Metadata │ hash8("_idx." + array_path) + BE16(index) │ RoaringSet bitmap of composite positions and docIds │
    │          ├───────────────────────────────────────────┼─────────────────────────────────────────────────────┤
    │          │ hash8("_exists." + path)                  │ RoaringSet bitmap of composite positions and docIds │
    └──────────┴───────────────────────────────────────────┴─────────────────────────────────────────────────────┘

    - hash8(path) — 8-byte hash of dot-notation property path → constant prefix size regardless of nesting depth. should be enough to minimize collision probability
    - encoded_value — standard per-type encoding (text, number, bool, etc.)
    - BE16(index) — big-endian uint16, 0-based array element index. uint16 is sufficient as no more than 2^16 roots or leaves will be supported

    Hash function choice for 8 bytes:
    - xxHash64 — excellent distribution, extremely fast, commonly used in Go (github.com/cespare/xxhash/v2). Likely already in Weaviate's dependency tree.
    - FNV-64a — in Go's standard library (hash/fnv), zero external dependency, good enough distribution for short strings.


---
3. Position Assignment

    Reference documents (property of type object)  

        doc123: {
            "name": "subdoc_123"
            "owner":{
                "firstname":"Marsha",
                "lastname":"Mallow",
                "nicknames":["Marshmallow", "M&M"]
            }
            "addresses":[{
                "city":"Berlin",
                "postcode":"10115",
                "numbers":[123, 1123]
            }],
            "tags":["german","premium"],
            "cars":[{
                "make":"BMW",
                "tires":[{
                    "width":225
                    "radiuses":[18,19]
                }],
                "colors":["black","orange"]
            }]
        }
        doc124: {
            "name": "subdoc_124"
            "owner":{
                "firstname":"Justin",
                "lastname":"Time",
                "nicknames":["watch"]
            }
            "addresses":[{
                "city":"Madrid",
                "postcode":"28001"
                "numbers":[124]
            },{
                "city":"London",
                "postcode":"SW1"
            }],
            "tags":["german","japanese","sedan"],
            "cars":[{
                "make":"Audi",
                "tires":[{
                    "width":205,
                    "radiuses":[17,18]
                },{
                    "width":225
                }]
            },{
                "make":"Kia",
                "tires":[{
                    "width":195
                    "radiuses":[]
                }],
                "colors":["white"]
            }]
        }
        doc125:{
            "name": "subdoc_125"
            "owner":{
                "firstname":"Anna",
                "lastname":"Wanna"
            }
            "addresses":[{
                "city":"Paris",
                "postcode":"75001",
                "numbers":[125]
            }],
            "tags":["electric"],
            "cars":[{
                "make":"Tesla",
                "tires":[{
                    "width":245
                    "radiuses":[18,19,20]
                }],
                "accessories":[{
                    "type":"charger"
                },{
                    "type":"mats"
                }],
                "colors":["yellow"]
            }]
        }

    Assignments:

        - doc123 (Marsha) — root=1, 10 leaves

        owner (intermediate) → {l1, l2}
        ├─ firstname="Marsha" → {l1, l2}
        ├─ lastname="Mallow" → {l1, l2}
        ├─ nicknames[0]="Marshmallow" → l1
        └─ nicknames[1]="M&M" → l2
        addresses[0] (intermediate) → {l3, l4}
        ├─ city="Berlin" → {l3, l4}
        ├─ postcode="10115" → {l3, l4}
        ├─ numbers[0]=123 → l3
        └─ numbers[1]=1123 → l4
        tags[0]="german" → l5
        tags[1]="premium" → l6
        cars[0] (intermediate) → {l7..l10}
        ├─ make="BMW" → {l7..l10}
        ├─ tires[0] (intermediate) → {l7, l8}
        │  ├─ width=225 → {l7, l8}
        │  ├─ radiuses[0]=18 → l7
        │  └─ radiuses[1]=19 → l8
        ├─ colors[0]="black" → l9
        └─ colors[1]="orange" → l10
        name="subdoc_123" → {l1..l10}


        - doc124 (Justin) — root=1, 11 leaves

        owner (intermediate) → {l1}
        ├─ firstname="Justin" → {l1}
        ├─ lastname="Time" → {l1}
        └─ nicknames[0]="watch" → l1
        addresses[0] (intermediate) → {l2}
        ├─ city="Madrid" → {l2}
        ├─ postcode="28001" → {l2}
        └─ numbers[0]=124 → l2
        addresses[1] (leaf, no numbers) → l3
        ├─ city="London" → {l3}
        └─ postcode="SW1" → {l3}
        tags[0]="german" → l4
        tags[1]="japanese" → l5
        tags[2]="sedan" → l6
        cars[0] (intermediate) → {l7..l9}
        ├─ make="Audi" → {l7..l9}
        ├─ tires[0] (intermediate) → {l7, l8}
        │  ├─ width=205 → {l7, l8}
        │  ├─ radiuses[0]=17 → l7
        │  └─ radiuses[1]=18 → l8
        └─ tires[1] (leaf, no radiuses) → l9
            └─ width=225 → {l9}
        cars[1] (intermediate) → {l10, l11}
        ├─ make="Kia" → {l10, l11}
        ├─ tires[0] (leaf, radiuses=[]) → l10
        │  └─ width=195 → {l10}
        └─ colors[0]="white" → l11
        name="subdoc_124" → {l1..l11}


        - doc125 (Anna) — root=1, 9 leaves

        owner (leaf, no nicknames) → l1
        ├─ firstname="Anna" → {l1}
        └─ lastname="Wanna" → {l1}
        addresses[0] (intermediate) → {l2}
        ├─ city="Paris" → {l2}
        ├─ postcode="75001" → {l2}
        └─ numbers[0]=125 → l2
        tags[0]="electric" → l3
        cars[0] (intermediate) → {l4..l9}
        ├─ make="Tesla" → {l4..l9}
        ├─ tires[0] (intermediate) → {l4..l6}
        │  ├─ width=245 → {l4..l6}
        │  ├─ radiuses[0]=18 → l4
        │  ├─ radiuses[1]=19 → l5
        │  └─ radiuses[2]=20 → l6
        ├─ accessories[0] (leaf) → l7
        │  └─ type="charger" → {l7}
        ├─ accessories[1] (leaf) → l8
        │  └─ type="mats" → {l8}
        └─ colors[0]="yellow" → l9
        name="subdoc_125" → {l1..l9}


    Key observations:
        - owner is a single object → treated as 1-element array → intermediate (has nicknames sub-array)
        - addresses[] → intermediate (has numbers sub-array)
        - cars[] → intermediate (has tires, colors, accessories sub-arrays)
        - cars[].tires[] → intermediate (has radiuses sub-array)
        - Elements with no descendant leaves (addresses[1] in d124, tires[1] in d124/Audi, tires[0] in d124/Kia with empty radiuses[], owner in d125 with no nicknames) → get their own leaf position    


    Rules (depth-first walk):

        1. Document = implicit 1-element object[] → root=1 always for standalone documents
        2. Object = 1-element object[] → unified code path
        3. Walk depth-first, processing nested arrays recursively before their parent
        4. Scalar array elements (text[], number[], etc.): each element gets the next leaf_idx
        5. Object array elements (object / object[]):
            - First, recursively process all nested arrays within the element to collect descendant leaf positions
            - Has descendants → intermediate: element's positions = union of all descendant leaf positions. No new leaf_idx assigned.
            - No descendants (no sub-arrays present, all sub-arrays empty, or sub-array properties missing entirely) → assign the next leaf_idx directly to this element
        6. Scalar properties at any level: inherit ALL leaf positions of their parent element

    Illustrated by edge cases from the examples:

        ┌───────────────────────┬─────────────────────────────────┬─────────────┬──────────────┐
        │        Element        │           Sub-arrays            │ Descendants │  Assignment  │
        ├───────────────────────┼─────────────────────────────────┼─────────────┼──────────────┤
        │ d123 owner            │ nicknames=["Marshmallow","M&M"] │ {l1, l2}    │ intermediate │
        ├───────────────────────┼─────────────────────────────────┼─────────────┼──────────────┤
        │ d125 owner            │ no nicknames property           │ none        │ gets own l1  │
        ├───────────────────────┼─────────────────────────────────┼─────────────┼──────────────┤
        │ d124 addresses[0]     │ numbers=[124]                   │ {l2}        │ intermediate │
        ├───────────────────────┼─────────────────────────────────┼─────────────┼──────────────┤
        │ d124 addresses[1]     │ no numbers property             │ none        │ gets own l3  │
        ├───────────────────────┼─────────────────────────────────┼─────────────┼──────────────┤
        │ d124 cars[0].tires[0] │ radiuses=[17,18]                │ {l7, l8}    │ intermediate │
        ├───────────────────────┼─────────────────────────────────┼─────────────┼──────────────┤
        │ d124 cars[0].tires[1] │ no radiuses property            │ none        │ gets own l9  │
        ├───────────────────────┼─────────────────────────────────┼─────────────┼──────────────┤
        │ d124 cars[1].tires[0] │ radiuses=[] (empty)             │ none        │ gets own l10 │
        └───────────────────────┴─────────────────────────────────┴─────────────┴──────────────┘

---
4. Bitmap Operations

    ┌──────────────┬──────────────────────────────────────────────────────────────────────────────────────┬─────────────────────────────────────────────────────────────────────┐
    │  Operation   │                                      Mechanics                                       │                             When to use                             │
    ├──────────────┼──────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────┤
    │ Direct AND   │ Position match (root+leaf+docID)                                                     │ When one operand's positions ⊇ the other's for matching elements    │
    ├──────────────┼──────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────┤
    │ MaskPosition │ Zero bits 47-32 (leaf), keep root+docID                                              │ Erase leaf differences to check same root+document                  │
    ├──────────────┼──────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────┤
    │ Strip        │ Zero bits 63-32, keep docID only                                                     │ Final result → docIDs                                               │
    ├──────────────┼──────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────┤
    │ ANDNOT       │ Raw position subtraction                                                             │ Negation (preserves per-element precision)                          │
    ├──────────────┼──────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────┤
    │ _idx loop    │ For each index N in _idx.{path}, AND both operands with _idx positions, check both   │ Verify both operands fall under the same element of an intermediate │
    │              │ non-empty                                                                            │  array                                                              │
    └──────────────┴──────────────────────────────────────────────────────────────────────────────────────┴─────────────────────────────────────────────────────────────────────┘

    NOTE: 
    - MaskPosition() and Strip() methods have to be added to the sroar library (final names yet to be decided)
    - "Direct AND" and "ANDNOT" are respectively sroar's And() and AndNot() methods

    When to use Direct AND

        Direct AND works when at least one operand's positions are a superset of the other's for matching elements. Three cases:

        ┌───────────────────────────────────────────┬────────────────────────────────────────┬─────────────────────────────────────────────────────────────────────────────────┐
        │                   Case                    │              Why it works              │                                     Example                                     │
        ├───────────────────────────────────────────┼────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────┤
        │ Same leaf element                         │ Identical positions                    │ addresses.city="Madrid" AND addresses.postcode="28001" → both {l2|d124}         │
        ├───────────────────────────────────────────┼────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────┤
        │ Ancestor → descendant                     │ Ancestor inherits descendant positions │ cars.tires.width=205 AND cars.tires.radiuses=17 → width {l7,l8} ⊇ radiuses {l7} │
        ├───────────────────────────────────────────┼────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────┤
        │ Parent-level scalar + any sibling subtree │ Scalar inherits ALL parent descendants │ cars.make="BMW" AND cars.colors="black" → make {l7..l10} ⊇ colors {l9}          │
        └───────────────────────────────────────────┴────────────────────────────────────────┴─────────────────────────────────────────────────────────────────────────────────┘

    When to use MaskPosition + AND

        When operands are in different subtrees and neither's positions contain the other's. MaskPosition erases leaf differences to check same root+docID.

        Sufficient when the lowest common ancestor (LCA) of both operands is the document root:

        addresses.city="Berlin" AND cars.make="BMW"
        → MaskPos({r1|l3..l4|d123}) AND MaskPos({r1|l7..l10|d123}) → {r1|d123} ✓

    When to use MaskPosition + _idx loop

        When the LCA is an intermediate array (not the root). MaskPosition alone is too coarse — it confirms same document but not same array element. The _idx loop verifies both
        operands fall under the same element of the intermediate parent.

        Which _idx to use: the intermediate array that is the LCA of both operands.

        ┌──────────────────────────────────────────────────────────────────────────┬────────────┬─────────────────┐
        │                                 Operands                                 │ LCA array  │  _idx to loop   │
        ├──────────────────────────────────────────────────────────────────────────┼────────────┼─────────────────┤
        │ cars.tires.width + cars.colors                                           │ cars       │ _idx.cars       │
        ├──────────────────────────────────────────────────────────────────────────┼────────────┼─────────────────┤
        │ cars.tires.radiuses + cars.accessories.type                              │ cars       │ _idx.cars       │
        ├──────────────────────────────────────────────────────────────────────────┼────────────┼─────────────────┤
        │ cars.tires.radiuses (value1) + cars.tires.radiuses (value2) on same tire │ cars.tires │ _idx.cars.tires │
        ├──────────────────────────────────────────────────────────────────────────┼────────────┼─────────────────┤
        │ addresses.numbers (value1) + addresses.numbers (value2) on same address  │ addresses  │ _idx.addresses  │
        └──────────────────────────────────────────────────────────────────────────┴────────────┴─────────────────┘

        Example — cars.tires.width=205 AND cars.colors="white" (LCA = cars):
        A=width=205 → {r1|l7..l8|d124}, B=colors="white" → {r1|l11|d124}
        MaskPos pre-filter → {r1|d124} → candidate d124
        _idx.cars loop:
        _idx.cars[0]={l7..l9}: A∩={l7..l8}✓, B∩=∅ ✗ (Audi has no colors)
        _idx.cars[1]={l10..l11}: A∩=∅ ✗
        → no same-car match → {} ✓ (205 is Audi, white is Kia)

    Decision flowchart

        Given filter conditions A and B:

        1. Is one operand an ancestor of the other, or a scalar at the parent level of the other? → Direct AND
        2. Are they properties of the same leaf element? → Direct AND
        3. Otherwise find the lowest common ancestor array:
        - LCA = document root → MaskPos(A) AND MaskPos(B)
        - LCA = intermediate array → MaskPos pre-filter + _idx.{LCA} loop

    Compound operations

        ┌──────────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────┐
        │         Pattern          │                                         Operation                                         │
        ├──────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
        │ ANY(prop = X)            │ filter + Strip (default)                                                                  │
        ├──────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
        │ ALL(prop = X)            │ failing = _exists.prop ANDNOT passing; failDocs = Strip(failing); allDocs ANDNOT failDocs │
        ├──────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
        │ NONE(prop = X)           │ matchDocs = Strip(match); allDocs ANDNOT matchDocs                                        │
        ├──────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
        │ Position arr[N]          │ filter AND _idx.arr[N+1]                                                                  │
        ├──────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
        │ ContainsAll [X,Y]        │ MaskPos(val=X) AND MaskPos(val=Y) — or _idx loop if array under intermediate              │
        ├──────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
        │ ContainsAny [X,Y]        │ Union(val=X, val=Y) + Strip                                                               │
        ├──────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
        │ IS NULL (doc-level)      │ allDocs ANDNOT Strip(_exists.prop)                                                        │
        ├──────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
        │ IS NULL (element-level)  │ _exists.parent ANDNOT _exists.prop                                                        │
        ├──────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
        │ Negation (doc-level)     │ allDocs ANDNOT Strip(match)                                                               │
        ├──────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
        │ Negation (element-level) │ _exists.prop ANDNOT match                                                                 │
        └──────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────┘
    
---
5. Complete Index

    Values Bucket (for filterable index)

        // key = hash8(path) + encoded_value → RoaringSet bitmap

        hash8("name") + "subdoc_123"                → {r1|l1..l10|d123}
        hash8("name") + "subdoc_124"                → {r1|l1..l11|d124}
        hash8("name") + "subdoc_125"                → {r1|l1..l9|d125}

        hash8("owner.firstname") + "Marsha"         → {r1|l1|d123, r1|l2|d123}
        hash8("owner.firstname") + "Justin"         → {r1|l1|d124}
        hash8("owner.firstname") + "Anna"           → {r1|l1|d125}

        hash8("owner.lastname") + "Mallow"          → {r1|l1|d123, r1|l2|d123}
        hash8("owner.lastname") + "Time"            → {r1|l1|d124}
        hash8("owner.lastname") + "Wanna"           → {r1|l1|d125}

        hash8("owner.nicknames") + "Marshmallow"    → {r1|l1|d123}
        hash8("owner.nicknames") + "M&M"            → {r1|l2|d123}
        hash8("owner.nicknames") + "watch"          → {r1|l1|d124}

        hash8("addresses.city") + "Berlin"          → {r1|l3|d123, r1|l4|d123}
        hash8("addresses.city") + "Madrid"          → {r1|l2|d124}
        hash8("addresses.city") + "London"          → {r1|l3|d124}
        hash8("addresses.city") + "Paris"           → {r1|l2|d125}

        hash8("addresses.postcode") + "10115"       → {r1|l3|d123, r1|l4|d123}
        hash8("addresses.postcode") + "28001"       → {r1|l2|d124}
        hash8("addresses.postcode") + "SW1"         → {r1|l3|d124}
        hash8("addresses.postcode") + "75001"       → {r1|l2|d125}

        hash8("addresses.numbers") + 123            → {r1|l3|d123}
        hash8("addresses.numbers") + 1123           → {r1|l4|d123}
        hash8("addresses.numbers") + 124            → {r1|l2|d124}
        hash8("addresses.numbers") + 125            → {r1|l2|d125}

        hash8("tags") + "german"                    → {r1|l5|d123, r1|l4|d124}
        hash8("tags") + "premium"                   → {r1|l6|d123}
        hash8("tags") + "japanese"                  → {r1|l5|d124}
        hash8("tags") + "sedan"                     → {r1|l6|d124}
        hash8("tags") + "electric"                  → {r1|l3|d125}

        hash8("cars.make") + "BMW"                  → {r1|l7..l10|d123}
        hash8("cars.make") + "Audi"                 → {r1|l7..l9|d124}
        hash8("cars.make") + "Kia"                  → {r1|l10|d124, r1|l11|d124}
        hash8("cars.make") + "Tesla"                → {r1|l4..l9|d125}

        hash8("cars.tires.width") + 225             → {r1|l7|d123, r1|l8|d123, r1|l9|d124}
        hash8("cars.tires.width") + 205             → {r1|l7|d124, r1|l8|d124}
        hash8("cars.tires.width") + 195             → {r1|l10|d124}
        hash8("cars.tires.width") + 245             → {r1|l4..l6|d125}

        hash8("cars.tires.radiuses") + 18           → {r1|l7|d123, r1|l8|d124, r1|l4|d125}
        hash8("cars.tires.radiuses") + 19           → {r1|l8|d123, r1|l5|d125}
        hash8("cars.tires.radiuses") + 17           → {r1|l7|d124}
        hash8("cars.tires.radiuses") + 20           → {r1|l6|d125}

        hash8("cars.colors") + "black"              → {r1|l9|d123}
        hash8("cars.colors") + "orange"             → {r1|l10|d123}
        hash8("cars.colors") + "white"              → {r1|l11|d124}
        hash8("cars.colors") + "yellow"             → {r1|l9|d125}

        hash8("cars.accessories.type") + "charger"  → {r1|l7|d125}
        hash8("cars.accessories.type") + "mats"     → {r1|l8|d125}

    Metadata Bucket

        // _idx entries (key = hash8("_idx." + path) + BE16(N)):

        hash8("_idx.owner") + 0                     → {r1|l1|d123, r1|l2|d123, r1|l1|d124, r1|l1|d125} // TODO entry needed?
        
        hash8("_idx.owner.nicknames") + 0           → {r1|l1|d123, r1|l1|d124}
        hash8("_idx.owner.nicknames") + 1           → {r1|l2|d123}
        
        hash8("_idx.addresses") + 0                 → {r1|l3|d123, r1|l4|d123, r1|l2|d124, r1|l2|d125}
        hash8("_idx.addresses") + 1                 → {r1|l3|d124}
        
        hash8("_idx.addresses.numbers") + 0         → {r1|l3|d123, r1|l2|d124, r1|l2|d125}
        hash8("_idx.addresses.numbers") + 1         → {r1|l4|d123}
        
        hash8("_idx.tags") + 0                      → {r1|l5|d123, r1|l4|d124, r1|l3|d125}
        hash8("_idx.tags") + 1                      → {r1|l6|d123, r1|l5|d124}
        hash8("_idx.tags") + 2                      → {r1|l6|d124}
        
        hash8("_idx.cars") + 0                      → {r1|l7..l10|d123, r1|l7..l9|d124, r1|l4..l9|d125}
        hash8("_idx.cars") + 1                      → {r1|l10|d124, r1|l11|d124}
        
        hash8("_idx.cars.tires") + 0                → {r1|l7|d123, r1|l8|d123, r1|l7|d124, r1|l8|d124, r1|l10|d124, r1|l4..l6|d125}
        hash8("_idx.cars.tires") + 1                → {r1|l9|d124}
        
        hash8("_idx.cars.tires.radiuses") + 0       → {r1|l7|d123, r1|l7|d124, r1|l4|d125}
        hash8("_idx.cars.tires.radiuses") + 1       → {r1|l8|d123, r1|l8|d124, r1|l5|d125}
        hash8("_idx.cars.tires.radiuses") + 2       → {r1|l6|d125}

        hash8("_idx.cars.colors") + 0               → {r1|l9|d123, r1|l11|d124, r1|l9|d125}
        hash8("_idx.cars.colors") + 1               → {r1|l10|d123}

        hash8("_idx.cars.accessories") + 0          → {r1|l7|d125}
        hash8("_idx.cars.accessories") + 1          → {r1|l8|d125}

        // _exists entries (key = hash8("_exists." + path)):

        hash8("_exists")                            → {r1|l1..l10|d123, r1|l1..l11|d124, r1|l1..l9|d125}
        hash8("_exists.name")                       → {r1|l1..l10|d123, r1|l1..l11|d124, r1|l1..l9|d125}
        hash8("_exists.owner")                      → {r1|l1|d123, r1|l2|d123, r1|l1|d124, r1|l1|d125}
        hash8("_exists.owner.firstname")            → {r1|l1|d123, r1|l2|d123, r1|l1|d124, r1|l1|d125}
        hash8("_exists.owner.lastname")             → {r1|l1|d123, r1|l2|d123, r1|l1|d124, r1|l1|d125}
        hash8("_exists.owner.nicknames")            → {r1|l1|d123, r1|l2|d123, r1|l1|d124}
        hash8("_exists.addresses")                  → {r1|l3|d123, r1|l4|d123, r1|l2|d124, r1|l3|d124, r1|l2|d125}
        hash8("_exists.addresses.city")             → {r1|l3|d123, r1|l4|d123, r1|l2|d124, r1|l3|d124, r1|l2|d125}
        hash8("_exists.addresses.postcode")         → {r1|l3|d123, r1|l4|d123, r1|l2|d124, r1|l3|d124, r1|l2|d125}
        hash8("_exists.addresses.numbers")          → {r1|l3|d123, r1|l4|d123, r1|l2|d124, r1|l2|d125}
        hash8("_exists.tags")                       → {r1|l5|d123, r1|l6|d123, r1|l4..l6|d124, r1|l3|d125}
        hash8("_exists.cars")                       → {r1|l7..l10|d123, r1|l7..l11|d124, r1|l4..l9|d125}
        hash8("_exists.cars.make")                  → {r1|l7..l10|d123, r1|l7..l11|d124, r1|l4..l9|d125}
        hash8("_exists.cars.tires")                 → {r1|l7|d123, r1|l8|d123, r1|l7..l10|d124, r1|l4..l6|d125}
        hash8("_exists.cars.tires.width")           → {r1|l7|d123, r1|l8|d123, r1|l7..l10|d124, r1|l4..l6|d125}
        hash8("_exists.cars.tires.radiuses")        → {r1|l7|d123, r1|l8|d123, r1|l7|d124, r1|l8|d124, r1|l4..l6|d125}
        hash8("_exists.cars.colors")                → {r1|l9|d123, r1|l10|d123, r1|l11|d124, r1|l9|d125}
        hash8("_exists.cars.accessories")           → {r1|l7|d125, r1|l8|d125}
        hash8("_exists.cars.accessories.type")      → {r1|l7|d125, r1|l8|d125}

        NOTE: London address (d124 l3) absent from _exists.addresses.numbers. Kia's tire (d124 l10) absent from _exists.cars.tires.radiuses (radiuses=[] empty). Anna's owner (d125 l1) absent from _exists.owner.nicknames.
    

---
6. Filtering Examples

    User Story 1: Scalar Arrays

    1.1 Exact match on scalar array values

        a) tags = "german"
        → {r1|l5|d123, r1|l4|d124} → Strip → {d123, d124}

        b) owner.nicknames = "M&M"
        → {r1|l2|d123} → Strip → {d123}

        c) addresses.numbers = 124
        → {r1|l2|d124} → Strip → {d124}

    1.2 Contains semantics

        a) tags ContainsAll ["german", "premium"]
        → MaskPos(tags="german") AND MaskPos(tags="premium") → {r1|d123}
        → Strip → {d123}

        b) cars.tires.radiuses ContainsAny [19, 20]
        → radiuses=19 ∪ radiuses=20 → {r1|l8|d123, r1|l5|d125, r1|l6|d125}
        → Strip → {d123, d125}

        c) tags ContainsAll ["german", "japanese"]
        → MaskPos(tags="german") AND MaskPos(tags="japanese") → {r1|d124}
        → Strip → {d124}

    1.3 Position within array

        a) tags[0] = "german" (→ _idx.tags BE16(1))
        → tags="german" AND _idx.tags[0] → {r1|l5|d123, r1|l4|d124}
        → Strip → {d123, d124}

        b) cars.tires.radiuses[2] = 20 (→ _idx.cars.tires.radiuses BE16(3))
        → radiuses=20 AND _idx.cars.tires.radiuses[2] → {r1|l6|d125}
        → Strip → {d125}

        c) owner.nicknames[1] = "M&M" (→ _idx.owner.nicknames BE16(2))
        → nicknames="M&M" AND _idx.owner.nicknames[1] → {r1|l2|d123}
        → Strip → {d123}

    1.4 Multiple conditions on same array field

        a) tags = "german" AND tags = "premium" (same array, different elements)
        → MaskPos(tags="german") AND MaskPos(tags="premium") → {r1|d123}
        → Strip → {d123}

        b) cars.tires.radiuses = 18 AND cars.tires.radiuses = 19 (same radiuses array)
        → A=radiuses=18 → {r1|l7|d123, r1|l8|d124, r1|l4|d125}
        → B=radiuses=19 → {r1|l8|d123, r1|l5|d125}
        → MaskPos(A) AND MaskPos(B) → {r1|d123, r1|d125}
        → Strip → {d123, d125}

        But wait — this says "same radiuses array" but MaskPos matches at doc level, not per-tire. Radiuses 18 and 19 could be on different tires! For same tire:
        → Need _idx.cars.tires loop (tires is intermediate parent of radiuses):
        - d123: _idx.cars.tires[0]={l7,l8}. A∩={l7} ✓, B∩={l8} ✓ → same tire MATCH
        - d125: _idx.cars.tires[0]={l4..l6}. A∩={l4} ✓, B∩={l5} ✓ → same tire MATCH
        → {d123, d125} ✓ (both have a single tire with radiuses 18 AND 19)

        c) addresses.numbers = 123 AND addresses.numbers = 1123
        → A={r1|l3|d123}, B={r1|l4|d123}
        → MaskPos(A) AND MaskPos(B) → {r1|d123}
        → Strip → {d123} (Berlin address has both numbers)

        For same-address verification: _idx.addresses loop:
        - d123: _idx.addresses[0]={l3,l4}. A∩={l3} ✓, B∩={l4} ✓ → same address MATCH ✓

    1.5 Presence check

        a) HAS(owner.nicknames) — owners with at least one nickname
        → _exists.owner.nicknames → Strip → {d123, d124} (Anna has none)

        b) HAS(cars.accessories)
        → _exists.cars.accessories → Strip → {d125}

        c) HAS(addresses.numbers) — docs where any address has numbers
        → _exists.addresses.numbers → Strip → {d123, d124, d125}
        (London address has no numbers, but Madrid address does → d124 matches)

    1.6 Negation

        a) tags != "german" (docs where NO tag is "german")
        → failDocs = Strip(tags="german") → {d123, d124}
        → allDocs ANDNOT failDocs → {d125}

        b) owner.nicknames NOT IN ["watch"]
        → failDocs = Strip(nicknames="watch") → {d124}
        → allDocs ANDNOT failDocs → {d123, d125}

        c) Per-element: cars.colors != "black" (color elements that are not black)
        → all = _exists.cars.colors, match = colors="black"
        → all ANDNOT match → {r1|l10|d123, r1|l11|d124, r1|l9|d125}
        (orange, white, yellow remain)

    1.7 Null state

        a) owner.nicknames IS NULL (docs where owner has no nicknames)
        → hasProp = Strip(_exists.owner.nicknames) → {d123, d124}
        → allDocs ANDNOT hasProp → {d125} (Anna has no nicknames)

        b) cars.colors IS NULL (docs where no car has colors)
        → hasProp = Strip(_exists.cars.colors) → {d123, d124, d125}
        → allDocs ANDNOT hasProp → {} (all have at least one car with colors)

        c) addresses.numbers IS NULL per address element (which addresses lack numbers?)
        → allAddr = _exists.addresses
        → hasNum = _exists.addresses.numbers
        → allAddr ANDNOT hasNum → {r1|l3|d124} (London address)
        → Strip → {d124}

    
    User Story 2: Nested Objects

    2.1 Exact match on scalar values in a path

        a) addresses.city = "Berlin"
        → {r1|l3|d123, r1|l4|d123} → Strip → {d123}

        b) owner.firstname = "Justin"
        → {r1|l1|d124} → Strip → {d124}

        c) cars.tires.width = 245
        → {r1|l4..l6|d125} → Strip → {d125}

    2.2 Multiple conditions on scalars in a path (same element)

        a) addresses.city = "Madrid" AND addresses.postcode = "28001" (same address)
        → A={r1|l2|d124}, B={r1|l2|d124}
        → Direct AND → {r1|l2|d124} → Strip → {d124} ✓

        b) owner.firstname = "Marsha" AND owner.lastname = "Mallow" (same owner)
        → A={r1|l1|d123, r1|l2|d123}, B={r1|l1|d123, r1|l2|d123}
        → Direct AND → {r1|l1|d123, r1|l2|d123} → Strip → {d123} ✓

        c) cars.tires.width = 205 AND cars.tires.radiuses = 17 (same tire, ancestor-descendant)
        → A=width=205→{r1|l7|d124, r1|l8|d124}, B=radiuses=17→{r1|l7|d124}
        → Direct AND → {r1|l7|d124} → Strip → {d124} ✓ (Audi's first tire)

    2.3 Multiple conditions on a document (cross-subtree)

        a) addresses.city = "Berlin" AND cars.make = "BMW" (sibling subtrees under root)
        → MaskPos({r1|l3|d123, r1|l4|d123}) AND MaskPos({r1|l7..l10|d123})
        → {r1|d123} → Strip → {d123}

        b) owner.firstname = "Justin" AND tags = "japanese" (doc-level owner + tags)
        → A=firstname="Justin"→{r1|l1|d124}, B=tags="japanese"→{r1|l5|d124}
        → MaskPos(A) AND MaskPos(B) → {r1|d124} → Strip → {d124}

        c) owner.firstname = "Anna" AND cars.accessories.type = "charger" (owner scalar + deep nested)
        → A=firstname="Anna"→{r1|l1|d125}, B=acc.type="charger"→{r1|l7|d125}
        → MaskPos(A) AND MaskPos(B) → {r1|d125} → Strip → {d125}

    2.4 Contains semantics (partial/complete document match)

        a) addresses contains {city:"Berlin", numbers:[123]} (same address, ancestor-descendant)
        → A=city="Berlin"→{r1|l3|d123, r1|l4|d123}, B=numbers=123→{r1|l3|d123}
        → Direct AND → {r1|l3|d123} → Strip → {d123} ✓

        b) cars contains {make:"BMW", colors:["black"]} (same car, cross-sibling)
        → A=make="BMW"→{r1|l7..l10|d123}, B=colors="black"→{r1|l9|d123}
        → Direct AND → {r1|l9|d123} → Strip → {d123} ✓
        (Works with Direct AND because make inherits all car descendants including colors positions)

        c) cars.tires contains {width:225, radiuses:[18]} (same tire, ancestor-descendant)
        → A=width=225→{r1|l7|d123, r1|l8|d123, r1|l9|d124}, B=radiuses=18→{r1|l7|d123, r1|l8|d124, r1|l4|d125}
        → Direct AND → {r1|l7|d123} → Strip → {d123} ✓
        (BMW's tire: width=225 has positions {l7,l8}, radius=18 at l7, AND gives l7)

    2.5 Negation

        a) addresses.city != "London" (docs where NO address has city London)
        → failDocs = Strip(city="London") → {d124}
        → allDocs ANDNOT failDocs → {d123, d125}

        b) cars.make NOT IN ["BMW", "Kia"]
        → failDocs = Strip(make="BMW" ∪ make="Kia") → {d123, d124}
        → allDocs ANDNOT failDocs → {d125}

        c) Per-element: cars.make != "Audi" (car elements that are NOT Audi)
        → all = _exists.cars.make, match = cars.make="Audi"
        → all ANDNOT match → {r1|l7..l10|d123, r1|l10|d124, r1|l11|d124, r1|l4..l9|d125}
        (BMW, Kia, Tesla remain) → Strip → {d123, d124, d125}

    
    User Story 3: Arrays of Objects (additional requirements)

    3.1 Filtering at any arbitrary level

        a) cars.tires.radiuses = 18 (3 levels deep)
        → {r1|l7|d123, r1|l8|d124, r1|l4|d125} → Strip → {d123, d124, d125}

        b) cars.tires.width = 205 AND cars.colors = "white" (cross-sibling under intermediate cars)
        → A=width=205→{r1|l7|d124, r1|l8|d124}, B=colors="white"→{r1|l11|d124}
        → MaskPos pre-filter → {r1|d124} → candidate d124
        → _idx.cars loop:
        - _idx.cars[0]={l7..l9}: A∩={l7,l8} ✓, B∩=∅ ✗ (Audi has no colors)
        - _idx.cars[1]={l10,l11}: A∩=∅ ✗
        → No car has both → {} ✓ (205 is Audi, white is Kia — different cars)

        c) cars.tires.width = 245 AND cars.accessories.type = "charger" (cross-sibling under intermediate)
        → A=width=245→{r1|l4..l6|d125}, B=acc="charger"→{r1|l7|d125}
        → MaskPos pre-filter → {r1|d125}
        → _idx.cars loop:
        - _idx.cars[0]={l4..l9}: A∩={l4..l6} ✓, B∩={l7} ✓ → same car MATCH
        → Strip → {d125}

    3.2 Any/All/None

        a) ALL(cars.tires.width > 200) — every tire must be wider than 200
        → passing = width>200 → {r1|l7|d123, r1|l8|d123, r1|l7|d124, r1|l8|d124, r1|l9|d124, r1|l4..l6|d125}
        → allTires = _exists.cars.tires.width → {r1|l7|d123, r1|l8|d123, r1|l7..l10|d124, r1|l4..l6|d125}
        → failing = allTires ANDNOT passing → {r1|l10|d124} (Kia's 195 tire)
        → failDocs = Strip → {d124}
        → allDocs ANDNOT failDocs → {d123, d125}

        b) NONE(cars.colors = "white") — no car has white color
        → matchDocs = Strip(colors="white") → {d124}
        → allDocs ANDNOT matchDocs → {d123, d125}

        c) ANY(cars.accessories.type = "charger")
        → Strip(acc="charger") → {d125}
    

---
7. Position Assignment (2)

    Reference documents (property of type []object)

        doc998: [{
            "name": "subdoc_123"
            "owner":{
                "firstname":"Marsha",
                "lastname":"Mallow",
                "nicknames":["Marshmallow", "M&M"]
            }
            "addresses":[{
                "city":"Berlin",
                "postcode":"10115",
                "numbers":[123, 1123]
            }],
            "tags":["german","premium"],
            "cars":[{
                "make":"BMW",
                "tires":[{
                "width":225
                "radiuses":[18,19]
                }],
                "colors":["black","orange"]
            }]
        }]
        doc999: [{
            "name": "subdoc_124"
            "owner":{
                "firstname":"Justin",
                "lastname":"Time",
                "nicknames":["watch"]
            }
            "addresses":[{
                "city":"Madrid",
                "postcode":"28001"
                "numbers":[124]
            },{
                "city":"London",
                "postcode":"SW1"
            }],
            "tags":["german","japanese","sedan"],
            "cars":[{
                "make":"Audi",
                "tires":[{
                    "width":205,
                    "radiuses":[17,18]
                },{
                    "width":225
                }]
            },{
                "make":"Kia",
                "tires":[{
                    "width":195
                    "radiuses":[]
                }],
                "colors":["white"]
            }]
        }, {
            "name": "subdoc_125"
            "owner":{
                "firstname":"Anna",
                "lastname":"Wanna"
            }
            "addresses":[{
                "city":"Paris",
                "postcode":"75001",
                "numbers":[125]
            }],
            "tags":["electric"],
            "cars":[{
                "make":"Tesla",
                "tires":[{
                    "width":245
                    "radiuses":[18,19,20]
                }],
                "accessories":[{
                    "type":"charger"
                },{
                    "type":"mats"
                }],
                "colors":["yellow"]
            }]
        }]


        ┌─────────────────────┬────────────┐
        │       Subdoc        │  Encoding  │
        ├─────────────────────┼────────────┤
        │ subdoc_123 (Marsha) │ r1|l*|d998 │
        ├─────────────────────┼────────────┤
        │ subdoc_124 (Justin) │ r1|l*|d999 │
        ├─────────────────────┼────────────┤
        │ subdoc_125 (Anna)   │ r2|l*|d999 │
        └─────────────────────┴────────────┘

        Justin and Anna share docID 999 but have different roots. Marsha has her own docID 998.

    Assignments:
  
    - doc998, root 1 (subdoc_123 / Marsha) — 10 leaves

    owner (intermediate) → {l1, l2}       ── r1|l*|d998
    ├─ firstname="Marsha" → {l1, l2}
    ├─ lastname="Mallow" → {l1, l2}
    ├─ nicknames[0]="Marshmallow" → l1
    └─ nicknames[1]="M&M" → l2
    addresses[0] (intermediate) → {l3, l4}
    ├─ city="Berlin" → {l3, l4}
    ├─ postcode="10115" → {l3, l4}
    ├─ numbers[0]=123 → l3
    └─ numbers[1]=1123 → l4
    tags[0]="german" → l5
    tags[1]="premium" → l6
    cars[0] (intermediate) → {l7..l10}
    ├─ make="BMW" → {l7..l10}
    ├─ tires[0] (intermediate) → {l7, l8}
    │  ├─ width=225 → {l7, l8}
    │  ├─ radiuses[0]=18 → l7
    │  └─ radiuses[1]=19 → l8
    ├─ colors[0]="black" → l9
    └─ colors[1]="orange" → l10
    name="subdoc_123" → {l1..l10}

    - doc999, root 1 (subdoc_124 / Justin) — 11 leaves

    owner (intermediate) → {l1}           ── r1|l*|d999
    ├─ firstname="Justin" → {l1}
    ├─ lastname="Time" → {l1}
    └─ nicknames[0]="watch" → l1
    addresses[0] (intermediate) → {l2}
    ├─ city="Madrid" → {l2}
    ├─ postcode="28001" → {l2}
    └─ numbers[0]=124 → l2
    addresses[1] (leaf, no numbers) → l3
    ├─ city="London" → {l3}
    └─ postcode="SW1" → {l3}
    tags[0]="german" → l4
    tags[1]="japanese" → l5
    tags[2]="sedan" → l6
    cars[0] (intermediate) → {l7..l9}
    ├─ make="Audi" → {l7..l9}
    ├─ tires[0] (intermediate) → {l7, l8}
    │  ├─ width=205 → {l7, l8}
    │  ├─ radiuses[0]=17 → l7
    │  └─ radiuses[1]=18 → l8
    └─ tires[1] (leaf, no radiuses) → l9
        └─ width=225 → {l9}
    cars[1] (intermediate) → {l10, l11}
    ├─ make="Kia" → {l10, l11}
    ├─ tires[0] (leaf, radiuses=[]) → l10
    │  └─ width=195 → {l10}
    └─ colors[0]="white" → l11
    name="subdoc_124" → {l1..l11}

    - doc999, root 2 (subdoc_125 / Anna) — 9 leaves

    owner (leaf, no nicknames) → l1       ── r2|l*|d999
    ├─ firstname="Anna" → {l1}
    └─ lastname="Wanna" → {l1}
    addresses[0] (intermediate) → {l2}
    ├─ city="Paris" → {l2}
    ├─ postcode="75001" → {l2}
    └─ numbers[0]=125 → l2
    tags[0]="electric" → l3
    cars[0] (intermediate) → {l4..l9}
    ├─ make="Tesla" → {l4..l9}
    ├─ tires[0] (intermediate) → {l4..l6}
    │  ├─ width=245 → {l4..l6}
    │  ├─ radiuses[0]=18 → l4
    │  ├─ radiuses[1]=19 → l5
    │  └─ radiuses[2]=20 → l6
    ├─ accessories[0] (leaf) → l7
    │  └─ type="charger" → {l7}
    ├─ accessories[1] (leaf) → l8
    │  └─ type="mats" → {l8}
    └─ colors[0]="yellow" → l9
    name="subdoc_125" → {l1..l9}

  
---
8. Complete Index (2)

    Values Bucket (for filterable index)

        // key = hash8(path) + encoded_value → RoaringSet bitmap

        hash8("name") + "subdoc_123"                → {r1|l1..l10|d998}
        hash8("name") + "subdoc_124"                → {r1|l1..l11|d999}
        hash8("name") + "subdoc_125"                → {r2|l1..l9|d999}

        hash8("owner.firstname") + "Marsha"         → {r1|l1|d998, r1|l2|d998}
        hash8("owner.firstname") + "Justin"         → {r1|l1|d999}
        hash8("owner.firstname") + "Anna"           → {r2|l1|d999}

        hash8("owner.lastname") + "Mallow"          → {r1|l1|d998, r1|l2|d998}
        hash8("owner.lastname") + "Time"            → {r1|l1|d999}
        hash8("owner.lastname") + "Wanna"           → {r2|l1|d999}

        hash8("owner.nicknames") + "Marshmallow"    → {r1|l1|d998}
        hash8("owner.nicknames") + "M&M"            → {r1|l2|d998}
        hash8("owner.nicknames") + "watch"          → {r1|l1|d999}

        hash8("addresses.city") + "Berlin"          → {r1|l3|d998, r1|l4|d998}
        hash8("addresses.city") + "Madrid"          → {r1|l2|d999}
        hash8("addresses.city") + "London"          → {r1|l3|d999}
        hash8("addresses.city") + "Paris"           → {r2|l2|d999}

        hash8("addresses.postcode") + "10115"       → {r1|l3|d998, r1|l4|d998}
        hash8("addresses.postcode") + "28001"       → {r1|l2|d999}
        hash8("addresses.postcode") + "SW1"         → {r1|l3|d999}
        hash8("addresses.postcode") + "75001"       → {r2|l2|d999}

        hash8("addresses.numbers") + 123            → {r1|l3|d998}
        hash8("addresses.numbers") + 1123           → {r1|l4|d998}
        hash8("addresses.numbers") + 124            → {r1|l2|d999}
        hash8("addresses.numbers") + 125            → {r2|l2|d999}

        hash8("tags") + "german"                    → {r1|l5|d998, r1|l4|d999}
        hash8("tags") + "premium"                   → {r1|l6|d998}
        hash8("tags") + "japanese"                  → {r1|l5|d999}
        hash8("tags") + "sedan"                     → {r1|l6|d999}
        hash8("tags") + "electric"                  → {r2|l3|d999}

        hash8("cars.make") + "BMW"                  → {r1|l7..l10|d998}
        hash8("cars.make") + "Audi"                 → {r1|l7..l9|d999}
        hash8("cars.make") + "Kia"                  → {r1|l10|d999, r1|l11|d999}
        hash8("cars.make") + "Tesla"                → {r2|l4..l9|d999}

        hash8("cars.tires.width") + 225             → {r1|l7|d998, r1|l8|d998, r1|l9|d999}
        hash8("cars.tires.width") + 205             → {r1|l7|d999, r1|l8|d999}
        hash8("cars.tires.width") + 195             → {r1|l10|d999}
        hash8("cars.tires.width") + 245             → {r2|l4..l6|d999}

        hash8("cars.tires.radiuses") + 18           → {r1|l7|d998, r1|l8|d999, r2|l4|d999}
        hash8("cars.tires.radiuses") + 19           → {r1|l8|d998, r2|l5|d999}
        hash8("cars.tires.radiuses") + 17           → {r1|l7|d999}
        hash8("cars.tires.radiuses") + 20           → {r2|l6|d999}

        hash8("cars.colors") + "black"              → {r1|l9|d998}
        hash8("cars.colors") + "orange"             → {r1|l10|d998}
        hash8("cars.colors") + "white"              → {r1|l11|d999}
        hash8("cars.colors") + "yellow"             → {r2|l9|d999}

        hash8("cars.accessories.type") + "charger"  → {r2|l7|d999}
        hash8("cars.accessories.type") + "mats"     → {r2|l8|d999}

    Metadata Bucket

        // _idx entries (key = hash8("_idx." + path) + BE16(N)):

        hash8("_idx.owner") + 0                     → {r1|l1|d998, r1|l2|d998, r1|l1|d999, r2|l1|d999}

        hash8("_idx.owner.nicknames") + 0           → {r1|l1|d998, r1|l1|d999}
        hash8("_idx.owner.nicknames") + 1           → {r1|l2|d998}

        hash8("_idx.addresses") + 0                 → {r1|l3|d998, r1|l4|d998, r1|l2|d999, r2|l2|d999}
        hash8("_idx.addresses") + 1                 → {r1|l3|d999}

        hash8("_idx.addresses.numbers") + 0         → {r1|l3|d998, r1|l2|d999, r2|l2|d999}
        hash8("_idx.addresses.numbers") + 1         → {r1|l4|d998}

        hash8("_idx.tags") + 0                      → {r1|l5|d998, r1|l4|d999, r2|l3|d999}
        hash8("_idx.tags") + 1                      → {r1|l6|d998, r1|l5|d999}
        hash8("_idx.tags") + 2                      → {r1|l6|d999}

        hash8("_idx.cars") + 0                      → {r1|l7..l10|d998, r1|l7..l9|d999, r2|l4..l9|d999}
        hash8("_idx.cars") + 1                      → {r1|l10|d999, r1|l11|d999}

        hash8("_idx.cars.tires") + 0                → {r1|l7|d998, r1|l8|d998, r1|l7|d999, r1|l8|d999, r1|l10|d999, r2|l4..l6|d999}
        hash8("_idx.cars.tires") + 1                → {r1|l9|d999}

        hash8("_idx.cars.tires.radiuses") + 0       → {r1|l7|d998, r1|l7|d999, r2|l4|d999}
        hash8("_idx.cars.tires.radiuses") + 1       → {r1|l8|d998, r1|l8|d999, r2|l5|d999}
        hash8("_idx.cars.tires.radiuses") + 2       → {r2|l6|d999}

        hash8("_idx.cars.colors") + 0               → {r1|l9|d998, r1|l11|d999, r2|l9|d999}
        hash8("_idx.cars.colors") + 1               → {r1|l10|d998}

        hash8("_idx.cars.accessories") + 0          → {r2|l7|d999}
        hash8("_idx.cars.accessories") + 1          → {r2|l8|d999}

        // _exists entries (key = hash8("_exists." + path)):

        hash8("_exists")                            → {r1|l1..l10|d998, r1|l1..l11|d999, r2|l1..l9|d999}
        hash8("_exists.name")                       → {r1|l1..l10|d998, r1|l1..l11|d999, r2|l1..l9|d999}
        hash8("_exists.owner")                      → {r1|l1|d998, r1|l2|d998, r1|l1|d999, r2|l1|d999}
        hash8("_exists.owner.firstname")            → {r1|l1|d998, r1|l2|d998, r1|l1|d999, r2|l1|d999}
        hash8("_exists.owner.lastname")             → {r1|l1|d998, r1|l2|d998, r1|l1|d999, r2|l1|d999}
        hash8("_exists.owner.nicknames")            → {r1|l1|d998, r1|l2|d998, r1|l1|d999}
        hash8("_exists.addresses")                  → {r1|l3|d998, r1|l4|d998, r1|l2|d999, r1|l3|d999, r2|l2|d999}
        hash8("_exists.addresses.city")             → {r1|l3|d998, r1|l4|d998, r1|l2|d999, r1|l3|d999, r2|l2|d999}
        hash8("_exists.addresses.postcode")         → {r1|l3|d998, r1|l4|d998, r1|l2|d999, r1|l3|d999, r2|l2|d999}
        hash8("_exists.addresses.numbers")          → {r1|l3|d998, r1|l4|d998, r1|l2|d999, r2|l2|d999}
        hash8("_exists.tags")                       → {r1|l5|d998, r1|l6|d998, r1|l4..l6|d999, r2|l3|d999}
        hash8("_exists.cars")                       → {r1|l7..l10|d998, r1|l7..l11|d999, r2|l4..l9|d999}
        hash8("_exists.cars.make")                  → {r1|l7..l10|d998, r1|l7..l11|d999, r2|l4..l9|d999}
        hash8("_exists.cars.tires")                 → {r1|l7|d998, r1|l8|d998, r1|l7..l10|d999, r2|l4..l6|d999}
        hash8("_exists.cars.tires.width")           → {r1|l7|d998, r1|l8|d998, r1|l7..l10|d999, r2|l4..l6|d999}
        hash8("_exists.cars.tires.radiuses")        → {r1|l7|d998, r1|l8|d998, r1|l7|d999, r1|l8|d999, r2|l4..l6|d999}
        hash8("_exists.cars.colors")                → {r1|l9|d998, r1|l10|d998, r1|l11|d999, r2|l9|d999}
        hash8("_exists.cars.accessories")           → {r2|l7|d999, r2|l8|d999}
        hash8("_exists.cars.accessories.type")      → {r2|l7|d999, r2|l8|d999}


---
9. Filtering Examples (2)

    Two levels of result: MaskPos → matching root elements; Strip → matching documents.
    allRoots = MaskPos(_exists) → {r1|d998, r1|d999, r2|d999}

    User Story 1: Scalar Arrays

    1.1 Exact match

        a) tags = "german"
        → {r1|l5|d998, r1|l4|d999} → MaskPos → {r1|d998, r1|d999} → Strip → {d998, d999}

        b) owner.nicknames = "M&M"
        → {r1|l2|d998} → MaskPos → {r1|d998} → Strip → {d998}

        c) addresses.numbers = 124
        → {r1|l2|d999} → MaskPos → {r1|d999} → Strip → {d999}

    1.2 Contains semantics

        a) tags ContainsAll ["german", "premium"]
        → MaskPos(="german") AND MaskPos(="premium")
        → {r1|d998, r1|d999} AND {r1|d998} → {r1|d998} → Strip → {d998}

        b) cars.tires.radiuses ContainsAny [19, 20]
        → =19 ∪ =20 → {r1|l8|d998, r2|l5|d999, r2|l6|d999}
        → MaskPos → {r1|d998, r2|d999} → Strip → {d998, d999}

        c) tags ContainsAll ["german", "japanese"]
        → MaskPos(="german") AND MaskPos(="japanese")
        → {r1|d998, r1|d999} AND {r1|d999} → {r1|d999} → Strip → {d999}

    1.3 Position within array

        a) tags[0] = "german"
        → tags="german" AND _idx.tags[0] → {r1|l5|d998, r1|l4|d999}
        → MaskPos → {r1|d998, r1|d999} → Strip → {d998, d999}

        b) cars.tires.radiuses[2] = 20
        → =20 AND _idx.cars.tires.radiuses[2] → {r2|l6|d999}
        → MaskPos → {r2|d999} → Strip → {d999}

        c) owner.nicknames[1] = "M&M"
        → ="M&M" AND _idx.owner.nicknames[1] → {r1|l2|d998}
        → MaskPos → {r1|d998} → Strip → {d998}

    1.4 Multiple conditions on same array

        a) tags = "german" AND tags = "premium"
        → MaskPos(="german") AND MaskPos(="premium") → {r1|d998} → Strip → {d998}

        b) cars.tires.radiuses = 18 AND radiuses = 19 (same tire — _idx.cars.tires loop)
        → A={r1|l7|d998, r1|l8|d999, r2|l4|d999}, B={r1|l8|d998, r2|l5|d999}
        → MaskPos(A) AND MaskPos(B) → {r1|d998, r2|d999}
        → _idx.cars.tires loop:
        - r1|d998: tires[0]={l7,l8}: A∩={l7}✓ B∩={l8}✓ → MATCH
        - r2|d999: tires[0]={l4..l6}: A∩={l4}✓ B∩={l5}✓ → MATCH
        → {r1|d998, r2|d999} → Strip → {d998, d999}

        c) addresses.numbers = 123 AND numbers = 1123 (same address — _idx.addresses loop)
        → A={r1|l3|d998}, B={r1|l4|d998}
        → MaskPos pre-filter → {r1|d998}
        → _idx.addresses[1]={r1|l3, r1|l4|d998}: A∩✓ B∩✓ → MATCH
        → {r1|d998} → Strip → {d998}

    1.5 Presence check

        a) HAS(owner.nicknames)
        → MaskPos(_exists.owner.nicknames) → {r1|d998, r1|d999} → Strip → {d998, d999}

        b) HAS(cars.accessories)
        → MaskPos(_exists.cars.accessories) → {r2|d999} → Strip → {d999}

        c) HAS(addresses.numbers)
        → MaskPos(_exists.addresses.numbers) → {r1|d998, r1|d999, r2|d999} → Strip → {d998, d999}

    1.6 Negation

        a) tags != "german" (root elements where NO tag is german)
        → failRoots = MaskPos(="german") → {r1|d998, r1|d999}
        → allRoots ANDNOT failRoots → {r2|d999} → Strip → {d999}

        b) owner.nicknames NOT IN ["watch"]
        → failRoots = MaskPos(="watch") → {r1|d999}
        → allRoots ANDNOT failRoots → {r1|d998, r2|d999} → Strip → {d998, d999}

        c) Per-element: cars.colors != "black"
        → _exists.cars.colors ANDNOT colors="black"
        → {r1|l10|d998, r1|l11|d999, r2|l9|d999} (orange, white, yellow)

    1.7 Null state

        a) owner.nicknames IS NULL
        → hasNick = MaskPos(_exists.owner.nicknames) → {r1|d998, r1|d999}
        → allRoots ANDNOT hasNick → {r2|d999} → Strip → {d999}

        b) cars.colors IS NULL (root elements where no car has colors)
        → hasColors = MaskPos(_exists.cars.colors) → {r1|d998, r1|d999, r2|d999}
        → allRoots ANDNOT hasColors → {}

        c) Per-element: which address elements lack numbers?
        → _exists.addresses ANDNOT _exists.addresses.numbers → {r1|l3|d999} (London)
        → MaskPos → {r1|d999} → Strip → {d999}

    
    User Story 2: Nested Objects

    2.1 Exact match on scalar in path

        a) addresses.city = "Berlin"
        → {r1|l3|d998, r1|l4|d998} → MaskPos → {r1|d998} → Strip → {d998}

        b) owner.firstname = "Justin"
        → {r1|l1|d999} → MaskPos → {r1|d999} → Strip → {d999}

        c) cars.tires.width = 245
        → {r2|l4..l6|d999} → MaskPos → {r2|d999} → Strip → {d999}

    2.2 Multiple conditions on same element

        a) addresses.city = "Madrid" AND addresses.postcode = "28001"
        → Direct AND → {r1|l2|d999} → MaskPos → {r1|d999} → Strip → {d999}

        b) owner.firstname = "Marsha" AND owner.lastname = "Mallow"
        → Direct AND → {r1|l1|d998, r1|l2|d998} → MaskPos → {r1|d998} → Strip → {d998}

        c) cars.tires.width = 205 AND cars.tires.radiuses = 17 (ancestor-descendant)
        → A={r1|l7|d999, r1|l8|d999}, B={r1|l7|d999}
        → Direct AND → {r1|l7|d999} → MaskPos → {r1|d999} → Strip → {d999}

    2.3 Cross-subtree

        a) addresses.city = "Berlin" AND cars.make = "BMW" (siblings under root)
        → MaskPos({r1|l3, r1|l4|d998}) AND MaskPos({r1|l7..l10|d998})
        → {r1|d998} AND {r1|d998} → {r1|d998} → Strip → {d998}

        b) owner.firstname = "Justin" AND tags = "japanese"
        → MaskPos({r1|l1|d999}) AND MaskPos({r1|l5|d999})
        → {r1|d999} → Strip → {d999}

        c) owner.firstname = "Anna" AND cars.accessories.type = "charger"
        → MaskPos({r2|l1|d999}) AND MaskPos({r2|l7|d999})
        → {r2|d999} → Strip → {d999}

    2.4 Contains semantics

        a) addresses contains {city:"Berlin", numbers:[123]}
        → A=city="Berlin"→{r1|l3, r1|l4|d998}, B=numbers=123→{r1|l3|d998}
        → Direct AND → {r1|l3|d998} → MaskPos → {r1|d998} → Strip → {d998}

        b) cars contains {make:"BMW", colors:["black"]}
        → A=make="BMW"→{r1|l7..l10|d998}, B=colors="black"→{r1|l9|d998}
        → Direct AND → {r1|l9|d998} → MaskPos → {r1|d998} → Strip → {d998}

        c) cars.tires contains {width:225, radiuses:[18]}
        → A=width=225→{r1|l7|d998, r1|l8|d998, r1|l9|d999}
        → B=radiuses=18→{r1|l7|d998, r1|l8|d999, r2|l4|d999}
        → Direct AND → {r1|l7|d998} → MaskPos → {r1|d998} → Strip → {d998}
        (BMW's tire: width=225 at {l7,l8}, radius=18 at l7, AND gives l7 ✓)

    2.5 Negation

        a) addresses.city != "London"
        → failRoots = MaskPos(="London") → {r1|d999}
        → allRoots ANDNOT failRoots → {r1|d998, r2|d999} → Strip → {d998, d999}

        b) cars.make NOT IN ["BMW", "Kia"]
        → failRoots = MaskPos(="BMW" ∪ ="Kia") → {r1|d998, r1|d999}
        → allRoots ANDNOT failRoots → {r2|d999} → Strip → {d999}

        c) Per-element: cars.make != "Audi"
        → _exists.cars.make ANDNOT make="Audi"
        → {r1|l7..l10|d998, r1|l10|d999, r1|l11|d999, r2|l4..l9|d999}
        (BMW, Kia, Tesla) → MaskPos → {r1|d998, r1|d999, r2|d999}

    
    User Story 3: Arrays of Objects

    3.1 Arbitrary level filtering

        a) cars.tires.radiuses = 18 (3 levels deep)
        → {r1|l7|d998, r1|l8|d999, r2|l4|d999}
        → MaskPos → {r1|d998, r1|d999, r2|d999} → Strip → {d998, d999}

        b) cars.tires.width = 205 AND cars.colors = "white" (LCA=cars)
        → A=width=205→{r1|l7|d999, r1|l8|d999}, B=colors="white"→{r1|l11|d999}
        → MaskPos pre-filter → {r1|d999}
        → _idx.cars loop for r1|d999:
        - cars[0]={l7..l9}: A∩={l7,l8}✓, B∩=∅ ✗ (Audi has no colors)
        - cars[1]={l10,l11}: A∩=∅ ✗
        → {} ✓ (205 is Audi, white is Kia — different cars)

        c) cars.tires.width = 245 AND cars.accessories.type = "charger" (LCA=cars)
        → A=width=245→{r2|l4..l6|d999}, B=acc="charger"→{r2|l7|d999}
        → MaskPos pre-filter → {r2|d999}
        → _idx.cars loop for r2|d999:
        - cars[0]={l4..l9}: A∩={l4..l6}✓, B∩={l7}✓ → MATCH
        → {r2|d999} → Strip → {d999}

    3.2 Any/All/None

        a) ALL(cars.tires.width > 200)
        → passing = {r1|l7|d998, r1|l8|d998, r1|l7|d999, r1|l8|d999, r1|l9|d999, r2|l4..l6|d999}
        → allTires = _exists.cars.tires.width
        → failing = allTires ANDNOT passing → {r1|l10|d999} (Kia's 195 tire)
        → failRoots = MaskPos → {r1|d999}
        → allRoots ANDNOT failRoots → {r1|d998, r2|d999} → Strip → {d998, d999}

        b) NONE(cars.colors = "white")
        → matchRoots = MaskPos(="white") → {r1|d999}
        → allRoots ANDNOT matchRoots → {r1|d998, r2|d999} → Strip → {d998, d999}

        c) ANY(cars.accessories.type = "charger")
        → MaskPos(="charger") → {r2|d999} → Strip → {d999}

    
    Verification: results are analogous across both configurations:

        ┌──────────────────────────────────┬───────────────┬────────────────────┐
        │              Query               │ Separate docs │  Two-doc object[]  │
        ├──────────────────────────────────┼───────────────┼────────────────────┤
        │ tags="german"                    │ {d123, d124}  │ {r1|d998, r1|d999} │
        ├──────────────────────────────────┼───────────────┼────────────────────┤
        │ ContainsAll [german,premium]     │ {d123}        │ {r1|d998}          │
        ├──────────────────────────────────┼───────────────┼────────────────────┤
        │ tags!="german"                   │ {d125}        │ {r2|d999}          │
        ├──────────────────────────────────┼───────────────┼────────────────────┤
        │ ALL(tires.width>200)             │ {d123, d125}  │ {r1|d998, r2|d999} │
        ├──────────────────────────────────┼───────────────┼────────────────────┤
        │ tires width=205 AND colors=white │ {}            │ {}                 │
        └──────────────────────────────────┴───────────────┴────────────────────┘

        Same operations, same logic, different encoding. Element isolation uses root_idx within a document and docID across documents.


---
10. _idx loop algorithm

    result = empty
    for N = 0, 1, 2, ...:
        carN  = _idx.cars[N]
        matchA = A AND carN
        matchB = B AND carN
        result = result OR (MaskPos(matchA) AND MaskPos(matchB))
        
    MaskPos(matchA) AND MaskPos(matchB) ensures both operands matched under car index N in the same root+docID.

    Verifying 3.1b — cars.tires.width = 205 AND cars.colors = "white":
        A = {r1|l7|d999, r1|l8|d999},  B = {r1|l11|d999}

        N=0: carN = _idx.cars[0] = {r1|l7..l10|d998, r1|l7..l9|d999, r2|l4..l9|d999}
            matchA = A AND carN = {r1|l7|d999, r1|l8|d999}  (d998 positions don't match — different docID)
            matchB = B AND carN = {}  (l11 not in cars[1])
            MaskPos({...}) AND MaskPos({}) = {}

        N=1: carN = _idx.cars[1] = {r1|l10|d999, r1|l11|d999}
            matchA = A AND carN = {}  (l7,l8 not in cars[2])
            matchB = B AND carN = {r1|l11|d999}
            MaskPos({}) AND MaskPos({...}) = {}

        result = {} ✓  (205 is Audi/cars[0], white is Kia/cars[1] — different cars)

    Verifying 3.1c — cars.tires.width = 245 AND cars.accessories.type = "charger":
        A = {r2|l4..l6|d999},  B = {r2|l7|d999}

        N=0: carN = _idx.cars[0] = {r1|l7..l10|d998, r1|l7..l9|d999, r2|l4..l9|d999}
            matchA = A AND carN = {r2|l4, r2|l5, r2|l6|d999}
            matchB = B AND carN = {r2|l7|d999}
            MaskPos(matchA) AND MaskPos(matchB) = {r2|d999} AND {r2|d999} = {r2|d999} ✓

        result = {r2|d999} ✓

    The pre-filter candidates = MaskPos(A) AND MaskPos(B) is an optimization — if empty, skip the loop entirely.


---
11. Claude's implementation plan

    Context

        Nested properties (object/object[] types) are fully stored and retrieved but cannot be filtered. The schema defines IndexFilterable on NestedProperty but it's unused.
        analyzeProps in inverted/objects.go routes nested types to extendPropertiesWithPrimitive which returns nil for unsupported types — effectively skipping them.

        The design uses 64-bit position encoding in RoaringSet bitmaps to enable:
        - Basic nested property filtering (exact match, range, contains, null, negation)
        - Cross-sibling correlation (conditions on different properties of the same array element)
        - ANY/ALL/NONE semantics on object arrays
        - Positional access within nested arrays

    Position Encoding

        64-bit value: (root_idx:16 << 48) | (leaf_idx:16 << 32) | (docID:32)

        - root_idx (bits 63-48): 1-based index into top-level object array. Always 1 for standalone objects and single object types.
        - leaf_idx (bits 47-32): 1-based contiguous counter per root, assigned depth-first to leaf array elements.
        - docID (bits 31-0): 32-bit internal document ID.

        Notation: r{root}|l{leaf}|d{docID}. RoaringSet stores these as uint64 values (sroar.Bitmap supports uint64 natively).

    Position Assignment Rules

        1. Document = implicit 1-element object[] with root=1
        2. Object = 1-element object[] (unified code path)
        3. Walk depth-first; process nested arrays recursively before parent
        4. Scalar array elements (text[], number[], etc.): each element gets next leaf_idx
        5. Object array elements: recursively collect descendant leaf positions first
        - Has descendants → intermediate node, positions = union of all descendant leaves
        - No descendants (no sub-arrays, empty sub-arrays, or all missing) → gets own leaf_idx
        6. Scalar properties at any level: inherit ALL leaf positions of their parent element

    Bitmap Operations

        - Direct AND: when one operand's positions are a superset of the other's (ancestor-descendant, same element, parent-level scalar + sibling array)
        - MaskPosition (zero bits 47-32, keep root+docID): for cross-sibling subtrees under same document root
        - Strip (zero bits 63-32, keep docID): final result extraction
        - _idx loop: for same-array correlation. For each index N: result |= MaskPos(A & _idx[N]) & MaskPos(B & _idx[N])

    Storage

        - Value buckets (per leaf path): property_{dotted.path} — StrategyRoaringSet, keys = analyzed values, values = position bitmaps
        - Metadata bucket (per top-level object property): nested_meta_{propName} — StrategyRoaringSet, stores:
        - _idx.{path}\x00{BE16(N)} → positions for array element N
        - _exists.{path} → positions where property exists
        - _exists → root-level all-positions (for allRoots)

    API Syntax

        Dot-notation in a single string: path: ["addresses.city"] (GraphQL/REST) or target: { property: "addresses.city" } (gRPC). No proto or schema changes needed.

    
    Phase 1: Filterable Index

    Step 1: Position Encoding Package

        New package: adapters/repos/db/inverted/nested/

        New file: nested/position.go

        func Encode(rootIdx, leafIdx uint16, docID uint32) uint64
        func DecodeRootIdx(pos uint64) uint16
        func DecodeLeafIdx(pos uint64) uint16
        func DecodeDocID(pos uint64) uint32
        func MaskPosition(pos uint64) uint64       // zero bits 47-32
        func StripBitmap(bm *sroar.Bitmap) *sroar.Bitmap   // extract docIDs into new bitmap
        func MaskPosBitmap(bm *sroar.Bitmap) *sroar.Bitmap // apply MaskPosition to all values

        New file: nested/position_test.go — encode/decode roundtrips, StripBitmap, MaskPosBitmap

    
    Step 2: Position Assignment Algorithm

        New file: nested/assign.go

        // PositionedValue: leaf value with its positions (docID=0, caller ORs in real docID)
        type PositionedValue struct {
            Path      string     // "addresses.city"
            Data      []byte     // analyzed value
            Positions []uint64   // positions with docID=0
        }

        // PositionedMeta: metadata entry with positions
        type PositionedMeta struct {
            BucketKey []byte     // key within metadata bucket
            Positions []uint64   // positions with docID=0
        }

        // AssignPositions walks nested property tree depth-first, assigns positions,
        // analyzes leaf values, and returns positioned values + metadata.
        // Positions have docID=0; caller ORs in real docID during write.
        func AssignPositions(
            prop *models.Property,
            value interface{},
            analyzer *Analyzer,
        ) ([]PositionedValue, []PositionedMeta, error)

        The function:
        1. Wraps value in single-element array if object type (unified path)
        2. Iterates array elements, assigning root_idx = 1,2,3...
        3. For each element, recursively walks nested schema depth-first
        4. For nested arrays: recursively assigns leaf positions to elements
        5. For scalar arrays: each element gets next leaf_idx, analyzed as Countable
        6. For scalar primitives: inherits parent element's positions, analyzed as Countable
        7. Emits _idx.{arrayPath} + BE16(N) for each array element
        8. Emits _exists.{path} for each present property
        9. Emits _exists for each root element

        Uses existing Analyzer.Text(), Analyzer.Int(), etc. for leaf value analysis.

        New file: nested/assign_test.go — tests with the three verified configurations:
        - 3 separate documents (root=1 each, different docIDs)
        - 1 document with 3-element object[] (root=1,2,3, same docID)
        - 2 documents with 1+2 element object[] (both mechanisms combined)

    
    Step 3: Index Flag Helpers

        File: adapters/repos/db/inverted/objects.go

        Add near existing HasFilterableIndex:

        func HasFilterableIndexNested(prop *models.NestedProperty) bool {
            if prop.IndexFilterable == nil { return true }
            return *prop.IndexFilterable
        }

        func HasAnyInvertedIndexNested(prop *models.NestedProperty) bool {
            return HasFilterableIndexNested(prop)
        }

    
    Step 4: Flatten Nested Schema to Leaf Paths

    File: adapters/repos/db/shard_init_properties.go

        type nestedLeafPath struct {
            Path   string                  // "addresses.city"
            Nested *models.NestedProperty
        }

        // flattenNestedPaths recursively walks NestedProperties and returns
        // all primitive leaf paths with their definitions.
        func flattenNestedPaths(prop *models.Property) []nestedLeafPath

        Also needed: flattenNestedArrayPaths to enumerate all intermediate array paths for metadata bucket _idx entries. These are paths to object[] or scalar array properties at any
        depth.

    
    Step 5: Bucket Creation

        File: adapters/repos/db/shard_init_properties.go

        Extend initPropertyBuckets. When property is object/object[]:

        if schema.IsNested(schema.DataType(prop.DataType[0])) {
            eg.Go(func() error {
                return s.createNestedPropertyBuckets(ctx, &propCopy, makeBucketOptions)
            })
        }

        New method createNestedPropertyBuckets:
        1. Call flattenNestedPaths(prop) → leaf paths
        2. For each leaf with HasFilterableIndexNested:
        - store.CreateOrLoadBucket(ctx, helpers.BucketFromPropNameLSM(leaf.Path), StrategyRoaringSet)
        3. Create metadata bucket:
        - store.CreateOrLoadBucket(ctx, "nested_meta_"+prop.Name, StrategyRoaringSet)
        4. For each leaf path (if IndexNullState enabled):
        - store.CreateOrLoadBucket(ctx, helpers.BucketFromPropNameNullLSM(leaf.Path), StrategyRoaringSet)

        File: adapters/repos/db/helpers/helpers.go

        func NestedMetaBucketFromPropNameLSM(propName string) string {
            return fmt.Sprintf("nested_meta_%s", propName)
        }

    
    Step 6: Write Path — Analysis

        File: adapters/repos/db/inverted/analyzer.go

        Add new output types:

        type NestedProperty struct {
            Name               string            // dot-notation path
            Items              []NestedCountable
            HasFilterableIndex bool
        }

        type NestedCountable struct {
            Data      []byte     // analyzed value
            Positions []uint64   // positions with docID=0
        }

        type NestedMetadata struct {
            BucketName string    // metadata bucket name
            Key        []byte    // key within bucket
            Positions  []uint64  // positions with docID=0
        }

        File: adapters/repos/db/inverted/objects.go

        In analyzeProps, add nested type handling after the existing ref/array/primitive checks:

        } else if dt, ok := schema.AsNested(prop.DataType); ok {
            nestedVals, nestedMeta, err := nested.AssignPositions(prop, input[key], a)
            if err != nil {
                return ..., err
            }
            // Convert PositionedValue → NestedProperty, PositionedMeta → NestedMetadata
            // Group by path, set HasFilterableIndex from nested schema
            ...
        }

        Return nested analysis alongside regular analysis. Options:
        - Option A: Add return values to Object(): ([]Property, []NestedProperty, []NestedMetadata, error)
        - Option B: Return a result struct: type AnalysisResult struct { Properties, NilProperties, NestedProps, NestedMeta }

        Option B is cleaner for extensibility. Propagate through AnalyzeObject in shard_write_inverted.go.

    
    Step 7: Write Path — LSM Write

        File: adapters/repos/db/shard_write_inverted_lsm.go

        New method:

        func (s *Shard) extendNestedInvertedIndicesLSM(
            nestedProps []inverted.NestedProperty,
            nestedMeta []inverted.NestedMetadata,
            docID uint64,
        ) error {
            for _, prop := range nestedProps {
                if !prop.HasFilterableIndex { continue }
                bucket := s.store.Bucket(helpers.BucketFromPropNameLSM(prop.Name))
                if bucket == nil { continue }
                for _, item := range prop.Items {
                    positions := orDocIDIntoPositions(item.Positions, uint32(docID))
                    bucket.RoaringSetAddList(item.Data, positions)
                }
            }
            for _, meta := range nestedMeta {
                bucket := s.store.Bucket(meta.BucketName)
                if bucket == nil { continue }
                positions := orDocIDIntoPositions(meta.Positions, uint32(docID))
                bucket.RoaringSetAddList(meta.Key, positions)
            }
            return nil
        }

        func orDocIDIntoPositions(partials []uint64, docID uint32) []uint64 {
            out := make([]uint64, len(partials))
            for i, p := range partials { out[i] = p | uint64(docID) }
            return out
        }

        Call from extendInvertedIndicesLSM (or directly from updateInvertedIndexLSM in shard_write_put.go).

    
    Step 8: Delete Path

        File: adapters/repos/db/shard_write_inverted_lsm_delete.go

        New method:

        func (s *Shard) deleteNestedInvertedIndicesLSM(
            nestedProps []inverted.NestedProperty,
            nestedMeta []inverted.NestedMetadata,
            docID uint64,
        ) error

        Same pattern as write but calls RoaringSetRemoveOne for each position. Works automatically because delete path re-analyzes the stored object via AnalyzeObject which now returns
        nested entries, then removes the exact positions that were written.

        File: adapters/repos/db/shard_write_put.go

        Update updateInvertedIndexLSM to handle nested delta. For simplicity in Phase 1: always delete all old nested entries and add all new ones (no smart delta for nested). The
        existing DeltaSkipSearchable doesn't apply to nested properties.

    
    Step 9: Filter Validation

        File: entities/filters/filters_validator.go

        Add nested path validation. When property name contains .:

        func (cw *clauseWrapper) validateNestedPath(class *models.Class, segments []string) error {
            // 1. Look up first segment as top-level property
            prop, err := schema.GetPropertyByName(class, segments[0])
            // 2. Verify it's object/object[]
            if !schema.IsNested(schema.DataType(prop.DataType[0])) { return error }
            // 3. Walk remaining segments through NestedProperties
            var current interface{ GetNestedProperties() []*models.NestedProperty } = &Property{prop}
            for _, seg := range segments[1:] {
                nested, err := schema.GetNestedPropertyByName(current, seg)
                // 4. If not last segment, verify it's object/object[]
                // 5. If last segment, verify it's primitive, validate value type match
            }
        }

        Integrate into existing validateClause flow: check for . in property name before regular property lookup.

    
    Step 10: Filter Execution — Searcher

        This is the most complex step. Two sub-phases:

    Step 10a: Independent resolution (each nested condition → docIDs independently)

        File: adapters/repos/db/inverted/searcher.go

        In extractPropValuePair, detect nested paths:

        if strings.Contains(propName, ".") {
            return s.extractNestedPropValuePair(ctx, filter, className, propName)
        }

        extractNestedPropValuePair:
        1. Split dotted path, walk schema to find leaf NestedProperty
        2. Create synthetic *models.Property from leaf (Name=dottedPath, DataType=leaf.DataType, IndexFilterable=leaf.IndexFilterable)
        3. Dispatch to existing extract methods (extractPrimitiveProp, extractTokenizableProp, etc.)
        4. These methods produce a propValuePair that reads from bucket property_{dottedPath}
        5. The bucket contains 64-bit positions; propValuePair.fetchDocIDs() returns a position bitmap
        6. Post-process: wrap in a custom resolver that calls nested.StripBitmap() to extract docIDs

        This gives "ANY element matches" semantics — a document matches if any nested element has the matching value. Sufficient for basic filtering (User Stories 1.*, 2.1, 2.2, 2.5).

    Step 10b: Position-aware resolution (cross-sibling correlation)

        New file: adapters/repos/db/inverted/nested/resolver.go

        For AND/OR nodes where children target the same nested array:

        // ResolveCorrelated handles filter clauses that need position-aware combination.
        // Detects common nested ancestors, applies appropriate bitmap operations,
        // strips to docIDs.
        func ResolveCorrelated(
            ctx context.Context,
            clauses []filters.Clause,
            operator filters.Operator,
            readPositions func(path string, value []byte) (*sroar.Bitmap, error),
            readMeta func(key []byte) (*sroar.Bitmap, error),
            class *models.Class,
        ) (*sroar.Bitmap, error)

        Algorithm:
        1. Parse all child paths to determine nesting structure
        2. Group children by lowest common ancestor (LCA)
        3. For each group:
        - LCA is document root → MaskPosBitmap(A) AND MaskPosBitmap(B) → Strip
        - LCA is intermediate array → _idx loop: for each N, result |= MaskPos(A & _idx[N]) & MaskPos(B & _idx[N]) → Strip
        - One is ancestor → Direct AND → Strip
        4. Combine group results with plain AND/OR on docIDs
        5. Combine with non-nested siblings via plain AND/OR

        Integration: In extractPropValuePairs (the plural version that handles boolean operands), detect when children include correlated nested paths and route to ResolveCorrelated.

        New file: nested/resolver_test.go — tests for all filtering examples from the design (14 requirements x 3 examples each)

    
    Step 11: API Layer

        File: adapters/handlers/grpc/v1/filters.go

        Extend extractDataTypeProperty:

        if strings.Contains(propName, ".") {
            return extractNestedDataType(authorizedGetClass, className, propName)
        }

        func extractNestedDataType(authorizedGetClass, className, dottedPath string) (schema.DataType, error) {
            segments := strings.Split(dottedPath, ".")
            class := authorizedGetClass(className)
            prop, _ := schema.GetPropertyByName(class, segments[0])
            // Walk through NestedProperties for remaining segments
            // Return leaf data type
        }

        File: adapters/handlers/graphql/local/common_filters/ — same dotted-path resolution for GraphQL

    
    Step 12: Null State for Nested Properties

        File: adapters/repos/db/shard_write_inverted.go

        In AnalyzeObject, extend null property tracking:

        When a top-level object/object[] property is missing (null):
        - Emit null entries for ALL leaf paths within its subtree
        - Use the per-leaf-path null buckets

        When a nested field is missing within a present object:
        - The AssignPositions function handles this by not emitting values for that path
        - The _exists metadata entries allow detecting missing nested fields via:
        _exists.{parent} ANDNOT _exists.{child} → positions where parent exists but child doesn't

    
    Phase 2: Rangeable Index

        Adds range query optimization for numeric/date nested properties using StrategyRoaringSetRange.

        Changes from Phase 1:

        1. Index flags: Add HasRangeableIndexNested(prop *models.NestedProperty) bool in objects.go
        2. Bucket creation (shard_init_properties.go): For numeric/date leaf paths with IndexRangeFilters, create property_{path}_rangeable (StrategyRoaringSetRange)
        3. Write path: Write positions to rangeable buckets. Note: RoaringSetRangeAdd(value, position) — position replaces docID
        4. Delete path: RoaringSetRangeRemove(value, position)
        5. Searcher: Range operators (>, <, >=, <=) read from rangeable bucket, return position bitmaps, feed into same resolution logic from Phase 1

        The position encoding and resolution logic from Phase 1 is fully reused. Only bucket creation and value write/delete need extension.

    
    Key Files

        ┌──────────────────────────────────────────────────────┬─────────────────────────────────────────────────────────────────────────────┐
        │                         File                         │                                   Change                                    │
        ├──────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
        │ adapters/repos/db/inverted/nested/position.go        │ NEW — 64-bit position encode/decode/MaskPos/Strip                           │
        ├──────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
        │ adapters/repos/db/inverted/nested/assign.go          │ NEW — depth-first position assignment algorithm                             │
        ├──────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
        │ adapters/repos/db/inverted/nested/resolver.go        │ NEW — position-aware filter resolution (Direct AND, MaskPos+AND, _idx loop) │
        ├──────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
        │ adapters/repos/db/inverted/objects.go                │ Has*IndexNested helpers, nested type handling in analyzeProps               │
        ├──────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
        │ adapters/repos/db/inverted/analyzer.go               │ NestedProperty, NestedCountable, NestedMetadata types                       │
        ├──────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
        │ adapters/repos/db/shard_init_properties.go           │ flattenNestedPaths, createNestedPropertyBuckets                             │
        ├──────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
        │ adapters/repos/db/helpers/helpers.go                 │ NestedMetaBucketFromPropNameLSM                                             │
        ├──────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
        │ adapters/repos/db/shard_write_inverted.go            │ AnalyzeObject returns nested analysis                                       │
        ├──────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
        │ adapters/repos/db/shard_write_inverted_lsm.go        │ extendNestedInvertedIndicesLSM                                              │
        ├──────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
        │ adapters/repos/db/shard_write_inverted_lsm_delete.go │ deleteNestedInvertedIndicesLSM                                              │
        ├──────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
        │ adapters/repos/db/shard_write_put.go                 │ Plumb nested analysis through update/delete paths                           │
        ├──────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
        │ adapters/repos/db/inverted/searcher.go               │ extractNestedPropValuePair, route to resolver                               │
        ├──────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
        │ entities/filters/filters_validator.go                │ validateNestedPath for dotted paths                                         │
        ├──────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
        │ adapters/handlers/grpc/v1/filters.go                 │ extractNestedDataType                                                       │
        └──────────────────────────────────────────────────────┴─────────────────────────────────────────────────────────────────────────────┘

    Verification

        1. Unit tests (in nested/ package):
        - Position encoding roundtrips
        - Position assignment for all three configurations (separate docs, single object[], multi-doc object[])
        - Edge cases: empty arrays, missing fields, deeply nested, elements with no descendants
        2. Integration tests (adapters/repos/db/...):
        - Write nested object + filter roundtrip
        - Update nested object + verify index consistency
        - Delete nested object + verify cleanup
        3. E2E tests (new package test/acceptance/nested_filters/):
        - Filter on nested primitive (addresses.city == "Berlin")
        - Cross-sibling correlation (addresses.city == "Berlin" AND addresses.number == 5 matches same address)
        - object[] ANY/ALL/NONE semantics
        - ContainsAll/ContainsAny on nested scalar arrays
        - Deeply nested paths (cars.tires.radiuses)
        - Null handling (missing parent, missing field, IS NULL filter)
        - Negation at document and element level
        - Mixed nested + non-nested filters in same query
        - gRPC and GraphQL filter paths