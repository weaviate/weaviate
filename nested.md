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

    RoaringSet mapping: high 32 bits (root|leaf) = container key; low 32 bits (docID) = value within container. Operations on container keys are O(containers).

    Why positions, not just docIDs? Positions encode element identity within arrays, enabling:
    - Same-element filtering (city=X AND postcode=Y on the same address)
    - Cross-sibling correlation (same parent element)
    - Positional access (addresses[0])
    - ALL/NONE operators & per-element negation

    NOTE: 
        32 bits to store docID limits max docID in the bitmap to 4,294,967,296. If number of supported roots and leaves per root would be decreased to 16,384 (2^14), there will be 36 bits left for docID bringing max supported docID to 68,719,476,736.
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

    Reference documents (property of type object):    

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

    ┌──────────────┬──────────────────────────────────────────────┬───────────────────────────────────────────────────────────┐
    │    Operation     │                                    Mechanics                                     │                                                When to use                                                │
    ├──────────────┼──────────────────────────────────────────────┼───────────────────────────────────────────────────────────┤
    │ Direct AND     │ Positions match exactly (root+leaf+docID)        │ Same-element; ancestor-descendant; doc-level scalar + any │
    ├──────────────┼──────────────────────────────────────────────┼───────────────────────────────────────────────────────────┤
    │ MaskPosition │ Zeros bits 47-32 (leaf), keeps root+docID        │ Cross-sibling subtrees under same level                                     │
    ├──────────────┼──────────────────────────────────────────────┼───────────────────────────────────────────────────────────┤
    │ Strip                │ Zeros bits 63-32, keeps docID only                     │ Final result extraction                                                                     │
    ├──────────────┼──────────────────────────────────────────────┼───────────────────────────────────────────────────────────┤
    │ ANDNOT             │ Raw position subtraction                                         │ Negation (preserves per-element precision)                                │
    ├──────────────┼──────────────────────────────────────────────┼───────────────────────────────────────────────────────────┤
    │ _idx loop        │ Iterate element positions via _idx per index │ Cross-sibling under intermediate array                                        │
    └──────────────┴──────────────────────────────────────────────┴───────────────────────────────────────────────────────────┘

    NOTE: 
        - MaskPosition() and Strip() methods have to be added to sroar library
        - "Direct AND" and "ANDNOT" are respectively sroar's And() and AndNot()
        - "_idx loop" process of traversing through _idx entries to verify whether filtered values belong to the same element of nested (intermediate, not root) array.
    

    Decision table:

    ┌──────────────────────────────────────┬────────────────────────────────────────────────────────────────┐
    │                         Relationship                         │                                                     Operation                                                        │
    ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
    │ Same property, same element                    │ Direct AND                                                                                                         │
    ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
    │ Ancestor → descendant (same subtree) │ Direct AND                                                                                                         │
    ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
    │ Doc-level scalar + any nested                │ Direct AND (full propagation)                                                                    │
    ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
    │ Sibling subtrees under root                    │ MaskPosition + AND                                                                                         │
    ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
    │ Sibling arrays under intermediate        │ MaskPosition pre-filter + _idx loop                                                        │
    ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
    │ Negation                                                         │ ANDNOT on raw positions                                                                                │
    ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
    │ ANY                                                                    │ Default — filter + Strip                                                                             │
    ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
    │ ALL                                                                    │ allElements ANDNOT passing → failDocs; allDocs ANDNOT failDocs │
    ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
    │ NONE                                                                 │ Strip(matching) → matchDocs; allDocs ANDNOT matchDocs                    │
    ├──────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
    │ Position access arr[N]                             │ filter AND _idx[N]                                                                                         │
    └──────────────────────────────────────┴────────────────────────────────────────────────────────────────┘


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

    ---
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

    ---
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
    

