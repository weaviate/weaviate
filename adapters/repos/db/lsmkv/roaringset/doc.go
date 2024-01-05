//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// The "roaringset" package contains all the LSM business logic that is unique
// to the "RoaringSet" strategy
//
// This package alone does not contain an entire LSM store. It's intended to be
// used as part of the [github.com/weaviate/weaviate/adapters/repos/db/lsmkv] package.
//
// # Motivation
//
// What makes the RoaringSet strategy unique is that it's essentially a fully
// persistent Roaring Bitmap that can be built up and updated incrementally
// (without write amplification) while being extremely fast to query.
//
// Without this specific strategy, it would not be efficient to use roaring
// bitmaps in an LSM store. For example:
//
//   - Lucene uses posting lists in the inverted index on disk and supports
//     converting them to a Roaring Bitmap at query time. This resulting bitmap
//     can then be cached. However, the cost to initially convert a posting list
//     to a roaring bitmap is quite huge. In our own tests, inserting 90M out of
//     100M possible ids into a [github.com/weaviate/sroar.Bitmap] takes about
//     3.5s.
//
//   - You could store a regular roaring bitmap, such as
//     [github.com/weaviate/sroar.Bitmap] in a regular LSM store, such as
//     RocksDB. This would fix the retrieval issue and you should be able to
//     retrieve and initialize a bitmap containing 90M objects in a few
//     milliseconds. However, the cost to incrementally update this bitmap would
//     be extreme. You would have to use a read-modify-write pattern which would
//     lead to huge write-amplification on large setups. A 90M roaring bitmap
//     is about 10.5MB, so to add a single entry (which would take up anywhere
//     from 1 bit to 2 bytes), you would have to read 10.5MB and write 10.5MB
//     again. That's not feasible except for bulk-loading. In Weaviate we cannot
//     always assume bulk loading, as user behavior and insert orders are
//     generally unpredictable.
//
// We solve this issue by making the LSM store roaring-bitmap-native. This way,
// we can keep the benefits of an LSM store (very fast writes) with the
// benefits of a serialized roaring bitmap (very fast reads/initializations).
//
// Essentially this means the RoaringSet strategy behaves like a fully
// persistent (and durable) Roaring Bitmap. See the next section to learn how
// it works under the hood.
//
// # Internals
//
// The public-facing methods make use of [github.com/weaviate/sroar.Bitmap].
// This serialized bitmap already fulfills many of the criteria needed in
// Weaviate. It can be initialized at almost no cost (sharing memory) or very
// little cost (copying some memory). Furthermore, its set, remove, and
// intersection methods work well for the inverted index use cases in Weaviate.
//
// So, the novel part in the lsmkv.RoaringSet strategy does not sit in the
// roaring bitmap itself, but rather in the way it's persisted. It uses the
// standard principles of an LSM store where each new write is first cached in
// a memtable (and of course written into a Write-Ahead-Log to make it
// durable). The memtable is flushed into a disk segment when specific criteria
// are met (memtable size, WAL size, idle time, time since last flush, etc.).
//
// This means that each layer (represented by [BitmapLayer]) only contains the
// deltas that were written in a specific time interval. When reading, all
// layers must be combined into a single bitmap (see [BitmapLayers.Flatten]).
//
// Over time segments can be combined into fewer, larger segments using an LSM
// Compaction process. The logic for that can be found in [BitmapLayers.Merge].
//
// To make sure access is efficient the entire RoaringSet strategy is built to
// avoid encoding/decoding steps. Instead we internally store data as simple
// byte slices. For example, see [SegmentNode]. You can access bitmaps without
// any meaningful allocations using [SegmentNode.Additions] and
// [SegmentNode.Deletions]. If you plan to hold on to the bitmap for a time
// window that is longer than holding a lock that prevents a compaction, you
// need to copy data (e.g. using [SegmentNode.AdditionsWithCopy]). Even with
// such a copy, reading a 90M-ids bitmap takes only single-digit milliseconds.
package roaringset
