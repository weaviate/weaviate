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

/*
# LSMKV (= Log-structured Merge-Tree Key-Value Store)

This package contains Weaviate's custom LSM store. While modeled after the
usecases that are required for Weaviate to be fast, reliable, and scalable, it
is technically completely independent. You could build your own database on top
of this key-value store.

Covering the architecture of [LSM Stores] in general goes beyond the scope of
this documentation. Therefore things that are specific to this implementation
are highlighted.

# Strategies

To understand the different type of buckets in this store, you need to
familiarize yourself with the following strategies. A strategy defines a
different usecase for a [Bucket].

  - "Replace"

    Replace resembles the classical key-value store. Each key has exactly one
    value. A subsequent PUT on an an existing key, replaces the value (hence
    the name "replace"). Once replaced a former value can no longer be
    retrieved, and will eventually be removed in compactions.

  - "Set" (aka "SetCollection")

    A set behaves like an unordered collection of independent values. In other
    words a single key has multiple values. For example, for key "foo", you
    could have values "bar1", "bar2", "bazzinga". A bucket of this type is
    optimized for cheap writes to add new set additions. For example adding
    another set element has a fixed cost independent of the number of the
    existing set length. This makes it very well suited for building an
    inverted index.

    Retrieving a Set has a slight cost to it if a set is spread across multiple
    segments. This cost will eventually reduce as more and more compactions
    happen. In the ideal case (fully compacted DB), retrieving a Set requires
    just a single disk read.

  - "Map" (aka "MapCollection")

    Maps are similar to Sets in the sense that for a single key there are
    multiple values. However, each value is in itself a key-value pair. This
    makes this type very similar to a dict or hashmap type. For example for
    key "foo", you could have value pairs: "bar":17, "baz":19.

    This makes a map a great use case for an inverted index that needs to store
    additional info beyond just the docid-pointer, such as in the case of a
    BM25 index where the term frequency needs to be stored.

    The same performance-considerations as for sets apply.

# Navigate around these docs

Good entrypoints to learn more about how this package works include [Store]
with [New] and [Store.CreateOrLoadBucket], as well as [Bucket] with
[Bucket.Get], [Bucket.GetBySecondary], [Bucket.Put], etc.

Each strategy also supports cursor types: [CursorReplace] can be created using [Bucket.Cursor], [CursorSet] can be created with [Bucket.SetCursor] , and [CursorMap] can be created with [Bucket.MapCursor].

[LSM Stores]: https://en.wikipedia.org/wiki/Log-structured_merge-tree
*/
package lsmkv
