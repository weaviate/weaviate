# Overview

This is a library for manipulating geometric shapes. Unlike many geometry
libraries, S2 is primarily designed to work with _spherical geometry_, i.e.,
shapes drawn on a sphere rather than on a planar 2D map. (In fact, the name S2
is derived from the mathematical notation for the unit sphere.) This makes it
especially suitable for working with geographic data.

The library consists of:

*   Basic representations of angles, intervals, latitude-longitude points, unit
    3D vectors, and conversions among them.

*   Various shapes over the unit sphere, such as spherical caps ("discs"),
    latitude-longitude rectangles, polylines, and polygons. These are
    collectively known as "regions".

*   Support for spatial indexing of collections of geometry, and algorithms for
    testing containment, finding nearby objects, finding intersections, etc.

*   A hierarchical decomposition of the sphere into regions called "cells". The
    hierarchy starts with the six faces of a projected cube and recursively
    subdivides them in a quadtree-like fashion.

*   The ability to approximate arbitrary regions as a collection of cells. This
    is useful for building inverted indexes that allow queries over arbitrarily
    shaped regions.

The implementations attempt to be precise both in terms of mathematical
definitions (e.g. whether regions include their boundaries, representations of
empty and full regions) and numerical accuracy (e.g. avoiding cancellation
error).

Note that the intent of this library is to represent geometry as a mathematical
abstraction. For example, although the unit sphere is obviously a useful
approximation for the Earth's surface, functions that are specifically related
to geography are not part of the core library (e.g. easting/northing
conversions, ellipsoid approximations, geodetic vs. geocentric coordinates,
etc).

See http://godoc.org/github.com/golang/geo for specific package documentation.

For an analogous library in C++, see
https://code.google.com/archive/p/s2-geometry-library/, and in Java, see
https://github.com/google/s2-geometry-library-java

# Status of the Go Library

This library is principally a port of [the C++ S2
library](https://code.google.com/archive/p/s2-geometry-library), adapting to Go
idioms where it makes sense. We detail the progress of this port below relative
to that C++ library.

*Note*: The C++ code on `code.google.com` is an out-of-date snapshot of the
Google-internal version, and isn't the basis of this Go library. When the C++
library is re-released, we will update this document.

## [ℝ¹](https://godoc.org/github.com/golang/geo/r1) - One-dimensional Cartesian coordinates

Full parity with C++.

## [ℝ²](https://godoc.org/github.com/golang/geo/r2) - Two-dimensional Cartesian coordinates

Full parity with C++.

## [ℝ³](https://godoc.org/github.com/golang/geo/r3) - Three-dimensional Cartesian coordinates

Full parity with C++.

## [S¹](https://godoc.org/github.com/golang/geo/s1) - Circular Geometry

**Complete**

*   ChordAngle

**Mostly complete**

*   Angle - Missing Arithmetic methods, Trigonometric methods, Conversion
    to/from s2.Point, s2.LatLng, convenience methods from E5/E6/E7
*   Interval - Missing ClampPoint, Complement, ComplementCenter,
    HaussdorfDistance

## [S²](https://godoc.org/github.com/golang/geo/s2) - Spherical Geometry

Approximately ~40% complete.

**Complete** These files have full parity with the C++ implementation.

*   Cap
*   Cell
*   CellID
*   LatLng
*   matrix3x3
*   Metric
*   PaddedCell
*   Point
*   Region
*   s2edge_clipping
*   s2edge_crosser
*   s2edge_crossings
*   s2rect_bounder
*   s2stuv.go (s2coords.h in C++) - This file is a collection of helper and
    conversion methods to and from ST-space, UV-space, and XYZ-space.
*   s2wedge_relations

**Mostly Complete** Files that have almost all of the features of the original
C++ code, and are reasonably complete enough to use in live code. Up to date
listing of the incomplete methods are documented at the end of each file.

*   CellUnion - Missing Union, Intersection, etc.
*   Loop - Loop is mostly complete now. Missing Projection, Distance,
    Contains, Intersects, Union, etc.
*   Polyline - Missing Projection, Intersects, Interpolate, etc.
*   Rect (AKA s2latlngrect in C++) - Missing Centroid, Distance,
    InteriorContains.
*   RegionCoverer - Missing FloodFill and SimpleCovering.
*   s2_test.go (AKA s2testing and s2textformat in C++) - Missing
    ConcentricLoopsPolygon and Fractal test shape generation. This file is a
    collection of testing helper methods.
*   s2edge_distances - Missing Intersection, ClosestPair.
*   ShapeIndex - Missing ShapeContainsPoint and FindContainingShapes

**In Progress** Files that have some work done, but are probably not complete
enough for general use in production code.

*   Polygon - Polygon is at the partial skeleton phase, the fields all exist,
    and some basic methods are implemented, but it's missing almost everything.
    Init with multiple loops, Area, Centroid, Distance, Projection,
    Intersection, Union, Contains, Normalized, etc.
*   PolylineSimplifier - Initial work has begun on this.
*   s2predicates.go - This file is a collection of helper methods used by other
    parts of the library.

**Not Started Yet.** These files (and their associated unit tests) have
dependencies on most of the In Progress files before they can begin to be
started.

*   BoundaryOperation
*   Builder - This is a robust tool for creating the various Shape types from
    collection of simpler S2 types.
*   BuilderGraph
*   BuilderLayers
*   BuilderSnapFunctions
*   ClosestEdgeQuery
*   ClosestPointQuery
*   ConvexHullQuery
*   CrossingEdgeQuery
*   EdgeTesselator
*   PointCompression
*   PointIndex
*   PolygonBuilder
*   RegionIntersection
*   RegionUnion
*   Projections
*   ShapeUtil - Most of this will end up in s2_test.
*   lexicon
*   priorityqueuesequence

### Encode/Decode

Encoding of S2 Go types is committed and is interoperable with C++ and Java.
Decoding for Loops and Rects is now completed. The remaining types will be
worked on in the future.
