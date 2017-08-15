#!/usr/bin/python

import random
import sys

import shapely.geometry


R = random.Random(0)


def r():
    return float(R.randint(-1000000, 1000000)) / 1000000


def randomCoord():
    return (r(), r())


def randomCoords(n):
    return [(r(), r()) for i in xrange(n)]


def goifyNestedFloat64Array(a):
    if isinstance(a[0], float):
        return '{' + ', '.join(repr(x) for x in a) + '}'
    else:
        return '{' + ', '.join(goifyNestedFloat64Array(x) for x in a) + '}'


def goifyGeometry(g):
    if isinstance(g, shapely.geometry.Point):
        return 'geom.NewPoint(geom.XY).MustSetCoords(geom.Coord%s)' % (goifyNestedFloat64Array([g.x, g.y]),)
    if isinstance(g, shapely.geometry.LineString):
        return 'geom.NewLineString(geom.XY).MustSetCoords([]geom.Coord%s)' % (goifyNestedFloat64Array(g.coords),)
    if isinstance(g, shapely.geometry.Polygon):
        coords = [g.exterior.coords] + [i.coords for i in g.interiors]
        return 'geom.NewPolygon(geom.XY).MustSetCoords([][]geom.Coord%s)' % (goifyNestedFloat64Array(coords),)
    if isinstance(g, shapely.geometry.MultiPoint):
        coords = [point.coords[0] for point in g.geoms]
        return 'geom.NewMultiPoint(geom.XY).MustSetCoords([]geom.Coord%s)' % (goifyNestedFloat64Array(coords),)
    if isinstance(g, shapely.geometry.MultiLineString):
        coords = [linestring.coords for linestring in g.geoms]
        return 'geom.NewMultiLineString(geom.XY).MustSetCoords([][]geom.Coord%s)' % (goifyNestedFloat64Array(coords),)
    if isinstance(g, shapely.geometry.MultiPolygon):
        coords = [[polygon.exterior.coords] + [i.coords for i in polygon.interiors] for polygon in g.geoms]
        return 'geom.NewMultiPolygon(geom.XY).MustSetCoords([][][]geom.Coord%s)' % (goifyNestedFloat64Array(coords),)
    if isinstance(g, shapely.geometry.GeometryCollection):
        return 'geom.NewGeometryCollection().MustPush(' + ', '.join(goifyGeometry(g) for g in g.geoms) + ')'
    raise 'Unknown type'


def randomPoint(coord=None):
    if coord is None:
        coord = randomCoord()
    return shapely.geometry.Point(coord)


def randomLineString(coords=None):
    if coords is None:
        coords = randomCoords(R.randint(2, 8))
    return shapely.geometry.LineString(coords)


def randomPolygon(rings=None):
    if rings is None:
        rings = [randomCoords(R.randint(3, 8))] + [randomCoords(R.randint(3, 8)) for i in xrange(R.randint(0, 4))]
    return shapely.geometry.Polygon(rings[0], rings[1:])


def randomMultiPoint():
    return shapely.geometry.MultiPoint([randomPoint() for i in xrange(R.randint(1, 8))])


def randomMultiLineString():
    return shapely.geometry.MultiLineString([randomLineString() for i in xrange(R.randint(1, 8))])


def randomMultiPolygon():
    return shapely.geometry.MultiPolygon([randomPolygon() for i in xrange(R.randint(1, 8))])


def randomSimpleGeometry():
    return R.choice([randomPoint, randomLineString, randomPolygon, randomMultiPoint, randomMultiLineString, randomMultiPolygon])()


def randomGeometryCollection():
    return shapely.geometry.GeometryCollection([randomSimpleGeometry() for i in xrange(R.randint(1, 8))])


def main(argv):
    f = open('random.go', 'w')
    # FIXME add GeoJSON support
    print >>f, 'package testdata'
    print >>f
    print >>f, '//go:generate python generate-random.py'
    print >>f
    print >>f, 'import ('
    print >>f, '\t"github.com/twpayne/go-geom"'
    print >>f, ')'
    print >>f
    print >>f, '// Random is a collection of randomly-generated test data.'
    print >>f, 'var Random = []struct {'
    print >>f, '\tG   geom.T'
    print >>f, '\tHex string'
    print >>f, '\tWKB []byte'
    print >>f, '\tWKT string'
    print >>f, '}{'
    for constructor in (
            randomPoint,
            randomLineString,
            randomPolygon,
            randomMultiPoint,
            randomMultiLineString,
            randomMultiPolygon,
            randomGeometryCollection,
            ):
        for i in xrange(8):
            g = constructor()
            print >>f, '\t{'
            print >>f, '\t\t%s,' % (goifyGeometry(g),)
            print >>f, '\t\t"%s",' % (g.wkb.encode('hex'),)
            print >>f, '\t\t[]byte("%s"),' % (''.join('\\x%02X' % ord(c) for c in g.wkb),)
            print >>f, '\t\t"%s",' % (g.wkt,)
            print >>f, '\t},'
    print >>f, '}'
    f.close()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
