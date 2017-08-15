package wkt

import (
	"bytes"
	"strconv"

	"github.com/twpayne/go-geom"
)

// Marshal marshals an arbitrary geometry.
func Marshal(g geom.T) (string, error) {
	b := &bytes.Buffer{}
	if err := write(b, g); err != nil {
		return "", err
	}
	return b.String(), nil
}

func write(b *bytes.Buffer, g geom.T) error {
	typeString := ""
	switch g := g.(type) {
	case *geom.Point:
		typeString = "POINT "
	case *geom.LineString:
		typeString = "LINESTRING "
	case *geom.Polygon:
		typeString = "POLYGON "
	case *geom.MultiPoint:
		typeString = "MULTIPOINT "
	case *geom.MultiLineString:
		typeString = "MULTILINESTRING "
	case *geom.MultiPolygon:
		typeString = "MULTIPOLYGON "
	case *geom.GeometryCollection:
		typeString = "GEOMETRYCOLLECTION "
	default:
		return geom.ErrUnsupportedType{Value: g}
	}
	layout := g.Layout()
	switch layout {
	case geom.NoLayout:
		// Special case for empty GeometryCollections
		if g, ok := g.(*geom.GeometryCollection); !ok || !g.Empty() {
			return geom.ErrUnsupportedLayout(layout)
		}
	case geom.XY:
	case geom.XYZ:
		typeString += "Z "
	case geom.XYM:
		typeString += "M "
	case geom.XYZM:
		typeString += "ZM "
	default:
		return geom.ErrUnsupportedLayout(layout)
	}
	if _, err := b.WriteString(typeString); err != nil {
		return err
	}
	switch g := g.(type) {
	case *geom.Point:
		return writeFlatCoords0(b, g.FlatCoords(), layout.Stride())
	case *geom.LineString:
		return writeFlatCoords1(b, g.FlatCoords(), layout.Stride())
	case *geom.Polygon:
		return writeFlatCoords2(b, g.FlatCoords(), 0, g.Ends(), layout.Stride())
	case *geom.MultiPoint:
		if g.Empty() {
			return writeEMPTY(b)
		}
		return writeFlatCoords1(b, g.FlatCoords(), layout.Stride())
	case *geom.MultiLineString:
		if g.Empty() {
			return writeEMPTY(b)
		}
		return writeFlatCoords2(b, g.FlatCoords(), 0, g.Ends(), layout.Stride())
	case *geom.MultiPolygon:
		if g.Empty() {
			return writeEMPTY(b)
		}
		return writeFlatCoords3(b, g.FlatCoords(), g.Endss(), layout.Stride())
	case *geom.GeometryCollection:
		if g.Empty() {
			return writeEMPTY(b)
		}
		if _, err := b.WriteRune('('); err != nil {
			return err
		}
		for i, g := range g.Geoms() {
			if i != 0 {
				if _, err := b.WriteString(", "); err != nil {
					return err
				}
			}
			if err := write(b, g); err != nil {
				return err
			}
		}
		_, err := b.WriteRune(')')
		return err
	}
	return nil
}

func writeCoord(b *bytes.Buffer, coord []float64) error {
	for i, x := range coord {
		if i != 0 {
			if _, err := b.WriteRune(' '); err != nil {
				return err
			}
		}
		if _, err := b.WriteString(strconv.FormatFloat(x, 'f', -1, 64)); err != nil {
			return err
		}
	}
	return nil
}

func writeEMPTY(b *bytes.Buffer) error {
	_, err := b.WriteString("EMPTY")
	return err
}

func writeFlatCoords0(b *bytes.Buffer, flatCoords []float64, stride int) error {
	if _, err := b.WriteRune('('); err != nil {
		return err
	}
	if err := writeCoord(b, flatCoords[:stride]); err != nil {
		return err
	}
	_, err := b.WriteRune(')')
	return err
}

func writeFlatCoords1(b *bytes.Buffer, flatCoords []float64, stride int) error {
	if _, err := b.WriteRune('('); err != nil {
		return err
	}
	for i, n := 0, len(flatCoords); i < n; i += stride {
		if i != 0 {
			if _, err := b.WriteString(", "); err != nil {
				return err
			}
		}
		if err := writeCoord(b, flatCoords[i:i+stride]); err != nil {
			return err
		}
	}
	_, err := b.WriteRune(')')
	return err
}

func writeFlatCoords2(b *bytes.Buffer, flatCoords []float64, start int, ends []int, stride int) error {
	if _, err := b.WriteRune('('); err != nil {
		return err
	}
	for i, end := range ends {
		if i != 0 {
			if _, err := b.WriteString(", "); err != nil {
				return err
			}
		}
		if err := writeFlatCoords1(b, flatCoords[start:end], stride); err != nil {
			return err
		}
		start = end
	}
	_, err := b.WriteRune(')')
	return err
}

func writeFlatCoords3(b *bytes.Buffer, flatCoords []float64, endss [][]int, stride int) error {
	if _, err := b.WriteRune('('); err != nil {
		return err
	}
	start := 0
	for i, ends := range endss {
		if i != 0 {
			if _, err := b.WriteString(", "); err != nil {
				return err
			}
		}
		if err := writeFlatCoords2(b, flatCoords, start, ends, stride); err != nil {
			return err
		}
		start = ends[len(ends)-1]
	}
	_, err := b.WriteRune(')')
	return err
}
