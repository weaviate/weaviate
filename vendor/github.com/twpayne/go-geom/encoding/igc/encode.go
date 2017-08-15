package igc

import (
	"fmt"
	"io"
	"math"
	"time"

	"github.com/twpayne/go-geom"
)

// An Encoder is an IGC encoder.
type Encoder struct {
	a string
	w io.Writer
}

type EncoderOption func(*Encoder)

func clamp(x, min, max int) int {
	switch {
	case x < min:
		return min
	case x > max:
		return max
	default:
		return x
	}
}

// NewEncoder returns a new Encoder that writes to w.
func NewEncoder(w io.Writer, options ...EncoderOption) *Encoder {
	e := &Encoder{w: w}
	for _, o := range options {
		o(e)
	}
	return e
}

// Encode encodes a LineString.
func (enc *Encoder) Encode(ls *geom.LineString) error {
	if enc.a != "" {
		if _, err := fmt.Fprintf(enc.w, "A%s\n", enc.a); err != nil {
			return err
		}
	}
	var t0 time.Time
	for i, n := 0, ls.NumCoords(); i < n; i++ {
		coord := ls.Coord(i)
		t := time.Unix(int64(coord[3]), 0).UTC()
		if t.Day() != t0.Day() || t.Month() != t0.Month() || t.Year() != t0.Year() {
			if _, err := fmt.Fprintf(enc.w, "HFDTE%02d%02d%02d\n", t.Day(), t.Month(), t.Year()%100); err != nil {
				return err
			}
			t0 = t
		}
		latMMin := int(math.Abs(60000 * coord[1]))
		if latMMin > 90*60000 {
			latMMin = 90 * 60000
		}
		latDeg, latMMin := latMMin/60000, latMMin%60000
		var latHemi string
		if coord[1] < 0 {
			latHemi = "S"
		} else {
			latHemi = "N"
		}
		lngMMin := int(math.Abs(60000 * coord[0]))
		if lngMMin > 180*60000 {
			lngMMin = 180 * 60000
		}
		lngDeg, lngMMin := lngMMin/60000, lngMMin%60000
		var lngHemi string
		if coord[0] < 0 {
			lngHemi = "W"
		} else {
			lngHemi = "E"
		}
		alt := clamp(int(coord[2]), 0, 10000)
		if _, err := fmt.Fprintf(enc.w, "B%02d%02d%02d%02d%05d%s%03d%05d%sA%05d%05d\n", t.Hour(), t.Minute(), t.Second(), latDeg, latMMin, latHemi, lngDeg, lngMMin, lngHemi, alt, alt); err != nil {
			return err
		}
	}
	return nil
}

func A(a string) EncoderOption {
	return func(e *Encoder) {
		e.a = a
	}
}
