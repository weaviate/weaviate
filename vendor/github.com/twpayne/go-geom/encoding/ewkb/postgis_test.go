// +build integration

package ewkb_test

import (
	"database/sql"
	"reflect"
	"testing"

	_ "github.com/lib/pq"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
)

func TestPostGIS(t *testing.T) {
	db, err := sql.Open("postgres", "postgres://localhost/go-geom-test?binary_parameters=yes&sslmode=disable")
	if err != nil {
		t.Fatalf("sql.Open(...) == _, %v, want _, <nil>", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("db.Close() == %v, want <nil>", err)
		}
	}()

	for _, stmt := range []string{
		"CREATE EXTENSION IF NOT EXISTS postgis;",
		"CREATE TEMP TABLE testgeoms (geom GEOMETRY);",
		"INSERT INTO testgeoms (geom) VALUES (ST_PolygonFromText('POLYGON((5 3, 5 0, 7 0, 7 3, 5 3))'));",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("db.Exec(%q) == _, %v, want _, <nil>", stmt, err)
		}
	}

	queryP := &ewkb.Polygon{
		Polygon: geom.NewPolygon(geom.XY).MustSetCoords([][]geom.Coord{{
			{4, 4}, {4, 0}, {8, 0}, {8, 4}, {4, 4},
		}}),
	}
	var p ewkb.Polygon
	if err := db.QueryRow("SELECT ST_AsEWKB(geom) FROM testgeoms WHERE ST_Within(geom, $1);", queryP).Scan(&p); err != nil {
		t.Fatalf("db.QueryRow(...).Scan(...) == %v, want <nil>", err)
	}
	if got, want := p.Coords(), [][]geom.Coord{{{5, 3}, {5, 0}, {7, 0}, {7, 3}, {5, 3}}}; !reflect.DeepEqual(got, want) {
		t.Errorf("p.Coords() == %v, want %v", got, want)
	}
}
