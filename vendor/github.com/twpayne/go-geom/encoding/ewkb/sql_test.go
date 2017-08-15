package ewkb_test

import (
	"fmt"
	"log"

	"gopkg.in/DATA-DOG/go-sqlmock.v1"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
)

func Example_scan() {

	type City struct {
		Name     string
		Location ewkb.Point
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT name, ST_AsEWKB\(location\) FROM cities WHERE name = \?;`).
		WithArgs("London").
		WillReturnRows(
			sqlmock.NewRows([]string{"name", "location"}).
				AddRow("London", []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x52\xb8\x1e\x85\xeb\x51\xc0\x3f\x45\xf0\xbf\x95\xec\xc0\x49\x40")),
		)

	var c City
	if err := db.QueryRow(`SELECT name, ST_AsEWKB(location) FROM cities WHERE name = ?;`, "London").Scan(&c.Name, &c.Location); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Longitude: %v\n", c.Location.X())
	fmt.Printf("Latitude: %v\n", c.Location.Y())
	fmt.Printf("SRID: %v\n", c.Location.SRID())

	// Output:
	// Longitude: 0.1275
	// Latitude: 51.50722
	// SRID: 4326

}

func Example_value() {

	type City struct {
		Name     string
		Location ewkb.Point
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	mock.ExpectExec(`INSERT INTO cities \(name, location\) VALUES \(\?, \?\);`).
		WithArgs("London", []byte("\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x52\xb8\x1e\x85\xeb\x51\xc0\x3f\x45\xf0\xbf\x95\xec\xc0\x49\x40")).
		WillReturnResult(sqlmock.NewResult(1, 1))

	c := City{
		Name:     "London",
		Location: ewkb.Point{geom.NewPoint(geom.XY).MustSetCoords(geom.Coord{0.1275, 51.50722}).SetSRID(4326)},
	}

	result, err := db.Exec(`INSERT INTO cities (name, location) VALUES (?, ?);`, c.Name, &c.Location)
	if err != nil {
		log.Fatal(err)
	}
	rowsAffected, _ := result.RowsAffected()
	fmt.Printf("%d rows affected", rowsAffected)

	// Output:
	// 1 rows affected

}
