package wkb_test

import (
	"fmt"
	"log"

	"gopkg.in/DATA-DOG/go-sqlmock.v1"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkb"
)

func Example_scan() {

	type City struct {
		Name     string
		Location wkb.Point
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT name, ST_AsBinary\(location\) FROM cities WHERE name = \?;`).
		WithArgs("London").
		WillReturnRows(
			sqlmock.NewRows([]string{"name", "location"}).
				AddRow("London", []byte("\x01\x01\x00\x00\x00\x52\xB8\x1E\x85\xEB\x51\xC0\x3F\x45\xF0\xBF\x95\xEC\xC0\x49\x40")),
		)

	var c City
	if err := db.QueryRow(`SELECT name, ST_AsBinary(location) FROM cities WHERE name = ?;`, "London").Scan(&c.Name, &c.Location); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Longitude: %v\n", c.Location.X())
	fmt.Printf("Latitude: %v\n", c.Location.Y())

	// Output:
	// Longitude: 0.1275
	// Latitude: 51.50722

}

func Example_value() {

	type City struct {
		Name     string
		Location wkb.Point
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	mock.ExpectExec(`INSERT INTO cities \(name, location\) VALUES \(\?, \?\);`).
		WithArgs("London", []byte("\x01\x01\x00\x00\x00\x52\xB8\x1E\x85\xEB\x51\xC0\x3F\x45\xF0\xBF\x95\xEC\xC0\x49\x40")).
		WillReturnResult(sqlmock.NewResult(1, 1))

	c := City{
		Name:     "London",
		Location: wkb.Point{geom.NewPoint(geom.XY).MustSetCoords(geom.Coord{0.1275, 51.50722})},
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
