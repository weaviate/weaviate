package main

import (
	"log"
	"os"

	"github.com/twpayne/go-geom/encoding/igc"
	"github.com/twpayne/go-kml"
)

func run() error {
	i, err := igc.Read(os.Stdin)
	if err != nil {
		return err
	}
	var gxCoords []kml.Element
	for _, coord := range i.LineString.Coords() {
		gxCoords = append(gxCoords, kml.GxCoord(kml.Coordinate{
			Lon: coord[0],
			Lat: coord[1],
			Alt: coord[2],
		}))
	}
	return kml.GxKML(
		kml.Placemark(
			kml.GxTrack(append([]kml.Element{kml.AltitudeMode("absolute")}, gxCoords...)...),
		),
	).WriteIndent(os.Stdout, "", "  ")
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
