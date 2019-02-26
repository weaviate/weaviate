
// graph.tx().rollback()
// mgmt = graph.openManagement()
//   newProp = mgmt.makePropertyKey("location").dataType(Geoshape.class).cardinality(Cardinality.SINGLE).make()
//   existingIndex = mgmt.getGraphIndex("search")
//   mgmt.addIndexKey(existingIndex, newProp)
// mgmt.commit()

// g.addV().property("location", Geoshape.point(52.366667, 4.9)) // Amsterdam

g.V().has("location", geoWithin(Geoshape.circle(51.233333, 6.783333, 190)))

