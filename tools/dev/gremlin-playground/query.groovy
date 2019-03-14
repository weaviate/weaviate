
// graph.tx().rollback()
// mgmt = graph.openManagement()
//   newProp = mgmt.makePropertyKey("location").dataType(Geoshape.class).cardinality(Cardinality.SINGLE).make()
//   existingIndex = mgmt.getGraphIndex("search")
//   mgmt.addIndexKey(existingIndex, newProp)
// mgmt.commit()

// g.addV().property("prop_17", Geoshape.point(52.366667, 4.9)).property("prop_15", "Manualamsterdam") // Amsterdam

// g.V().has("location", geoWithin(Geoshape.circle(51.233333, 6.783333, 190)))

// g.V().has("kind", "thing").has("prop_f1", "Amsterdam").valueMap()

// g.V()
// .has("prop_17", geoWithin(Geoshape.circle(51.225555, 6.782778, 200))).range(0, 100)
// .path().by(valueMap())

// g.V().has("kind", "thing").has("classId", "class_3").union(
// or(has("prop_15", eq("Amsterdam")), has("prop_15", eq("Berlin"))).union(union(has("prop_15").count().as("count").project("count").by(select("count"))).as("name").project("name").by(select("name")))
// )
// 8

// g.V().values("name")

g.V().has("kind", "thing").has("classId", "class_3")
.union(
     union(
       has('prop_15').count().project("count").project("name"),
       groupCount().by('prop_15').order(local).by(values, decr).limit(local, 3).project('histogram').project('name')
     )
  )
  .group().by(select(keys).unfold()).by(
    select(values).unfold().group()
    .by( select(keys).unfold())
    .by( select(values).unfold())
  )

