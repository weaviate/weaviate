
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

// g.V().hasLabel("person").group().by("name").by(
//   fold().match(
//   __.as("a").unfold().values("networth").count().as("networth__count"),
//   __.as("a").unfold().values("networth").max().as("networth__maximum"),
//   __.as("a").unfold().values("networth").mean().as("networth__mean"),
//   __.as("a").unfold().values("networth").min().as("networth__minimum"),
//   __.as("a").unfold().values("networth").groupCount().order(local).by(values, decr).select(keys).limit(local, 1).as("networth__mode"),
//   __.as("a").unfold().values("age").count().as("age__count"),
//   __.as("a").unfold().values("age").max().as("age__maximum"),
//   __.as("a").unfold().values("age").mean().as("age__mean"),
//   __.as("a").unfold().values("age").min().as("age__minimum"),
//   __.as("a").unfold().values("age").groupCount().order(local).by(values, decr).select(keys).limit(local, 1).as("age__mode")
//   )
//   .select("networth__count","networth__maximum","networth__mean","networth__minimum","networth__mode").as("networth")
//   .select("age__count","age__maximum","age__mean","age__minimum","age__mode").as("age")
//   .select("networth", "age")
// ) 

g.V().has("kind", "thing").has("classId", "class_3")
				.group().by("prop_1a").by(
					fold()
						.match(
							__.as("a").unfold().values("prop_19").count().project("population_count").as("population__count"),
							__.as("a").unfold().values("prop_15").count().project("name_count").as("name__count")
						)
						.select("population__count").as("population")
						.select("name__count").as("name")
            .select("population", "name")
					)

