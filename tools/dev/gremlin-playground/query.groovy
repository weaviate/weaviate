// g.V().has("kind", "thing").hasLabel("class_59").out().path().by(valueMap("prop_6b")).by(valueMap("prop_74"))

// exactly one ref prop
g.V().has("kind", "thing").has("prop_7d", "AB457")
  .optional(
    outE("prop_7f").inV().hasLabel("class_5d")
  ).dedup().path().by(valueMap())


//  possibly multiple ref props
g.V().has("kind", "thing").has("prop_7d", "AB457")
  .union(
    optional( outE("prop_7f").inV().hasLabel("class_5d")),
    optional(outE("prop_7e").inV().hasLabel("class_5f")),
  ).dedup().path().by(valueMap())

//  verifying the snippet doesn't break if a class has no outgoing edges at all
g.V().has("kind", "thing").has("prop_6b", "CantDecideWhichCountryItBelongsToo")
  .union(
    optional( outE("prop_7f").inV().hasLabel("class_5d")),
    optional(outE("prop_7e").inV().hasLabel("class_5f")),
  ).dedup().path().by(valueMap())

//  verifying it also works with infintely nested props
g.V().has("kind", "thing").hasLabel("class_5e")
  .union(
    optional(outE("prop_7f").inV().hasLabel("class_5d").optional(outE("prop_7a").inV().hasLabel("class_59"))),
    optional(outE("prop_7e").inV().hasLabel("class_5f")),
  ).dedup().path().by(valueMap())

// //  shortening the query if no ref prop is desired
// g.V().has("kind", "thing").has("prop_6b")
//   .path().by(valueMap())

g.V().has("kind", "thing").hasLabel("class_59").union(optional(outE("prop_6d").inV().hasLabel("class_5b"))).range(0, 100).path().by(valueMap())
