g.V().has("kind", "thing").or(
    has("classId", "class_3").has("prop_15", eq("Amsterdam")),
    has("classId", "class_4").has("prop_16", eq("Amsterdam")),
    has("classId", "class_5").has("prop_17", eq("Amsterdam")),
  ).valueMap("uuid", "prop_15", "classId")
