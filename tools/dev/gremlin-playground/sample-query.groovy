g.V().has("classId", "class_3")
    
    .union(

      // overall meta.count
      union(
          count().as("count").project("count").by(select("count"))
        )
          .as("meta").project("meta").by(select("meta")),

      // string or date
      union(
        has("prop_19").count()
          .as("count").project("count").by(select("count")),
        groupCount().by("prop_15")
          .order(local).by(values, decr).limit(local, 3)
          .as("topOccurrences").project("topOccurrences").by(select("topOccurrences"))
        )
          .as("combined").project("name").by(select("combined")),

      // bool
      union(
        groupCount().by("prop_19")
          .as("boolGroupCount").project("boolGroupCount").by(select("boolGroupCount")),
        has("prop_19").count()
          .as("count").project("count").by(select("count")),
      )
        .as("combined").project("isCapital").by(select("combined")),

      // int or number
      aggregate("prop_18_aggregation").by("prop_18").cap("prop_18_aggregation").limit(1)
        .as("mean", "sum", "max", "min", "count")
        .select("mean", "sum", "max", "min", "count")
        .by(mean(local)).by(sum(local)).by(max(local)).by(min(local)).by(count(local))
        .as("combined").project("population").by(select("combined")),
      )


