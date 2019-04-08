
// g.V().has("pro, textContainsFuzzy("belin").valueMap()

g.V().has("prop_47").match(__.as("a").(textContainsFuzzy("belin")))
