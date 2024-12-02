package admin

import "github.com/weaviate/weaviate/adapters/repos/db"

type Manager struct {
	// TODO interface? private fields? move maintenance mode stuff here too or just have migrator?
	// is admin the right abstraction or is it more about reindexer vs configurator?
	Migrator *db.Migrator
}
