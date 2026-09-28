//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rbac

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	fileadapter "github.com/casbin/casbin/v2/persist/file-adapter"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

// newManagerAt opens a Manager on the policy file under dir. Calling it again
// with the same dir acts as a restart, because Init re-reads policy.csv.
func newManagerAt(t *testing.T, dir string) (*Manager, error) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	return New(dir, rbacconf.Config{Enabled: true},
		config.Authentication{OIDC: config.OIDC{Enabled: true}, APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"test-user"}}},
		false, nil, logger)
}

var (
	attacker  = conv.UserNameWithTypeFromId("attacker", authentication.AuthTypeDb)
	adminRole = conv.PrefixRoleName(authorization.Admin)
)

// requireRestartWithoutAdmin restarts on dir and checks the policy file still
// loads and does not make attacker admin.
func requireRestartWithoutAdmin(t *testing.T, dir string) *Manager {
	t.Helper()
	m, err := newManagerAt(t, dir)
	require.NoError(t, err, "policy.csv must still load after the rejected write")
	roles, err := m.casbin.GetRolesForUser(attacker)
	require.NoError(t, err)
	assert.NotContains(t, roles, adminRole, "a stored value must not make attacker admin after a restart")
	return m
}

func TestUpsertRejectsRowsPolicyFileCannotStore(t *testing.T) {
	tests := []struct {
		name     string
		resource string
	}{
		// The comma gives the p row a fifth field, which fails every load.
		{name: "comma", resource: "namespaces/JFxylh,(select*from(select(sleep(20)))a)"},
		{name: "bare quote", resource: `namespaces/a"b`},
		{name: "leading quote", resource: `namespaces/"a`},
		{name: "LF", resource: "namespaces/a\nb"},
		{name: "CRLF", resource: "namespaces/a\r\nb"},
		{name: "row longer than the loader reads", resource: "namespaces/" + strings.Repeat("a", 70*1024)},
		// Each line after the split is a well-formed row, so the file loads and the
		// middle one assigns admin.
		{name: "LF injects an admin assignment", resource: "namespaces/x, (C), namespaces\ng, db:attacker, role:admin\np, role:evil, y"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := freshPolicyDir(t)
			m, err := newManagerAt(t, dir)
			require.NoError(t, err)
			require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{
				"incumbent": {{Resource: "namespaces/ok", Verb: authorization.READ, Domain: authorization.NamespacesDomain}},
			}))

			err = m.CreateRolesPermissions(map[string][]authorization.Policy{
				"good": {{Resource: "namespaces/fine", Verb: authorization.READ, Domain: authorization.NamespacesDomain}},
				"evil": {{Resource: tt.resource, Verb: authorization.READ, Domain: authorization.NamespacesDomain}},
			})
			require.Error(t, err)
			roles, err := m.GetRoles()
			require.NoError(t, err)
			assert.NotContains(t, roles, "good", "a refused upsert must store none of its roles")
			assert.NotContains(t, roles, "evil", "a refused upsert must store none of its roles")

			// The next write saves the whole model, so a refused row left in memory
			// would reach policy.csv here.
			require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{
				"later": {{Resource: "namespaces/later", Verb: authorization.READ, Domain: authorization.NamespacesDomain}},
			}))
			restarted := requireRestartWithoutAdmin(t, dir)
			roles, err = restarted.GetRoles()
			require.NoError(t, err)
			assert.Contains(t, roles, "incumbent")
			assert.NotContains(t, roles, "evil", "a rejected role must not be stored")
		})
	}
}

func TestAddRolesForUserRejectsSubjectsPolicyFileCannotStore(t *testing.T) {
	tests := []struct {
		name    string
		subject string
	}{
		{name: "OIDC user with comma", subject: "oidc:a,b"},
		{name: "OIDC user with quote", subject: `oidc:a"b`},
		{name: "OIDC user with LF", subject: "oidc:a\nb"},
		{name: "OIDC user with CRLF", subject: "oidc:a\r\nb"},
		{name: "OIDC user longer than the loader reads", subject: "oidc:" + strings.Repeat("a", 70*1024)},
		{name: "group with comma", subject: "group:a,b"},
		{name: "group with quote", subject: `group:"admins"`},
		// casbin loads a 'g' row with extra fields and reads the first two, so
		// "g, db:attacker, role:admin, role:viewer" makes attacker admin.
		{name: "comma injects an admin grant", subject: "db:attacker, role:admin"},
		{name: "LF injects an admin assignment", subject: "oidc:x, role:viewer\ng, db:attacker, role:admin\ng, oidc:y"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := freshPolicyDir(t)
			m, err := newManagerAt(t, dir)
			require.NoError(t, err)

			err = m.AddRolesForUser(tt.subject, []string{authorization.Viewer})
			require.Error(t, err)
			users, err := m.casbin.GetUsersForRole(conv.PrefixRoleName(authorization.Viewer))
			require.NoError(t, err)
			assert.NotContains(t, users, tt.subject, "a refused assignment must not reach the running model")

			// The next write saves the whole model, so a refused row left in memory
			// would reach policy.csv here.
			require.NoError(t, m.AddRolesForUser("db:later", []string{authorization.Viewer}))
			requireRestartWithoutAdmin(t, dir)
		})
	}
}

// TestStorableInputSurvivesRestart checks that characters the loader does not
// treat specially round-trip.
func TestStorableInputSurvivesRestart(t *testing.T) {
	dir := freshPolicyDir(t)
	m, err := newManagerAt(t, dir)
	require.NoError(t, err)

	resource := "namespaces/a:b-c_d.e*(f|g)[h]{i}#j k"
	subject := "oidc:https://issuer.example/a b#c@d"
	require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{
		"fine": {{Resource: resource, Verb: authorization.READ, Domain: authorization.NamespacesDomain}},
	}))
	require.NoError(t, m.AddRolesForUser(subject, []string{"fine"}))

	restarted, err := newManagerAt(t, dir)
	require.NoError(t, err)
	rows, err := restarted.casbin.GetFilteredNamedPolicy("p", 0, conv.PrefixRoleName("fine"))
	require.NoError(t, err)
	assert.Equal(t, [][]string{{conv.PrefixRoleName("fine"), resource, authorization.READ, authorization.NamespacesDomain}}, rows)
	users, err := restarted.casbin.GetUsersForRole(conv.PrefixRoleName("fine"))
	require.NoError(t, err)
	assert.Contains(t, users, subject)
}

// TestValidateStorableRowMatchesFileAdapter checks that conv.ValidateStorableRow
// passes a row if and only if casbin's file adapter saves and loads it unchanged.
func TestValidateStorableRowMatchesFileAdapter(t *testing.T) {
	const maxLine = 64*1024 - 1
	p := func(resource string) []string { return []string{"p", "role:a", resource, "R", "namespaces"} }
	pWithSize := func(size int) []string {
		row := p("")
		row[2] = strings.Repeat("a", size-len(strings.Join(row, ", ")))
		return row
	}
	tests := []struct {
		name     string
		row      []string
		storable bool
	}{
		{name: "plain", row: p("namespaces/x"), storable: true},
		{name: "regex characters", row: p("namespaces/a:b-c_d.e*(f|g)[h]{i}$^+?\\"), storable: true},
		{name: "unicode", row: p("namespaces/é日本"), storable: true},
		{name: "space and tab inside", row: p("namespaces/a b\tc"), storable: true},
		{name: "hash inside", row: p("namespaces/a#b"), storable: true},
		{name: "hash first", row: []string{"p", "#role:a", "namespaces/x", "R", "namespaces"}, storable: true},
		{name: "NUL", row: p("namespaces/a\x00b"), storable: true},
		{name: "lone CR inside", row: p("namespaces/a\rb"), storable: true},
		{name: "empty field", row: p(""), storable: true},
		{name: "trailing space on a middle field", row: p("namespaces/x "), storable: true},
		{name: "comma", row: p("namespaces/a,b"), storable: false},
		{name: "comma and space", row: p("namespaces/a, b"), storable: false},
		{name: "bare quote", row: p(`namespaces/a"b`), storable: false},
		{name: "leading quote", row: p(`"namespaces/a`), storable: false},
		{name: "quoted field", row: p(`"namespaces/a"`), storable: false},
		{name: "only a quote", row: p(`"`), storable: false},
		{name: "LF", row: p("namespaces/a\nb"), storable: false},
		{name: "CRLF", row: p("namespaces/a\r\nb"), storable: false},
		{name: "leading space on a middle field", row: p(" namespaces/x"), storable: false},
		{name: "leading tab on a middle field", row: p("\tnamespaces/x"), storable: false},
		{name: "trailing space on the last field", row: []string{"g", "db:a", "role:a "}, storable: false},
		{name: "trailing CR on the last field", row: []string{"g", "db:a", "role:a\r"}, storable: false},
		// The loader trims every Unicode space, not only ASCII ones.
		{name: "NBSP leading a middle field", row: p("\u00a0namespaces/x"), storable: false},
		{name: "ideographic space leading a middle field", row: p("\u3000namespaces/x"), storable: false},
		{name: "line separator leading a middle field", row: p("\u2028namespaces/x"), storable: false},
		{name: "NEL leading a middle field", row: p("\u0085namespaces/x"), storable: false},
		{name: "NBSP trailing the last field", row: []string{"g", "db:a", "role:a\u00a0"}, storable: false},
		{name: "zero-width space and BOM leading a middle field", row: p("\u200b\ufeffnamespaces/x"), storable: true},
		{name: "line and paragraph separators inside", row: p("namespaces/a\u2028b\u2029c\u0085d"), storable: true},
		{name: "DEL, VT, FF and ESC inside", row: p("namespaces/a\x7f\v\f\x1bb"), storable: true},
		{name: "invalid UTF-8 inside", row: p("namespaces/a\xff\xc0\xaf\xed\xa0\x80b"), storable: true},
		{name: "grouping plain", row: []string{"g", "oidc:https://issuer/a b@c", "role:a"}, storable: true},
		{name: "grouping comma", row: []string{"g", "db:a, role:admin", "role:a"}, storable: false},
		{name: "longest line the loader reads", row: pWithSize(maxLine), storable: true},
		{name: "one byte too long", row: pWithSize(maxLine + 1), storable: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "policy.csv")
			sentinel := []string{"g", "db:sentinel", "role:a"}

			m, err := model.NewModelFromString(MODEL)
			require.NoError(t, err)
			writer, err := casbin.NewEnforcer(m)
			require.NoError(t, err)
			for _, row := range [][]string{tt.row, sentinel} {
				if row[0] == "p" {
					_, err = writer.AddNamedPolicy("p", row[1:])
				} else {
					_, err = writer.AddNamedGroupingPolicy("g", row[1:])
				}
				require.NoError(t, err)
			}
			writer.SetAdapter(fileadapter.NewAdapter(path))
			require.NoError(t, writer.SavePolicy())

			m, err = model.NewModelFromString(MODEL)
			require.NoError(t, err)
			reader, err := casbin.NewEnforcer(m)
			require.NoError(t, err)
			reader.SetAdapter(fileadapter.NewAdapter(path))
			roundTrips := false
			if reader.LoadPolicy() == nil {
				var loaded [][]string
				ps, err := reader.GetNamedPolicy("p")
				require.NoError(t, err)
				for _, row := range ps {
					loaded = append(loaded, append([]string{"p"}, row...))
				}
				gs, err := reader.GetNamedGroupingPolicy("g")
				require.NoError(t, err)
				for _, row := range gs {
					loaded = append(loaded, append([]string{"g"}, row...))
				}
				roundTrips = assert.ObjectsAreEqual([][]string{tt.row, sentinel}, loaded) ||
					assert.ObjectsAreEqual([][]string{sentinel, tt.row}, loaded)
			}

			assert.Equal(t, tt.storable, roundTrips, "file adapter round trip")
			err = conv.ValidateStorableRow(tt.row...)
			assert.Equal(t, tt.storable, err == nil, "ValidateStorableRow: %v", err)
		})
	}
}
