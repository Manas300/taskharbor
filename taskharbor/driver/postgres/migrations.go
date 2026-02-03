package postgres

import (
	"context"
	"embed"
	"io/fs"
	"path"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

/*
We will use go:embed to embed the
migration files during compile time
so it can be accessible @ run time.
*/
var migrationFS embed.FS

/*
This will be stored in th_schema_migrations
and version can be the filename (001, 002, etc)
*/
type migration struct {
	version  string
	filename string
	sql      string
}

/*
This function will apply all embedded SQL
migrations in the version order.
  - reads applied version from th_schema_migrations
  - executes each migration inside a transaction
  - records the migration version on success
*/
func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	return nil
}

/*
This function will load the migrations
as a FS and sort it and return a list
of applicable migrations
*/
func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return nil, err
	}

	var migs []migration
	for _, e := range entries {
		if e.IsDir() {
			continue
		}

		name := e.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}

		b, err := migrationFS.ReadFile(
			path.Join("migrations", name),
		)
		if err != nil {
			return nil, err
		}

		migs = append(migs, migration{
			version:  name,
			filename: name,
			sql:      string(b),
		})
	}

	// Sort in lexicographical order
	sort.Slice(migs, func(i, j int) bool {
		return migs[i].filename < migs[j].filename
	})

	return migs, nil
}
