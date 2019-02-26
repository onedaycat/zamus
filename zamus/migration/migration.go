package migration

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	migrate "github.com/rubenv/sql-migrate"
)

func Up(db *sql.DB, dir string, max int) (int, error) {
	migrations := &migrate.FileMigrationSource{
		Dir: dir,
	}

	n, err := migrate.ExecMax(db, "mysql", migrations, migrate.Up, max)
	if err != nil {
		return 0, err
	}

	return n, nil
}

func Down(db *sql.DB, dir string, max int) (int, error) {
	migrations := &migrate.FileMigrationSource{
		Dir: dir,
	}

	n, err := migrate.ExecMax(db, "mysql", migrations, migrate.Down, max)
	if err != nil {
		return 0, err
	}

	return n, nil
}
