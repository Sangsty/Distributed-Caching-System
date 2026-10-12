package main

// Registers the "pgx" database/sql driver, the only third-party dependency of
// the whole project. It is imported here, and only here, so every other
// package stays dependency-free and tests without a database.
import _ "github.com/jackc/pgx/v5/stdlib"
