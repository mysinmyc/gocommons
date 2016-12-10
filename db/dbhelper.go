package db

import (
	"database/sql"
)

type DbHelper struct {
	Db *sql.DB
}

func NewDbHelper(pDb *sql.DB) *DbHelper {

	return &DbHelper{Db: pDb}
}
