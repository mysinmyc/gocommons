package db

import (
	"database/sql"
	"reflect"
	"strings"
)

type DbType string

const (
	DbType_sqlite3 DbType ="sqlite3"
	DbType_mysql DbType ="mysql"
	DbType_unknown DbType ="unknown"

)

type DbHelper struct {
	Db *sql.DB
	dbType  DbType
	beansInitialized bool
}

func NewDbHelper(pDriver string, pDataSourceName string) (*DbHelper, error) {

	vDb,vDbError := sql.Open(pDriver, pDataSourceName)

	if vDbError !=nil {
		return nil, vDbError
	}

	return NewDbHelperFor(vDb), nil
}

func NewDbHelperFor(pDb *sql.DB) *DbHelper {
	return &DbHelper{Db: pDb}
}

func GetDbType(pDb *sql.DB) DbType {

	vDriver :=reflect.TypeOf(pDb.Driver()).String()

	if strings.Contains(vDriver,"sqlite3") {
		return DbType_sqlite3
	}
	if strings.Contains(vDriver,"mysql") {
		return DbType_mysql
	}
	return DbType_unknown
}

func (vSelf *DbHelper) GetDbType() DbType {
	if vSelf.dbType == "" {
		vSelf.dbType = GetDbType(vSelf.Db)	
	}

	return vSelf.dbType
}

func (vSelf *DbHelper) GetDb() *sql.DB {
	return vSelf.Db
}


func (vSelf *DbHelper) Exec(pQuery string, pParameters ...interface{}) (sql.Result,error) {
	return vSelf.Db.Exec(pQuery,pParameters...)
}

func (vSelf *DbHelper) Close() error {
	return vSelf.Db.Close()
}
