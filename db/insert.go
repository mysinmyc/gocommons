package db

import (
	"database/sql"
	"github.com/mysinmyc/gocommons/diagnostic"
	"strings"
)

type SqlInsert struct {
	dbHelper  *DbHelper
	table     string
	fields    []string
	statement *sql.Stmt
	options   InsertOptions
}

type InsertOptions struct {
	Replace bool
}

func BuildInsertStatementString(pDbType DbType, pTable string, pFields []string, pOptions InsertOptions) (string, error) {

	vParameters := make([]string, len(pFields))
	for vCnt := 0; vCnt < len(pFields); vCnt++ {
		vParameters[vCnt] = "?"
	}

	var vRis string
	if pOptions.Replace {
		switch pDbType {
		case DbType_sqlite3:
			vRis = "insert or replace into"
		case DbType_mysql:
			vRis = "replace into"
		default:
			return "", diagnostic.NewError("replace not supported for dbtype %v", nil,pDbType)
		}
	} else {
		vRis = "insert into"
	}

	vRis += " " + pTable + "(" + strings.Join(pFields, ",") + ") values(" + strings.Join(vParameters, ",")+")"

	return vRis,nil
}

func (vSelf *DbHelper) CreateInsert(pTable string, pFields []string, pOptions InsertOptions) (*SqlInsert, error) {

	vStatementString, vStatementStringError := BuildInsertStatementString(vSelf.GetDbType(), pTable, pFields, pOptions)
	if vStatementStringError != nil {
		return nil, diagnostic.NewError("failed to build insert statement", vStatementStringError)
	}

	vStatement, vStatementError := vSelf.GetDb().Prepare(vStatementString)
	if vStatementError != nil {
		return nil, diagnostic.NewError("failed to prepare insert statement %s", vStatementError, vStatementString)
	}

	return &SqlInsert{dbHelper: vSelf, table: pTable, options: pOptions, statement: vStatement},nil
}

func (vSelf *SqlInsert) Exec(pParameters ...interface{}) (sql.Result, error) {
	return vSelf.statement.Exec(pParameters...)
}

func (vSelf *SqlInsert) Close() error {
	return vSelf.statement.Close()
}
