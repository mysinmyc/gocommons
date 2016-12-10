package db

import (
	"database/sql"
	"strings"
)

type SqlInsert struct {
	dbHelper        *DbHelper
	statementString string
	table           string
	modifiers       string
	fields          []string
}

func CreateInsertStatement(pTable string, pModifiers string, pFields []string) string {

	vParameters := make([]string, len(pFields))
	for vCnt := 0; vCnt < len(pFields); vCnt++ {
		vParameters[vCnt] = "?"
	}
	return "insert " + pModifiers + " into " + pTable + "( " + strings.Join(pFields, ",") + ") values(" + strings.Join(vParameters, ",") + ");"

}

func (vSelf *DbHelper) CreateInsert(pTable string, pModifiers string, pFields []string) *SqlInsert {

	return &SqlInsert{dbHelper: vSelf, table: pTable, modifiers: pModifiers, statementString: CreateInsertStatement(pTable, pModifiers, pFields)}
}

func (vSelf *SqlInsert) Execute(pParameters ...interface{}) (sql.Result, error) {
	return vSelf.dbHelper.Db.Exec(vSelf.statementString, pParameters...)
}
