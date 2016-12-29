package db

import (
	sql "database/sql"
	"github.com/mysinmyc/gocommons/diagnostic"
)

type DbHelperRows struct {
	rows 		*sql.Rows
	fields 		[]interface{}	
	columns		[]string
}

func (vSelf *DbHelper) Query(pQuery string, pArgs ...interface{}) (*DbHelperRows,error) {

	vRows,vError:= vSelf.db.Query(pQuery, pArgs...)

	if vError!=nil {
		return nil,diagnostic.NewError("failed to perform query %s", vError, pQuery)
	}
	vRis:= &DbHelperRows{rows:vRows}

	return vRis,nil	
}


func (vSelf *DbHelperRows) Close() error {
	return vSelf.rows.Close()
}

func (vSelf *DbHelperRows) Columns() ([]string,error) {
	return vSelf.rows.Columns()
}

func (vSelf *DbHelperRows) Err() error {
	return vSelf.rows.Err()
}

func (vSelf *DbHelperRows) Next() bool {
	return vSelf.rows.Next()
}

func (vSelf *DbHelperRows) Scan(pDest ...interface{}) error {
	return vSelf.rows.Scan(pDest...)
}
