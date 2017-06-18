package db

import (
	"sync"
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
	bulkManager  BulkManager
	mutex *sync.Mutex
}

type InsertOptions struct {
	Replace bool
	NumberOfAdditionalRows int
}

func BuildInsertStatementString(pDbType DbType, pTable string, pFields []string, pOptions InsertOptions) (string, error) {

	vRowValues := " ("
	for vCnt := 0; vCnt < len(pFields); vCnt++ {
		if vCnt > 0 {
			vRowValues+=",?"
		}	else {
			vRowValues+= "?"
		}
	}
	vRowValues+=")"

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

	vRis += " " + pTable + "(" + strings.Join(pFields, ",") + ") values "
	
	for vCnt:=-1; vCnt < pOptions.NumberOfAdditionalRows; vCnt++ {
		if vCnt > -1 {
			vRis+=" ,"
		}
		vRis+=vRowValues
	}

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

	return &SqlInsert{dbHelper: vSelf, table: pTable, options: pOptions, statement: vStatement, fields: pFields, mutex: &sync.Mutex{}},nil
}


func (vSelf *SqlInsert) Exec(pParameters ...interface{}) (sql.Result, error) {

	if vSelf.bulkManager !=nil{
		vEnqueueError:=vSelf.bulkManager.Enqueue(pParameters...)
		if vEnqueueError !=nil {
			return nil, diagnostic.NewError("Error during bulk enqueue", vEnqueueError)
		}

		return nil,nil
	}	
	return vSelf.statement.Exec(pParameters...)
}


func (vSelf *SqlInsert) BeginBulk(pBulkOptions BulkOptions) (bool,error) {

	if vSelf.bulkManager != nil {
		return false,nil
	}

	diagnostic.LogDebug("SqlInsert.BeginBulk", "Starting bulk insert on %s table", vSelf.table)

	vBulkManager,vBulkManagerError:= BuildBulkManager(vSelf,pBulkOptions)
	if vBulkManagerError != nil {
		return false,diagnostic.NewError("Error creating bulk manager",vBulkManagerError)
	}

	vBeginError:=vBulkManager.Begin()
	if vBeginError != nil {
		return false,diagnostic.NewError("Error starting bulk manager",vBeginError)
	}
	vSelf.bulkManager=vBulkManager
	return true,nil
}

func (vSelf *SqlInsert) Commit() (error) {
	if vSelf.bulkManager == nil {
		return nil
	}

	if diagnostic.IsLogTrace() {
		diagnostic.LogDebug("SqlInsert.Commit", "Commit data in real table %s", vSelf.table)
	}
	vCommitError:= vSelf.bulkManager.Commit()
	if vCommitError != nil {
		return diagnostic.NewError("Commit failed",vCommitError)
	}
	return nil
}

func (vSelf *SqlInsert) EndBulk() (error) {
		
	if vSelf.bulkManager == nil {
		return nil
	}

	diagnostic.LogDebug("SqlInsert.EndBulk", "Ending bulk operation...")
	vEndBulkError:= vSelf.bulkManager.End()
	if vEndBulkError != nil {
		return diagnostic.NewError("EndBulk failed",vEndBulkError)
	}
	vSelf.bulkManager=nil
	return nil
}

func (vSelf *SqlInsert) Close() error {

	if vSelf.bulkManager != nil  {
		vEndBulkError := vSelf.EndBulk()
		if vEndBulkError != nil {
			return diagnostic.NewError("Error closing insert bulk", vEndBulkError)
		}
	}

	vCloseStatementError:=vSelf.statement.Close()
	if vCloseStatementError != nil {
		return diagnostic.NewError("Error closing statement", vCloseStatementError)
	}

	return nil

}

func (vSelf *SqlInsert) Lock() {
	vSelf.mutex.Lock()
}

func (vSelf *SqlInsert) Unlock() {
	vSelf.mutex.Unlock()
}
