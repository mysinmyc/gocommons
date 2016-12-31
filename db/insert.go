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
	bulk	  *SqlInsertBulk
}

type SqlInsertBulk struct {
	insertStatement   *sql.Stmt
	pendingItems      int64
}

type InsertOptions struct {
	Replace bool
}

const (
	BulkInsert_BatchSize int64=500
)

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

	return &SqlInsert{dbHelper: vSelf, table: pTable, options: pOptions, statement: vStatement, fields: pFields},nil
}


func (vSelf *SqlInsert) Exec(pParameters ...interface{}) (sql.Result, error) {

	if vSelf.bulk !=nil{
		vSqlResult,vError:= vSelf.bulk.insertStatement.Exec(pParameters...)
		if vError !=nil {
			return vSqlResult, diagnostic.NewError("Error during insert bulk", vError)
		}

		vSelf.bulk.pendingItems++
		if vSelf.bulk.pendingItems > BulkInsert_BatchSize {
			diagnostic.LogDebug("SqlInsert.Exec", "BulkInsert batch size of %d reached, forcing commit", BulkInsert_BatchSize)
			vCommitError:=vSelf.Commit()	
			if vCommitError != nil {
				return nil, diagnostic.NewError("Error during bulk checkpoint", vCommitError)
			}
		}
		return vSqlResult,nil
	}	
	return vSelf.statement.Exec(pParameters...)
}


func (vSelf *SqlInsert) BeginBulk() (bool,error) {

	if vSelf.bulk != nil {
		return false,nil
	}

	var vInsertStatement *sql.Stmt
	
	vDbType := vSelf.dbHelper.GetDbType()	
	switch vDbType {
		case DbType_sqlite3:

			vSelf.dbHelper.SetMaxOpenConns(1)

			vSelf.dbHelper.Exec("ATTACH DATABASE ':memory:' AS __memorydb")
		
			_,vCreateTableError:=vSelf.dbHelper.GetDb().Exec("CREATE TABLE IF NOT EXISTS __memorydb."+vSelf.table+"_bulk as select * from "+vSelf.table+" where 2=1")
			if vCreateTableError != nil {
				return false,diagnostic.NewError("An error occurred while creating temp table", vCreateTableError)
			}

			vStatementString, vStatementStringError := BuildInsertStatementString(vDbType, "__memorydb."+vSelf.table+"_bulk", vSelf.fields,InsertOptions{} )
			if vStatementStringError != nil {
				return false, diagnostic.NewError("failed to build insert statement", vStatementStringError)
			}

			var vInsertStatementError error
			vInsertStatement, vInsertStatementError = vSelf.dbHelper.GetDb().Prepare(vStatementString)
			if vInsertStatementError != nil {
				return false, diagnostic.NewError("failed to prepare insert statement %s", vInsertStatementError, vStatementString)
			}
		case DbType_mysql:
			_,vCreateTableError:=vSelf.dbHelper.GetDb().Exec("CREATE TABLE IF NOT EXISTS "+vSelf.table+"_bulk ENGINE=MEMORY as select * from "+vSelf.table+" where 2=1")
			if vCreateTableError != nil {
				return false,diagnostic.NewError("An error occurred while creating temp table", vCreateTableError)
			}

			vStatementString, vStatementStringError := BuildInsertStatementString(vDbType, vSelf.table+"_bulk", vSelf.fields,InsertOptions{} )
			if vStatementStringError != nil {
				return false, diagnostic.NewError("failed to build insert statement", vStatementStringError)
			}

			var vInsertStatementError error
			vInsertStatement, vInsertStatementError = vSelf.dbHelper.GetDb().Prepare(vStatementString)
			if vInsertStatementError != nil {
				return false, diagnostic.NewError("failed to prepare insert statement %s", vInsertStatementError, vStatementString)
			}
		default:
			return false,diagnostic.NewError("Bulk not supported for dbType %s", nil, vDbType)
	}

	vSelf.bulk= &SqlInsertBulk {insertStatement: vInsertStatement}
	return true,nil
}

func (vSelf *SqlInsert) Commit() (error) {
	if vSelf.bulk == nil {
		return nil
	}

	vDbType := vSelf.dbHelper.GetDbType()	
	switch vDbType {
		case DbType_sqlite3:
			var vInsertModifiers string
			if vSelf.options.Replace {
				vInsertModifiers = " or replace "
			}

			_,vCommitError:=vSelf.dbHelper.GetDb().Exec("insert "+vInsertModifiers+" into "+vSelf.table+" select * from   __memorydb."+vSelf.table+"_bulk")
			if vCommitError != nil {
				return diagnostic.NewError("An error occurred while commit bulk", vCommitError)
			}

			_,vDeleteError:=vSelf.dbHelper.GetDb().Exec("delete from  __memorydb."+vSelf.table+"_bulk")
			if vDeleteError != nil {
				return diagnostic.NewError("An error occurred while cleaning temp table during commit", vDeleteError)
			}
		case DbType_mysql:
			var vInsertPrefix string
			if vSelf.options.Replace {
				vInsertPrefix = "replace "
			} else {
				vInsertPrefix = "insert into "
			}

			_,vCommitError:=vSelf.dbHelper.GetDb().Exec(vInsertPrefix+" into "+vSelf.table+" select * from  "+vSelf.table+"_bulk")
			if vCommitError != nil {
				return diagnostic.NewError("An error occurred while commit bulk", vCommitError)
			}

			_,vDeleteError:=vSelf.dbHelper.GetDb().Exec("delete from "+vSelf.table+"_bulk")
			if vDeleteError != nil {
				return diagnostic.NewError("An error occurred while cleaning temp table during commit", vDeleteError)
			}

	}
	vSelf.bulk.pendingItems=0
	return nil
}

func (vSelf *SqlInsert) EndBulk() (error) {
		
	if vSelf.bulk == nil {
		return nil
	}

	vCommitError:=vSelf.Commit()
	
	vSelf.bulk =nil	
	
	vDbType := vSelf.dbHelper.GetDbType()	
	switch vDbType {
		case DbType_sqlite3:
			vSelf.dbHelper.SetMaxOpenConns(-1)
			
			_,vDropTableError:=vSelf.dbHelper.GetDb().Exec("drop table __memorydb."+vSelf.table+"_bulk")
			if vDropTableError != nil {
				//return diagnostic.NewError("An error occurred while dropping temp table", vDropTableError)
				diagnostic.LogWarning("SqlInsert.EndBulk","An error occurred while dropping temp table", vDropTableError)
			}
	}
	return vCommitError
}

func (vSelf *SqlInsert) Close() error {

	if vSelf.bulk != nil  {
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
