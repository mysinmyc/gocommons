package db

import (
	"database/sql"
	"github.com/mysinmyc/gocommons/diagnostic"
)

type BulkManagerInMemory struct {
        parent *SqlInsert
        insertStatement   *sql.Stmt
        batchSize int
        pendingRowsCount int
}

func NewBulkManagerInMemory(pParent *SqlInsert, pBatchSize int) (BulkManager, error) {
	vRis:= &BulkManagerInMemory{parent:pParent,batchSize:pBatchSize}
	return vRis,nil
	
}

func (vSelf *BulkManagerInMemory) Begin() error {
	var vInsertStatement *sql.Stmt
	vDbType := vSelf.parent.dbHelper.GetDbType()	
	switch vDbType {
		case DbType_sqlite3:

			vSelf.parent.dbHelper.SetMaxOpenConns(1)

			vSelf.parent.dbHelper.Exec("ATTACH DATABASE ':memory:' AS __memorydb")
		
			_,vCreateTableError:=vSelf.parent.dbHelper.GetDb().Exec("CREATE TABLE IF NOT EXISTS __memorydb."+vSelf.parent.table+"_bulk as select * from "+vSelf.parent.table+" where 2=1")
			if vCreateTableError != nil {
				return diagnostic.NewError("An error occurred while creating temp table", vCreateTableError)
			}

			vStatementString, vStatementStringError := BuildInsertStatementString(vDbType, "__memorydb."+vSelf.parent.table+"_bulk", vSelf.parent.fields,InsertOptions{} )
			if vStatementStringError != nil {
				return diagnostic.NewError("failed to build insert statement", vStatementStringError)
			}

			var vInsertStatementError error
			vInsertStatement, vInsertStatementError = vSelf.parent.dbHelper.GetDb().Prepare(vStatementString)
			if vInsertStatementError != nil {
				return diagnostic.NewError("failed to prepare insert statement %s", vInsertStatementError, vStatementString)
			}
		case DbType_mysql:
			_,vCreateTableError:=vSelf.parent.dbHelper.GetDb().Exec("CREATE TABLE IF NOT EXISTS "+vSelf.parent.table+"_bulk ENGINE=MEMORY as select * from "+vSelf.parent.table+" where 2=1")
			if vCreateTableError != nil {
				return diagnostic.NewError("An error occurred while creating temp table", vCreateTableError)
			}

			vStatementString, vStatementStringError := BuildInsertStatementString(vDbType, vSelf.parent.table+"_bulk", vSelf.parent.fields,InsertOptions{} )
			if vStatementStringError != nil {
				return diagnostic.NewError("failed to build insert statement", vStatementStringError)
			}

			var vInsertStatementError error
			vInsertStatement, vInsertStatementError = vSelf.parent.dbHelper.GetDb().Prepare(vStatementString)
			if vInsertStatementError != nil {
				return diagnostic.NewError("failed to prepare insert statement %s", vInsertStatementError, vStatementString)
			}
		default:
			return diagnostic.NewError("Bulk not supported for dbType %s", nil, vDbType)
	}

	vSelf.insertStatement=vInsertStatement
	return nil	
}

func (vSelf *BulkManagerInMemory) Enqueue(pParameters ...interface{}) error {

	_,vError:= vSelf.insertStatement.Exec(pParameters...)
	if vError !=nil {
		return diagnostic.NewError("Error during insert bulk", vError)
	}

	vSelf.pendingRowsCount++
	if vSelf.pendingRowsCount == vSelf.batchSize {
		diagnostic.LogDebug("BulkManagerInMemory.Enqueue", "BulkInsert batch size of %d reached, forcing commit", vSelf.batchSize)
		vCommitError:=vSelf.Commit()	
		if vCommitError != nil {
			return diagnostic.NewError("Error during bulk checkpoint", vCommitError)
		}
	}
	return nil
}

func (vSelf *BulkManagerInMemory) Commit() error {

	if vSelf.pendingRowsCount==0 {
		return nil
	}


	vDbType := vSelf.parent.dbHelper.GetDbType()	
	switch vDbType {
		case DbType_sqlite3:
			var vInsertModifiers string
			if vSelf.parent.options.Replace {
				vInsertModifiers = " or replace "
			}

			_,vCommitError:=vSelf.parent.dbHelper.GetDb().Exec("insert "+vInsertModifiers+" into "+vSelf.parent.table+" select * from   __memorydb."+vSelf.parent.table+"_bulk")
			if vCommitError != nil {
				return diagnostic.NewError("An error occurred while commit bulk", vCommitError)
			}

			_,vDeleteError:=vSelf.parent.dbHelper.GetDb().Exec("delete from  __memorydb."+vSelf.parent.table+"_bulk")
			if vDeleteError != nil {
				return diagnostic.NewError("An error occurred while cleaning temp table during commit", vDeleteError)
			}
		case DbType_mysql:
			var vInsertPrefix string
			if vSelf.parent.options.Replace {
				vInsertPrefix = "replace "
			} else {
				vInsertPrefix = "insert into "
			}

			_,vCommitError:=vSelf.parent.dbHelper.GetDb().Exec(vInsertPrefix+" into "+vSelf.parent.table+" select * from  "+vSelf.parent.table+"_bulk")
			if vCommitError != nil {
				return diagnostic.NewError("An error occurred while commit bulk", vCommitError)
			}

			_,vDeleteError:=vSelf.parent.dbHelper.GetDb().Exec("delete from "+vSelf.parent.table+"_bulk")
			if vDeleteError != nil {
				return diagnostic.NewError("An error occurred while cleaning temp table during commit", vDeleteError)
			}

	}
	vSelf.pendingRowsCount=0
	return nil
}

func (vSelf *BulkManagerInMemory) End() error {
        vCommitError:= vSelf.Commit()
        if vCommitError != nil {
                return diagnostic.NewError("Commit failed",vCommitError)
        }
        return nil
}

