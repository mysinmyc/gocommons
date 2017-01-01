package db

import (
	"database/sql"
	"github.com/mysinmyc/gocommons/diagnostic"
)

type BulkManager interface {
	Begin() error
	Enqueue(...interface{}) error
	Commit() error
	End() error
}


type BulkManagerMultiRows struct {
	parent *SqlInsert
	insertStatement   *sql.Stmt
	batchSize int
	pendingRowsCount int
	pendingRows []interface{}
}

func NewBulkManagerMultiRows(pParent *SqlInsert, pBatchSize int) (BulkManager, error) {
	return &BulkManagerMultiRows{parent:pParent,batchSize:pBatchSize},nil
}

func (vSelf *BulkManagerMultiRows) Begin() error {
	
	vStatementString, vStatementStringError := BuildInsertStatementString(vSelf.parent.dbHelper.GetDbType(), vSelf.parent.table, vSelf.parent.fields,InsertOptions{Replace:vSelf.parent.options.Replace, NumberOfAdditionalRows: vSelf.batchSize -1} )
	if vStatementStringError != nil {
		return diagnostic.NewError("failed to build insert statement", vStatementStringError)
	}

	vStatement, vStatementError := vSelf.parent.dbHelper.GetDb().Prepare(vStatementString)
        if vStatementError != nil {
                return diagnostic.NewError("failed to prepare insert statement %s", vStatementError, vStatementString)
        }
	vSelf.insertStatement = vStatement

	vSelf.pendingRows = make([]interface{},0,vSelf.batchSize*len(vSelf.parent.fields))
	return nil
}

func (vSelf *BulkManagerMultiRows) Enqueue(pParameters ...interface{}) error {
	vSelf.pendingRows = append(vSelf.pendingRows, pParameters...)	
	vSelf.pendingRowsCount++

	if vSelf.pendingRowsCount == vSelf.batchSize {
		return vSelf.Commit()
	}
	return nil
}

func (vSelf *BulkManagerMultiRows) Commit() error {

	if vSelf.pendingRowsCount==0 {
		return nil
	}

	if vSelf.pendingRowsCount == vSelf.batchSize {

		_,vInsertError := vSelf.insertStatement.Exec(vSelf.pendingRows...)
		if vInsertError != nil {
			return diagnostic.NewError("Failed to bulk insert data",vInsertError)
		}
	}else {

		vStatementString, vStatementStringError := BuildInsertStatementString(vSelf.parent.dbHelper.GetDbType(), vSelf.parent.table, vSelf.parent.fields,InsertOptions{Replace:vSelf.parent.options.Replace, NumberOfAdditionalRows: vSelf.pendingRowsCount -1} )
		if vStatementStringError != nil {
			return diagnostic.NewError("failed to build insert statement", vStatementStringError)
		}

		_,vInsertError := vSelf.parent.dbHelper.Exec(vStatementString,vSelf.pendingRows...)
		if vInsertError != nil {
			return diagnostic.NewError("Failed to bulk insert data",vInsertError)
		}
	}
	vSelf.pendingRowsCount=0
	vSelf.pendingRows = make([]interface{},0,vSelf.batchSize*len(vSelf.parent.fields))
	return nil
}

func (vSelf *BulkManagerMultiRows) End() error {
	vCommitError:= vSelf.Commit()
	if vCommitError != nil {
		return diagnostic.NewError("Commit failed",vCommitError)
	}
	return nil
}
