package db

import (
	"os"
	"testing"
	"strconv"
	"strings"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/go-sql-driver/mysql"
)

var (
	dummyData = strings.Repeat("X",500)
)
func TestSqlite3InsertBulk(pTest *testing.T) {

	vTempDb:=os.TempDir()+"/__test"+strconv.Itoa(os.Getpid())+".db"
	defer os.Remove(vTempDb)
	vDbHelper,vDbHelperError:= NewDbHelper(string(DbType_sqlite3), vTempDb)
	if vDbHelperError != nil {
		pTest.Error(vDbHelperError)
	}

	defer vDbHelper.Close()
	
	_,vCreateTableError := vDbHelper.Exec("create table if not exists test (fielda text primary key)")
	if vCreateTableError != nil {
		pTest.Error("failed to create table",vCreateTableError)
	}

	testInsert(vDbHelper,"test","fielda",10000,true,true,pTest)
	testInsert(vDbHelper,"test","fielda",10000,true,true,pTest)

	
}

func TestMysqlInsertBulk(pTest *testing.T) {

	vDbHelper,vDbHelperError:= NewDbHelper(string(DbType_mysql), "test:test@tcp(127.0.0.1:3306)/test")

	if vDbHelperError != nil {
		pTest.Error(vDbHelperError)
	}

	defer vDbHelper.Close()

	vTableName:="__test"+strconv.Itoa(os.Getpid())	
	_,vCreateTableError := vDbHelper.Exec("create table "+vTableName+" (fielda varchar(700) primary key)")
	if vCreateTableError != nil {
		pTest.Error("failed to create table",vCreateTableError)
	}

	testInsert(vDbHelper,vTableName,"fielda",20000,true,true,pTest)

	defer vDbHelper.Exec("drop table "+vTableName)	
}

func testInsert(pDbHelper *DbHelper, pTargetTable string, pColumnName string, pRowsToInsert int, pReplace bool, pBulk bool, pTest *testing.T) {

	vInsert,vCreateInsertError:=pDbHelper.CreateInsert(pTargetTable,[]string {pColumnName}, InsertOptions{Replace:pReplace})
	if vCreateInsertError != nil {
		pTest.Error("An error occurred while creating insert",vCreateInsertError)
	}

	if pBulk {	
		_,vBeginBulkError:=vInsert.BeginBulk()
		if vBeginBulkError != nil {
			pTest.Error("An error occurred while begin bulk",vBeginBulkError)
		}	
	}

	for vCnt:=0;vCnt<pRowsToInsert;vCnt++ {
		_,vExecError:=vInsert.Exec("Item "+strconv.Itoa(vCnt)+dummyData)
		if vExecError!=nil {
			pTest.Error(vExecError)
		}
	}

	if pBulk {
		vEndBulkError:=vInsert.EndBulk()
		if vEndBulkError != nil {
			pTest.Error("An error occurred while end bulk")
		}	
	}

	vRow:=pDbHelper.GetDb().QueryRow("select count(*) from "+pTargetTable)

	var vRowsInTable int
	vCountError:=vRow.Scan(&vRowsInTable) 
	if vCountError != nil {
		pTest.Error("an error occurred while counting rows")
	}

	if vRowsInTable != pRowsToInsert {
		pTest.Errorf("invalid number of rows in table: current %d expected %d",vRowsInTable,pRowsToInsert)
	}

	//pDbHelper.Close()
}
