package db

import (
	"encoding/json"
	"fmt"
	"github.com/mysinmyc/gocommons/diagnostic"
	"github.com/mysinmyc/gocommons/persistency"
)

const (
	TABLE_BEANS            = "beans"
	FIELD_BEANS_ID         = "id"
	FIELD_BEANS_SERIALIZED = "serialized"
	DDL_BEANS_SQLITE       = "create table if not exists " + TABLE_BEANS + " (" + FIELD_BEANS_ID + " text  PRIMARY KEY , " + FIELD_BEANS_SERIALIZED + " BLOB)"
	DDL_BEANS_MYSQL        = "create table if not exists " + TABLE_BEANS + " (" + FIELD_BEANS_ID + " varchar(700)  PRIMARY KEY , " + FIELD_BEANS_SERIALIZED + " BLOB)"
)

type IndentifiableInDb interface {
	GetIdInDb() string
}


func (vSelf *DbHelper) initBeans() error {
	if vSelf.beansInitialized {
		return nil
	}

	switch vSelf.GetDbType() {
		case DbType_sqlite3:
			_,vCreateError:=vSelf.Exec(DDL_BEANS_SQLITE)
			return vCreateError
		case DbType_mysql:
			_,vCreateError:=vSelf.Exec(DDL_BEANS_MYSQL)
			return vCreateError
	}

	return diagnostic.NewError("Beans not supported for dbtype %s", nil,vSelf.GetDbType())
}

func (vSelf *DbHelper) LoadBean(pBean IndentifiableInDb) error {

	vSelf.initBeans()

	vRows, vError := vSelf.GetDb().Query(
		fmt.Sprintf("select %s from %s where %s=?", FIELD_BEANS_SERIALIZED, TABLE_BEANS, FIELD_BEANS_ID), pBean.GetIdInDb())

	if vError != nil {
		return vError
	}

	defer vRows.Close()

	if vRows.Next() == false {
		return &persistency.BeanNotFoundError{BeanID: pBean.GetIdInDb()}
	}

	var vData []byte
	vColumnError := vRows.Scan(&vData)

	if vColumnError != nil {
		return vColumnError
	}

	vUnmarshalError := json.Unmarshal(vData, pBean)

	if vUnmarshalError != nil {
		return vUnmarshalError
	}

	return nil
}

func (vSelf *DbHelper) SaveBean(pBean IndentifiableInDb) error {
	
	vSelf.initBeans()
	vMarshalledBean, vMarshallingError := json.Marshal(pBean)

	if vMarshallingError != nil {
		return diagnostic.NewError("Error while marshalling bean to json",vMarshallingError)
	}

	vInsert,vInsertError:=vSelf.CreateInsert(TABLE_BEANS,[]string{FIELD_BEANS_ID, FIELD_BEANS_SERIALIZED}, InsertOptions{ Replace:true})	
	defer vInsert.Close()	
	if vInsertError != nil {
		return diagnostic.NewError("Error while creating insert",vInsertError)
	}

	_, vInsertExec := vInsert.Exec(pBean.GetIdInDb(), vMarshalledBean)

	if vInsertExec != nil {
		return diagnostic.NewError("Error while executing insert",vInsertExec)
	}

	return nil
}
