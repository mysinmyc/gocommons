package persistency

import (
	"database/sql"
	"encoding/json"
	"fmt"
)

const (
	TABLE_BEANS            = "beans"
	FIELD_BEANS_ID         = "id"
	FIELD_BEANS_SERIALIZED = "serialized"
	DDL_BEANS_SQLITE       = "create table if not exists " + TABLE_BEANS + " (" + FIELD_BEANS_ID + " text  PRIMARY KEY , " + FIELD_BEANS_SERIALIZED + " BLOB)"
)

type IndentifiableInDb interface {
	GetIdInDb() string
}

type BeanNotFoundError struct {
	error
	beanID string
}

func (vSelf *BeanNotFoundError) Error() string {
	return fmt.Sprintf("Bean with id %s not found", vSelf.beanID)
}

func SqlLiteLoadBean(pDb *sql.DB, pBean IndentifiableInDb) error {

	vRows, vError := pDb.Query(
		fmt.Sprintf("select %s from %s where %s=?", FIELD_BEANS_SERIALIZED, TABLE_BEANS, FIELD_BEANS_ID), pBean.GetIdInDb())

	if vError != nil {
		return vError
	}

	defer vRows.Close()

	if vRows.Next() == false {
		return &BeanNotFoundError{beanID: pBean.GetIdInDb()}
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

func SqlLiteSaveBean(pDb *sql.DB, pBean IndentifiableInDb) error {

	vMashalledBean, vMarshallingError := json.Marshal(pBean)

	if vMarshallingError != nil {
		return vMarshallingError
	}

	_, vDbError := pDb.Exec(fmt.Sprintf("insert or replace into %s (%s,%s) values (?,?)", TABLE_BEANS, FIELD_BEANS_ID, FIELD_BEANS_SERIALIZED), pBean.GetIdInDb(), vMashalledBean)
	return vDbError
}
