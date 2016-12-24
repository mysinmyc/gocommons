package db

type SqlFactory interface {
	CreateInsertStatement(string,string, InsertOptions)
}
