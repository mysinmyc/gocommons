package myhttp

import (
	"fmt"
	"math/rand"
	"net/http"
)

var (
	_sessions = make(map[string]*Session)
)

type Session struct {
	id         string
	parameters map[string]interface{}
}

func newSession() *Session {

	var vSessionID string
	for {
		vSessionID = fmt.Sprintf("%x", rand.Uint32())

		if _sessions[vSessionID] == nil {
			break
		}
	}

	vNewSession := &Session{id: vSessionID, parameters: make(map[string]interface{})}
	_sessions[vSessionID] = vNewSession
	return vNewSession
}

func GetOrCreateSession(pResponse http.ResponseWriter, pRequest *http.Request) *Session {

	vSessionCookie, vError := pRequest.Cookie("_sessionid")

	if vError == nil {
		vCurSession := _sessions[vSessionCookie.Value]

		if vCurSession != nil {
			return vCurSession
		}
	}

	vRis := newSession()
	http.SetCookie(pResponse, &http.Cookie{Name: "_sessionid", Value: vRis.id})
	return vRis
}

func (vSelf *Session) GetValue(pKey string, pDefaultValue interface{}) interface{} {

	vRis := vSelf.parameters[pKey]

	if vRis == nil {
		vRis = pDefaultValue
	}
	return vRis
}

func (vSelf *Session) SetValue(pKey string, pValue interface{}) {
	vSelf.parameters[pKey] = pValue
}
