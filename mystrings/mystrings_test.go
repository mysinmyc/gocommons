package mystrings

import (
	"testing"
)

type replacementTest struct {
	Source string
	Position int
	Replacement string
	Expected string
}

var (
	replacementTests= []replacementTest {
		replacementTest{Source:"ciao ciao", Position:5, Replacement:"mia", Expected:"ciao miao"},
		replacementTest{Source:"ciao ciao", Position:-1, Replacement:"mia", Expected:"ciao ciao"},
		replacementTest{Source:"ciao ciao", Position:10, Replacement:"mia", Expected:"ciao ciao"},
		replacementTest{Source:"ciao ciao", Position:4, Replacement:",", Expected:"ciao,ciao"}}
)


func TestReplaceAt(pTest *testing.T) {

	for _,vCurReplacementTest:= range replacementTests {
		vCurRis:=ReplaceAt(vCurReplacementTest.Source,vCurReplacementTest.Position,vCurReplacementTest.Replacement)
		if vCurRis != vCurReplacementTest.Expected {
			pTest.Errorf("replacement test %#v failed, result is %s", vCurReplacementTest,vCurRis)
		}
	}
}
