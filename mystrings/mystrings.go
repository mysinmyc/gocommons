package mystrings


func ReplaceAt(pSource string, pPosition int, pReplacement string) string {

	if pPosition <0 {
		return  pSource
	}

	if len(pSource) <= pPosition {
		return pSource
	}

	var vRis string

	if pPosition ==0 {
		vRis=pReplacement
	} else{
		vRis=pSource[0:pPosition]
	}

	vRis += pReplacement

	if len(vRis) < len(pSource){
		vRis+=pSource[len(vRis):]
	}

			
	return vRis
}
