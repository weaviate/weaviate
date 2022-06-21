package additional

func CertaintyToDist(maybeCertainty *float64) (distPtr *float64) {
	if maybeCertainty != nil {
		dist := 1 - *maybeCertainty
		distPtr = &dist
	}
	return
}

func CertaintyToScore(maybeCertainty *float64) (scorePtr *float64) {
	if distPtr := CertaintyToDist(maybeCertainty); distPtr != nil {
		score := -(*distPtr)
		scorePtr = &score
	}
	return
}
