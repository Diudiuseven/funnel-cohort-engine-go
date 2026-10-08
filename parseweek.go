package funnel

import "strconv"

func parseWeekKey(key string, y, w *int) (string, error) {
	if len(key) != 8 || key[4:6] != "-W" {
		return key, strconv.ErrSyntax
	}
	yy, err1 := strconv.Atoi(key[0:4])
	ww, err2 := strconv.Atoi(key[6:8])
	if err1 != nil || err2 != nil {
		return key, strconv.ErrSyntax
	}
	*y, *w = yy, ww
	return key, nil
}
