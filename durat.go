package main

import (
	"math"
	"regexp"
	"strconv"
)

var durat *regexp.Regexp = nil

func strToDuration(s string) float64 {
	if durat == nil {
		durat = regexp.MustCompile("(((?P<hh>[0-9]{1,2}):)?(?P<mm>[0-9]{1,2}):)?(?P<ss>[0-9]{1,2}).(?P<ms>[0-9]+)")
	}
	matches := durat.FindStringSubmatch(s)
	names := durat.SubexpNames()

	var dur float64 = 0.0

	for i, match := range matches {
		switch names[i] {
		case "hh":
			hh, _ := strconv.Atoi(match)
			dur = dur + float64(hh)*3600.0
		case "mm":
			mm, _ := strconv.Atoi(match)
			dur = dur + float64(mm)*60.0
		case "ss":
			ss, _ := strconv.Atoi(match)
			dur = dur + float64(ss)
		case "ms":
			ms, _ := strconv.Atoi(match)
			dur = dur + float64(ms)/math.Pow10(len(match))
		}
	}
	return dur
}
