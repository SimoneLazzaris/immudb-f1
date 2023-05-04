/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"fmt"
	"log"
	"math"
	"regexp"
	"strconv"
	"strings"
	"unicode"
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

func valstring(name string, record []string) string {
	var t []string
	mdata := metadata[name]
	for i, field := range record {
		castType, ok := mdata.cast[i]
		if !ok {
			castType = isString
		}
		switch castType {
		case isString:
			t = append(t, fmt.Sprintf("'%s'", str_clean(field)))
		case isInt:
			ii := 0.0
			if field == "NULL" {
				t = append(t, "NULL")
				break
			}
			if field != "" {
				var err error
				ii, err = strconv.ParseFloat(field, 64)
				if err != nil {
					log.Printf("FIELDS: %v", record)
					log.Printf("Unable to convert field %s [%d]: %s", field, i, err.Error())
					ii = -1.0
				}
			}
			t = append(t, strconv.Itoa(int(ii)))
		case isFloat:
			ii := 0.0
			if field == "NULL" {
				t = append(t, "NULL")
				break
			}
			if field != "" {
				var err error
				ii, err = strconv.ParseFloat(field, 64)
				if err != nil {
					log.Printf("FIELDS: %v", record)
					log.Printf("Unable to convert field %s [%d]: %s", field, i, err.Error())
					ii = -1.0
				}
			}
			t = append(t, fmt.Sprintf("%f", ii))
		case isTimestamp:
			t = append(t, fmt.Sprintf("CAST('%s' AS TIMESTAMP)", field))
		case isDuration:
			t = append(t, fmt.Sprintf("%f", strToDuration(str_clean(field))))
		}
	}
	return strings.Join(t, ",")
}

func str_clean(s string) string {
	s1 := strings.ToValidUTF8(s, string([]rune{unicode.ReplacementChar}))
	s2 := strings.ReplaceAll(s1, "%", "%%")
	//	s3 := strings.ReplaceAll(s2, "'", "''") // escape single quote by doubling them
	s3 := strings.ReplaceAll(s2, "'", "?") // escape single quote by doubling them
	return s3
}
