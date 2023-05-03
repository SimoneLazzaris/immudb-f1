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
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"unicode"
	// 	"math/rand"
	// 	"sync"
	// 	"time"

	// 	"github.com/codenotary/immudb/pkg/api/schema"
	immudb "github.com/codenotary/immudb/pkg/client"
	// 	"google.golang.org/protobuf/types/known/emptypb"
)

type cfg struct {
	IpAddr   string
	Port     int
	Username string
	Password string
	DBName   string
}

func parseConfig() (c cfg) {
	flag.StringVar(&c.IpAddr, "addr", "", "IP address of immudb server")
	flag.IntVar(&c.Port, "port", 3322, "Port number of immudb server")
	flag.StringVar(&c.Username, "user", "immudb", "Username for authenticating to immudb")
	flag.StringVar(&c.Password, "pass", "immudb", "Password for authenticating to immudb")
	flag.StringVar(&c.DBName, "db", "defaultdb", "Name of the database to use")
	flag.Parse()
	return
}

func connect(config cfg) (immudb.ImmuClient, context.Context) {
	ctx := context.Background()
	opts := immudb.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port)

	var client immudb.ImmuClient
	var err error

	client, err = immudb.NewImmuClient(opts)
	if err != nil {
		log.Printf("Failed to connect. Reason: %s", err.Error())
		return client, ctx
	}
	err = client.OpenSession(ctx, []byte(config.Username), []byte(config.Password), config.DBName)
	if err != nil {
		log.Fatalln("Failed to connect. Reason:", err)
	}
	return client, ctx
}

const (
	isString = iota
	isInt
	isTimestamp
	isFloat
	isDuration
)

type t_metadata struct {
	create []string
	cast   map[int]int // position of field to cast to integers
}

var metadata = map[string]t_metadata{
	"circuits": t_metadata{
		create: []string{
			"CREATE TABLE circuits(circuitId INTEGER, circuitRef VARCHAR, name VARCHAR, location VARCHAR, country VARCHAR, lat FLOAT, lng FLOAT, alt INTEGER, url VARCHAR, PRIMARY KEY circuitId);",
		},
		cast: map[int]int{0: isInt, 5: isFloat, 6: isFloat, 7: isInt},
	},
	"constructors": t_metadata{
		create: []string{
			"CREATE TABLE constructors(constructorId INTEGER, constructorRef VARCHAR, name VARCHAR,nationality VARCHAR,url VARCHAR, PRIMARY KEY constructorId);",
		},
		cast: map[int]int{0: isInt},
	},
	"constructorResults": t_metadata{
		create: []string{
			"CREATE TABLE constructorResults(constructorResultsId INTEGER, raceId INTEGER, constructorId INTEGER, points FLOAT, status VARCHAR, PRIMARY KEY constructorResultsId)",
		},
		cast: map[int]int{0: isInt, 1: isInt, 2: isInt, 3: isFloat},
	},
	"constructorStandings": t_metadata{
		create: []string{
			"CREATE TABLE constructorStandings(constructorStandingsId INTEGER, raceId INTEGER, constructorId INTEGER, points FLOAT, position INTEGER, positionText VARCHAR, wins INTEGER, PRIMARY KEY constructorStandingsId)",
		},
		cast: map[int]int{0: isInt, 1: isInt, 2: isInt, 3: isFloat, 4: isInt, 6: isInt},
	},
	"drivers": t_metadata{
		create: []string{
			"CREATE TABLE drivers(driverId INTEGER, driverRef VARCHAR,number INTEGER, code VARCHAR[3], forename 	VARCHAR, surname VARCHAR, dob VARCHAR ,nationality VARCHAR, url VARCHAR, PRIMARY KEY driverId);",
		},
		cast: map[int]int{0: isInt, 2: isInt},
	},
	"driverStandings": t_metadata{
		create: []string{
			"CREATE TABLE driverStandings(driverStandingsId INTEGER, raceId INTEGER, driverId INTEGER, points FLOAT, position INTEGER, positionText VARCHAR, wins INTEGER, PRIMARY KEY driverStandingsId)",
		},
		cast: map[int]int{0: isInt, 1: isInt, 2: isInt, 3: isFloat, 4: isInt, 6: isInt},
	},
	"lapTimes": t_metadata{
		create: []string{
			"CREATE TABLE lapTimes(raceId INTEGER, driverId INTEGER, lap INTEGER, position INTEGER, time FLOAT, milliseconds INTEGER, PRIMARY KEY (raceId, driverId, lap))",
		},
		cast: map[int]int{0: isInt, 1: isInt, 2: isInt, 3: isInt, 4: isDuration, 5: isInt},
	},
	"pitStops": t_metadata{
		create: []string{
			"CREATE TABLE pitStops(raceId INTEGER, driverId INTEGER, stop INTEGER, lap INTEGER, time VARCHAR, duration FLOAT, milliseconds INTEGER, PRIMARY KEY (raceId, driverId, stop))",
		},
		cast: map[int]int{0: isInt, 1: isInt, 2: isInt, 3: isInt, 5: isDuration, 6: isInt},
	},
	"qualifying": t_metadata{
		create: []string{
			"CREATE TABLE qualifying(qualifyId INTEGER, raceId INTEGER, driverId INTEGER, constructorId INTEGER, number INTEGER, position INTEGER, q1 VARCHAR, q2 VARCHAR, q3 VARCHAR, PRIMARY KEY qualifyId)",
			"CREATE INDEX ON qualifying(constructorId)",
		},
		cast: map[int]int{0: isInt, 1: isInt, 2: isInt, 3: isInt, 4: isInt, 5: isInt},
	},
	"races": t_metadata{
		create: []string{
			"CREATE TABLE races(raceId INTEGER, year INTEGER, round INTEGER, circuitId INTEGER, name VARCHAR, datetime TIMESTAMP, url VARCHAR, PRIMARY KEY raceId)",
		},
		cast: map[int]int{0: isInt, 1: isInt, 2: isInt, 3: isInt, 5: isTimestamp},
	},
	"results": t_metadata{
		create: []string{
			"CREATE TABLE results(resultId INTEGER, raceId INTEGER, driverId INTEGER, constructorId INTEGER, number INTEGER, grid INTEGER, position INTEGER, positionText VARCHAR, positionOrder INTEGER, points FLOAT, laps INTEGER ,time VARCHAR, milliseconds INTEGER,fastestLap INTEGER, rank INTEGER, fastestLapTime VARCHAR, fastestLapSpeed FLOAT ,statusId INTEGER, PRIMARY KEY resultId)",
		},
		cast: map[int]int{0: isInt, 1: isInt, 2: isInt, 3: isInt, 4: isInt, 5: isInt, 6: isInt,
			8: isInt, 9: isFloat, 10: isInt, 12: isInt, 13: isInt, 14: isInt, 16: isFloat, 17: isInt},
	},
	"seasons": t_metadata{
		create: []string{
			"CREATE TABLE seasons(year INTEGER, url VARCHAR, PRIMARY KEY year)",
		},
		cast: map[int]int{0: isInt},
	},
	"status": t_metadata{
		create: []string{
			"CREATE TABLE status(statusId INTEGER, status VARCHAR, PRIMARY KEY statusId)",
		},
		cast: map[int]int{0: isInt},
	},
}

func str_clean(s string) string {
	s1 := strings.ToValidUTF8(s, string([]rune{unicode.ReplacementChar}))
	s2 := strings.ReplaceAll(s1, "%", "%%")
	//	s3 := strings.ReplaceAll(s2, "'", "''") // escape single quote by doubling them
	s3 := strings.ReplaceAll(s2, "'", "?") // escape single quote by doubling them
	return s3
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
func load_table(client immudb.ImmuClient, ctx context.Context, name string) {
	filename := fmt.Sprintf("CSV/%s.csv", name)
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Unable to open file %s: %s", filename, err.Error())
	}
	r := csv.NewReader(f)

	columns, err := r.Read()
	if err != nil {
		log.Fatalln(err)
	}
	column_string := strings.Join(columns, ",")
	
	// tx, err := client.NewTx(ctx)
	tx, err := client.NewTx(ctx, immudb.UnsafeMVCC(), immudb.SnapshotMustIncludeTxID(0), immudb.SnapshotRenewalPeriod(0))
	if err != nil {
		log.Fatalf("Load Table %s. Error while creating transaction: %s", name, err)
	}
	for _, q := range metadata[name].create {
		err = tx.SQLExec(ctx, q, nil)
		if err != nil {
			log.Fatalf("Load Table %s. Error in query %s while creating table: %s", q, name, err)
		}
	}
	tx_count := 1 // the create table is a valid instruction

	if name == "results" {
		tx.SQLExec(ctx, "CREATE INDEX ON results(driverId);CREATE INDEX ON results(statusId);", nil)
		tx_count++
	}

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalln(err)
		}
		value_string := valstring(name, record)
		qstring := fmt.Sprintf("INSERT INTO %s(%s) VALUES (%s)", name, column_string, value_string)
		//log.Printf(qstring)
		err = tx.SQLExec(ctx, qstring, nil)
		if err != nil {
			log.Fatalln(err)
		}

		tx_count = tx_count + 1
		if tx_count > 256 {
			_, err = tx.Commit(ctx)
			if err != nil {
				log.Fatalln(err)
			}
			tx, err = client.NewTx(ctx)
			if err != nil {
				log.Fatalln(err)
			}
			tx_count = 0
		}

	}

	if tx_count != 0 {
		_, err = tx.Commit(ctx)
		if err != nil {
			log.Fatalln(err)
		}
	}
}

var tabs = []string{
	"circuits",
	"constructors",
	"constructorResults",
	"constructorStandings",
	"driverStandings",
	"lapTimes",
	"pitStops",
	"qualifying",
	"drivers",
	"races",
	"results",
	"seasons",
	"status",
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	c := parseConfig()
	ic, ctx := connect(c)
	for _, t := range tabs {
		load_table(ic, ctx, t)
	}

}
