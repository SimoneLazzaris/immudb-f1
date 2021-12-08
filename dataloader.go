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

	var client immudb.ImmuClient
	var err error

	client = immudb.NewClient()
	err = client.OpenSession(ctx, []byte(config.Username), []byte(config.Password), config.DBName)
	if err != nil {
		log.Fatalln("Failed to connect. Reason:", err)
	}
	return client, ctx
}
type t_metadata struct {
	create string
	intcast []int // position of field to cast to integers
	tstamp []int  // position of field to cast to timestamp
}

var metadata = map[string]t_metadata{
	"circuits": t_metadata{
		create: "CREATE TABLE circuits(circuitId INTEGER, circuitRef VARCHAR, name VARCHAR, location VARCHAR, country VARCHAR, lat INTEGER, lng INTEGER ,alt INTEGER ,url VARCHAR, PRIMARY KEY circuitId);",
		intcast: []int{0, 5, 6, 7},
	},
	"drivers":  t_metadata{
		create: "CREATE TABLE drivers(driverId INTEGER, driverRef VARCHAR,number INTEGER, code VARCHAR[3], forename 	VARCHAR, surname VARCHAR, dob VARCHAR ,nationality VARCHAR, url VARCHAR, PRIMARY KEY driverId);",
		intcast: []int{0, 2},
	},
	"constructors": t_metadata{
		create: "CREATE TABLE constructors(constructorId INTEGER, constructorRef VARCHAR, name VARCHAR,nationality VARCHAR,url VARCHAR, PRIMARY KEY constructorId);",
		intcast: []int{0},
	},
	"races":  t_metadata{
		create: "CREATE TABLE races(raceId INTEGER, year INTEGER, round INTEGER, circuitId INTEGER, name VARCHAR, date VARCHAR, time VARCHAR, url VARCHAR, PRIMARY KEY raceId)",
		intcast: []int{0, 1, 2, 3},
	},
}

var create_stmt = map[string]string{
	"circuits": "CREATE TABLE circuits(circuitId INTEGER, circuitRef VARCHAR, name VARCHAR, location VARCHAR, country VARCHAR, lat INTEGER, lng INTEGER ,alt INTEGER ,url VARCHAR, PRIMARY KEY circuitId);",
	"drivers": "CREATE TABLE drivers(driverId INTEGER, driverRef VARCHAR,number INTEGER, code VARCHAR[3], forename VARCHAR, surname VARCHAR, dob VARCHAR ,nationality VARCHAR, url VARCHAR, PRIMARY KEY driverId);",
	"constructors": "CREATE TABLE constructors(constructorId INTEGER, constructorRef VARCHAR, name VARCHAR,nationality VARCHAR,url VARCHAR, PRIMARY KEY constructorId);",
	"races": "CREATE TABLE races(raceId INTEGER, year INTEGER, round INTEGER, circuitId INTEGER, name VARCHAR, date VARCHAR, time VARCHAR, url VARCHAR, PRIMARY KEY raceId)",
}
var intvalues = map[string][]int {
	"circuits": []int{0, 5, 6, 7},
	"drivers": []int{0, 2},
	"constructors": []int{0},
	"races": []int{0, 1, 2, 3},
}

func str_clean(s string) string {
	s1 := strings.ToValidUTF8(s,string([]rune{unicode.ReplacementChar}))
	s2 := strings.ReplaceAll(s1, "%", "%%")
	s3 := strings.ReplaceAll(s2, "'", ".")
	return s3
}

func valstring(name string, record []string) string {
	var t []string
	cvt := intvalues[name]
	for i,field := range record {
		to_int := false
		for _,v := range cvt {
			if i==v {
				to_int = true
				break
			}
		}
		if to_int {
			ii := 0.0
			if field != "" {
				var err error
				ii, err = strconv.ParseFloat(field, 64)
				if err != nil {
					log.Fatalf("Unable to convert field %s: %s", field, err.Error())
				}
			}
			t = append(t, strconv.Itoa(int(ii)))
		} else {
			t = append(t, fmt.Sprintf("'%s'",str_clean(field)))
		}
	}
	return strings.Join(t,",")
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

	tx, err := client.NewTx(ctx)
	if err != nil {
		log.Fatalf("Load Table %s. Error while creating transaction: %s", name, err)
	}
	err = tx.SQLExec(ctx, create_stmt[name], nil)
	if err != nil {
		log.Fatalf("Load Table %s. Error while creating table: %s", name, err)
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
		log.Printf(qstring)
		err = tx.SQLExec(ctx, qstring, nil)
		if err != nil {
			log.Fatalln(err)
		}

	}
	_, err = tx.Commit(ctx)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	c := parseConfig()
	ic, ctx := connect(c)
	//load_table(ic, ctx, "circuits")
	//load_table(ic, ctx, "drivers")
	//load_table(ic, ctx, "constructors")
	load_table(ic, ctx, "races")
}
