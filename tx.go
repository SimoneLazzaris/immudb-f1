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
	"log"
	"sync/atomic"
	"time"

	immudb "github.com/codenotary/immudb/pkg/client"
)

var collisions uint64

func init() {
	collisions = 0
}

type t_tx struct {
	name       string
	statements []string
	tx         immudb.Tx
	ctx        context.Context
	ic         immudb.ImmuClient
	txsize     int
	total      int
}

func MakeTx(ctx context.Context, client immudb.ImmuClient, name string, txsize int) *t_tx {
	return &t_tx{
		name:  name,
		tx:    nil,
		ctx:   ctx,
		ic:    client,
		txsize: txsize,
		total: 0,
	}
}

func (t *t_tx) Add(stm string) {
	t.statements = append(t.statements, stm)
	if len(t.statements) >= 256 {
		t.Commit()
	}
}

func (t *t_tx) Commit() {
	if len(t.statements) == 0 {
		return
	}
	var err error
	for i := 0; i < 5; i++ {
		t.tx, err = t.ic.NewTx(t.ctx)
		// t.tx, err = t.ic.NewTx(t.ctx, immudb.UnsafeMVCC(), immudb.SnapshotMustIncludeTxID(0), immudb.SnapshotRenewalPeriod(0))
		if err != nil {
			log.Fatalf("Load Table %s. Error while creating transaction: %s", t.name, err)
		}
		for _, s := range t.statements {
			t.tx.SQLExec(t.ctx, s, nil)
		}
		log.Printf("Committing %d [%d] in table %s", len(t.statements), t.total, t.name)
		_, err := t.tx.Commit(t.ctx)
		if err == nil {
			break
		}
		atomic.AddUint64(&collisions, 1)
		log.Printf("Tx Error in table %s (%s), retrying", t.name, err)
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		log.Fatal("TX error in table %s (%s), abort", t.name, err)
	}
	t.total = t.total + len(t.statements)
	t.statements = []string{}
}
