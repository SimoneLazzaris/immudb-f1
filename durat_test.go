package main

import (
	"testing"
)

func TestStrToDuration(t *testing.T) {
	if strToDuration("1:01:38.698") != 3698.698 {
		t.Errorf("AARGH")
	}
	if strToDuration("1:32.342") != 92.342 {
		t.Errorf("AARGH")
	}
	if strToDuration("1.23") != 1.23 {
		t.Errorf("AARGH")
	}

}
