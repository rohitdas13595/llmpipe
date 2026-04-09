package interrupt

import "testing"

func TestNoInterrupt(t *testing.T) {
	var n NoInterrupt
	if n.ShouldInterrupt("any words", true) {
		t.Fatal("expected false")
	}
}

func TestMinWords(t *testing.T) {
	m := MinWords{N: 2}
	if m.ShouldInterrupt("one", true) {
		t.Fatal("one word should not interrupt")
	}
	if !m.ShouldInterrupt("one two", true) {
		t.Fatal("two words should interrupt when bot speaking")
	}
	if m.ShouldInterrupt("one two", false) {
		t.Fatal("should not interrupt when bot not speaking")
	}
}

func TestMinWordsDefaultN(t *testing.T) {
	m := MinWords{N: 0}
	if !m.ShouldInterrupt("x", true) {
		t.Fatal("N<=0 should behave as 1 word")
	}
}
