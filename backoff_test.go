package autotx

import (
	"testing"
	"time"
)

func TestSimpleExponentialBackOff(t *testing.T) {
	b := newSimpleExponentialBackOff()
	schedule := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		40 * time.Millisecond,
		80 * time.Millisecond,
		160 * time.Millisecond,
		320 * time.Millisecond,
		640 * time.Millisecond,
		1 * time.Second,
		1 * time.Second,
		1 * time.Second,
	}
	for i, expected := range schedule {
		if actual := b.NextBackOff(); actual != expected {
			t.Errorf("Attempt %d should be %v but was %v", i+1, expected, actual)
		}
	}
}
