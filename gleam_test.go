package gleam

import (
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func init() {
	beam.Init()
}

func TestCreateList(t *testing.T) {
	p := beam.NewPipeline()
	s := p.Root()

	CreateList(s, []int{5, 6, 7, 8, 9}) // PCollection<int>
}

func TestCreate(t *testing.T) {
	p := beam.NewPipeline()
	s := p.Root()

	Create(s, 5, 6, 7, 8, 9)               // PCollection<int>
	Create(s, []int{5, 6}, []int{7, 8, 9}) // PCollection<[]int>
	Create(s, []int{5, 6, 7, 8, 9})        // PCollection<[]int>
	Create(s, "a", "b", "c")               // PCollection<string>
}

func TestExplode(t *testing.T) {
	p := beam.NewPipeline()
	s := p.Root()

	d := Create(s, []int{1, 2, 3, 4, 5}) // PCollection<[]int>
	Explode(s, d)                        // PCollection<int>
}

func TestFlaten(t *testing.T) {
	p := beam.NewPipeline()
	s := p.Root()

	a := CreateList(s, []int{5, 6})    // PCollection<int>
	b := CreateList(s, []int{7, 8, 9}) // PCollection<int>
	c := CreateList(s, []int{10, 11})  // PCollection<int>
	Flatten(s, a, b, c)                // PCollection<int>
}

func TestImpulse(t *testing.T) {
	p := beam.NewPipeline()
	s := p.Root()

	Impulse(s) // PCollection<[]byte>
}

func TestImpulseValue(t *testing.T) {
	p := beam.NewPipeline()
	s := p.Root()

	ImpulseValue(s, []byte{}) // PCollection<[]byte>
}
