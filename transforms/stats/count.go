package stats

import (
	"golang.org/x/exp/constraints"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"

	"github.com/kyleconroy/gleam"
)

type Numeric interface {
	constraints.Integer | constraints.Float
}

func ApproximateQuantiles[T any](s beam.Scope, c gleam.PCollection[T], less func(a, b T) bool, opt stats.Opts) gleam.PCollection[T] {
	return gleam.PCollection[T]{stats.ApproximateQuantiles(s, c.Col, less, opt)}
}

func ApproximateWeightedQuantiles[T any](s beam.Scope, c gleam.PCollection[T], less func(a, b T) bool, opt stats.Opts) gleam.PCollection[T] {
	return gleam.PCollection[T]{stats.ApproximateWeightedQuantiles(s, c.Col, less, opt)}
}

func Count[T any](s beam.Scope, c gleam.PCollection[T]) gleam.PCollection[gleam.KV[T, int]] {
	return gleam.PCollection[gleam.KV[T, int]]{stats.Count(s, c.Col)}
}

func CountElms[T any](s beam.Scope, c gleam.PCollection[T]) gleam.PCollection[int] {
	return gleam.PCollection[int]{stats.CountElms(s, c.Col)}
}

func Max[T Numeric](s beam.Scope, c gleam.PCollection[T]) gleam.PCollection[T] {
	return gleam.PCollection[T]{stats.Max(s, c.Col)}
}

func MaxPerKey[A Numeric, B any](s beam.Scope, c gleam.PCollection[gleam.KV[A,B]]) gleam.PCollection[gleam.KV[A,B]] {
	return gleam.PCollection[gleam.KV[A,B]]{stats.MaxPerKey(s, c.Col)}
}

func Mean[T Numeric](s beam.Scope, c gleam.PCollection[T]) gleam.PCollection[T] {
	return gleam.PCollection[T]{stats.Mean(s, c.Col)}
}

func MeanPerKey[A Numeric, B any](s beam.Scope, c gleam.PCollection[gleam.KV[A,B]]) gleam.PCollection[gleam.KV[A,B]] {
	return gleam.PCollection[gleam.KV[A,B]]{stats.MeanPerKey(s, c.Col)}
}


func Min[T Numeric](s beam.Scope, c gleam.PCollection[T]) gleam.PCollection[T] {
	return gleam.PCollection[T]{stats.Min(s, c.Col)}
}

func MinPerKey[A Numeric, B any](s beam.Scope, c gleam.PCollection[gleam.KV[A,B]]) gleam.PCollection[gleam.KV[A,B]] {
	return gleam.PCollection[gleam.KV[A,B]]{stats.MinPerKey(s, c.Col)}
}

func Sum[T Numeric](s beam.Scope, c gleam.PCollection[T]) gleam.PCollection[T] {
	return gleam.PCollection[T]{stats.Sum(s, c.Col)}
}

func SumPerKey[A Numeric, B any](s beam.Scope, c gleam.PCollection[gleam.KV[A,B]]) gleam.PCollection[gleam.KV[A,B]] {
	return gleam.PCollection[gleam.KV[A,B]]{stats.SumPerKey(s, c.Col)}
}



