package filter

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/filter"

	"github.com/kyleconroy/gleam"
)

func Distinct[T any](s beam.Scope, c gleam.PCollection[T]) gleam.PCollection[T] {
	return gleam.PCollection[T]{filter.Distinct(s, c.Col)}
}

func Exclude[T any](s beam.Scope, c gleam.PCollection[T], fn func(T) bool) gleam.PCollection[T] {
	return gleam.PCollection[T]{filter.Exclude(s, c.Col, fn)}
}

func Include[T any](s beam.Scope, c gleam.PCollection[T], fn func(T) bool) gleam.PCollection[T] {
	return gleam.PCollection[T]{filter.Include(s, c.Col, fn)}
}
