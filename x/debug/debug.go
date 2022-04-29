package debug

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"

	"github.com/kyleconroy/gleam"
)


func Head[T any](s beam.Scope, c gleam.PCollection[T], n int) gleam.PCollection[T] {
	return gleam.PCollection[T]{debug.Head(s, c.Col, n)}
}

func Print[T any](s beam.Scope, c gleam.PCollection[T]) gleam.PCollection[T] {
	return gleam.PCollection[T]{debug.Print(s, c.Col)}
}
