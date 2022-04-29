package top

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/top"

	"github.com/kyleconroy/gleam"
)

func Smallest[T any](s beam.Scope, c gleam.PCollection[T], n int, fn func(a, b T) bool) gleam.PCollection[T] {
	return gleam.PCollection[T]{top.Smallest(s, c.Col, n, fn)}
}

func SmallestPerKey[K, V any](s beam.Scope, c gleam.PCollection[gleam.KV[K,V]], n int, fn func(a, b V) bool) gleam.PCollection[gleam.KV[K,[]V]] {
	return gleam.PCollection[gleam.KV[K,[]V]]{top.SmallestPerKey(s, c.Col, n, fn)}
}


func Largest[T any](s beam.Scope, c gleam.PCollection[T], n int, fn func(a, b T) bool) gleam.PCollection[T] {
	return gleam.PCollection[T]{top.Largest(s, c.Col, n, fn)}
}

func LargestPerKey[K, V any](s beam.Scope, c gleam.PCollection[gleam.KV[K,V]], n int, fn func(a, b V) bool) gleam.PCollection[gleam.KV[K,[]V]] {
	return gleam.PCollection[gleam.KV[K,[]V]]{top.LargestPerKey(s, c.Col, n, fn)}
}


