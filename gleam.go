package gleam

import (
	"context"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

type KV[K any, V any] struct {
	Key K
	Val V
}

type DoFn[T, E any] interface {
	ProcessElement(context.Context, T, func(E))
}

type PCollection[T any] struct {
	Col beam.PCollection
}

func ParDo[T, E any](s beam.Scope, fn func(T) E, c PCollection[T]) PCollection[E] {
	return PCollection[E]{beam.ParDo(s, fn, c.Col)}
}

func ParDoFn[T, E any](s beam.Scope, fn DoFn[T, E], c PCollection[T]) PCollection[E] {
	return PCollection[E]{beam.ParDo(s, fn, c.Col)}
}

func Emit[T, E any](s beam.Scope, fn func(T, func(E)), c PCollection[T]) PCollection[E] {
	return PCollection[E]{beam.ParDo(s, fn, c.Col)}
}

func ParDoPerKey[K, V, E any](s beam.Scope, fn func(K, V) E, c PCollection[KV[K, V]]) PCollection[E] {
	return PCollection[E]{beam.ParDo(s, fn, c.Col)}
}

func Create[T any](s beam.Scope, values ...T) PCollection[T] {
	vals := make([]interface{}, len(values))
	for i, v := range values {
		vals[i] = v
	}
	return PCollection[T]{beam.Create(s, vals...)}
}

func CreateList[T any](s beam.Scope, values []T) PCollection[T] {
	return PCollection[T]{beam.CreateList(s, values)}
}

func DropKey[A, B any](s beam.Scope, c PCollection[KV[A, B]]) PCollection[B] {
	return PCollection[B]{beam.DropKey(s, c.Col)}
}

func DropValue[A, B any](s beam.Scope, c PCollection[KV[A, B]]) PCollection[A] {
	return PCollection[A]{beam.DropValue(s, c.Col)}
}

func Explode[T any](s beam.Scope, c PCollection[[]T]) PCollection[T] {
	return PCollection[T]{beam.Explode(s, c.Col)}
}

func Flatten[T any](s beam.Scope, cols ...PCollection[T]) PCollection[T] {
	var values []beam.PCollection
	for i, _ := range cols {
		values = append(values, cols[i].Col)
	}
	return PCollection[T]{beam.Flatten(s, values...)}
}

func Impulse(s beam.Scope) PCollection[[]byte] {
	return PCollection[[]byte]{beam.Impulse(s)}
}

func ImpulseValue(s beam.Scope, v []byte) PCollection[[]byte] {
	return PCollection[[]byte]{beam.ImpulseValue(s, v)}
}

func Partition[T any](s beam.Scope, n int, fn func(T) int, c PCollection[T]) []PCollection[T] {
	cols := beam.Partition(s, n, fn, c.Col)
	out := make([]PCollection[T], len(cols))
	for i, col := range cols {
		out[i] = PCollection[T]{col}
	}
	return out
}

func Reshuffle[T any](s beam.Scope, c PCollection[T]) PCollection[T] {
	return PCollection[T]{beam.Reshuffle(s, c.Col)}
}

func SwapKV[A, B any](s beam.Scope, c PCollection[KV[A, B]]) PCollection[KV[B, A]] {
	return PCollection[KV[B, A]]{beam.SwapKV(s, c.Col)}
}
