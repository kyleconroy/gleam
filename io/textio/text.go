package textio

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"

	"github.com/kyleconroy/gleam"
	)

func Immediate(s beam.Scope, filename string) (gleam.PCollection[string], error) {
	c, err := textio.Immediate(s, filename)
	return gleam.PCollection[string]{c}, err
}

func Read(s beam.Scope, path string) gleam.PCollection[string] {
	return gleam.PCollection[string]{textio.Read(s, path)}
}

func ReadAll(s beam.Scope, c gleam.PCollection[string]) gleam.PCollection[string] {
	return gleam.PCollection[string]{textio.ReadAll(s, c.Col)}
}


func ReadAllSdf(s beam.Scope, c gleam.PCollection[string]) gleam.PCollection[string] {
	return gleam.PCollection[string]{textio.ReadAllSdf(s, c.Col)}
}

func ReadSdf(s beam.Scope, glob string) gleam.PCollection[string] {
	return gleam.PCollection[string]{textio.ReadSdf(s, glob)}
}


func Write(s beam.Scope, path string, c gleam.PCollection[string]) {
	textio.Write(s, path, c.Col)
}
