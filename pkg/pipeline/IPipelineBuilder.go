package pipeline

import "github.com/teambenny/goetl"

type IPipelineBuilder interface {
	Build() (goetl.PipelineIface, error)
}
