package iterators

import "github.com/natasakasikovic/Key-Value-engine/src/model"

type Iterator interface {
	Next() (*model.Record, error)
	Stop()
}
