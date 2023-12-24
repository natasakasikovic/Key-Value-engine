package memtable

type DataStructure interface {
	Insert(key string, value []byte)
	Delete(key string) //should be logical
	IsFull(capacity uint64) bool
	Find(key string) string //return value of the key
}

type Memtable struct {
	data     DataStructure
	capacity uint64
	keys     []string
}

func NewMemtable(data DataStructure, capacity uint64) *Memtable {
	if capacity == 0 {
		capacity = 20
	}

	return &Memtable{
		data:     data,
		capacity: capacity,
	}
}

func (memtable *Memtable) Delete(key string) {
	memtable.data.Delete(key)
}

func (memtable *Memtable) Get(key string) string {
	return memtable.data.Find(key)
}
