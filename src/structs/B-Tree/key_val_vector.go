package btree

type key_value_pair struct {
	key   string
	value MemtableValue
}

// Vector (array) containing keys and the values they represent.
// To be used in B-Tree nodes.
type kv_vector struct {
	data []key_value_pair
}

// Pushes a key-value pair to the end of a vector.
func (vec *kv_vector) PushBack(key string, value MemtableValue) {
	var item key_value_pair = key_value_pair{key: key, value: value}
	vec.data = append(vec.data, item)
}

// Inserts a key-value pair at the specified index, pushing other elements forward.
func (vec *kv_vector) Insert(index uint, key string, value MemtableValue) {
	vec.PushBack("", MemtableValue{})
	copy(vec.data[index+1:], vec.data[index:])
	vec.data[index] = key_value_pair{key: key, value: value}
}

// Deletes a key-value pair at the specified index.
func (vec *kv_vector) Delete(index uint) {
	var vector_size int = len(vec.data)
	copy(vec.data[index:], vec.data[index+1:])
	vec.data = vec.data[:vector_size-1]
}

func (vec *kv_vector) Size() int {
	return len(vec.data)
}

// Returns a pointer to the key-value pair at the specified index
func (vec *kv_vector) getPairRef(index int) *key_value_pair {
	return &vec.data[index]
}

func (vec *kv_vector) GetKeyAt(index int) string {
	return vec.getPairRef(index).key
}

func (vec *kv_vector) GetValueAt(index int) MemtableValue {
	return vec.getPairRef(index).value
}

func (vec *kv_vector) GetValueReferenceAt(index int) *MemtableValue {
	return &vec.getPairRef(index).value
}

func (vec *kv_vector) SetValueAt(index int, value MemtableValue) {
	vec.data[index].value = value
}

func (vec *kv_vector) Set(index int, key string, value MemtableValue) {
	vec.data[index].key = key
	vec.data[index].value = value
}

func (vec *kv_vector) Get(index int) (string, MemtableValue) {
	return vec.data[index].key, vec.data[index].value
}

// Returns the index that contains the specified key.
// If a key isn't present, returns -1
func (vec *kv_vector) FindKey(key string) int {
	for i := 0; i < vec.Size(); i++ {
		if vec.getPairRef(i).key == key {
			return i
		}
	}
	return -1
}
