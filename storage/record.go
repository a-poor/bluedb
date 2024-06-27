package storage

const RecordIDKey = "_id"

type Record struct {
	Key   string         `json:"key"`
	Tomb  bool           `json:"tomb,omitempty"`
	Value map[string]any `json:"value,omitempty"`
}

func NewRecord(did uint, value map[string]any) (Record, error) {
	// Generate an id...
	id, err := NewID(did)
	if err != nil {
		return Record{}, err
	}

	// If value isn't initialized, initialize it
	v := value
	if v == nil {
		v = make(map[string]any)
	}

	// Set the record ID in the value
	v[RecordIDKey] = id

	// Create and return the value
	return Record{
		Key:   id,
		Value: v,
	}, nil
}
