package ingens

func (ing *Ingens) SetNx(key, value []byte) (err error) {
	if ing.isClosed() {
		return ErrDatabaseIsClosed
	}

	return ing.btree.setnx(key, value)
}

func (ing *Ingens) Get(key []byte) ([]byte, error) {
	if ing.isClosed() {
		return nil, ErrDatabaseIsClosed
	}

	return ing.btree.get(key)
}
