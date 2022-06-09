package ingens

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"github/suixinpr/ingens/base"
	"github/suixinpr/ingens/nodes"
	"sync/atomic"
)

var (
	// ErrLockEntryTimeout
	ErrLockEntryTimeout = errors.New("acquire lock timeout")

	// ErrRepeatedEntry Entry already exists and cannot be inserted repeatedly
	ErrRepeatedEntry = errors.New("entry already exists and cannot be inserted repeatedly.")

	// ErrNotFoundEntry entry does not exist.
	ErrNotFoundEntry = errors.New("entry does not exist.")

	// ErrDeadEntry
	ErrDeadEntry = errors.New("entry is dead")
)

// 操作

func (ing *Ingens) get(snapshot base.TransactionId, key []byte) ([]byte, error) {
	// search node
	node, _, err := ing.search(key)
	if err != nil {
		return nil, err
	}

	// 右移
	node, err = ing.moveRightForDown(node, key, false)
	if err != nil {
		return nil, err
	}

	// search
	off, found := node.BinarySearch(key)
	if !found {
		node.RUnlock()
		node.Release()
		return nil, ErrNotFoundEntry
	}

	// Search data entry in version chain
	de := node.GetDataEntry(off)
	if snapshot < de.Tid() {
		if de = ing.umgr.SearchInVersionChain(de, snapshot); de == nil {
			node.RUnlock()
			node.Release()
			return nil, ErrNotFoundEntry
		}
	}

	// dead
	if de.IsDead() {
		node.RUnlock()
		node.Release()
		return nil, ErrNotFoundEntry
	}

	// value
	value := make([]byte, de.ValueSize())
	copy(value, de.Value())

	// finish
	node.RUnlock()
	node.Release()
	return value, nil
}

func (ing *Ingens) setnx(tid base.TransactionId, key, value []byte) error {
	// lock entry
	if ok := ing.lmgr.Lock(key); ok {
		return ErrLockEntryTimeout
	} else {
		defer ing.lmgr.Unlock(key)
	}

	// search node
	node, stack, err := ing.search(key)
	if err != nil {
		return err
	}

	// 释放读锁，获取写锁
	node.RUnlock()
	node.Lock()

	// 右移
	node, err = ing.moveRightForDown(node, key, true)
	if err != nil {
		return err
	}

	// search
	off, found := node.BinarySearch(key)
	if !found {
		// date entry
		de := nodes.NewDataEntry(ing.mmgr, tid, key, value)
		defer ing.mmgr.Free(de)
		return ing.insertDataEntry(node, off, de, stack)
	} else {
		old := node.GetDataEntry(off)
		if old.IsDead() {
			// date entry
			de := nodes.NewDataEntry(ing.mmgr, tid, key, value)
			defer ing.mmgr.Free(de)
			return ing.updateDataEntry(node, off, de, stack)
		} else {
			node.Unlock()
			node.Release()
			return ErrRepeatedEntry
		}
	}
}

func (ing *Ingens) update(tid base.TransactionId, key, value []byte) error {
	// lock entry
	if ok := ing.lmgr.Lock(key); ok {
		return ErrLockEntryTimeout
	} else {
		defer ing.lmgr.Unlock(key)
	}

	// search node
	node, stack, err := ing.search(key)
	if err != nil {
		return err
	}

	// 释放读锁，获取写锁
	node.RUnlock()
	node.Lock()

	// 右移
	node, err = ing.moveRightForDown(node, key, true)
	if err != nil {
		return err
	}

	// search
	off, found := node.BinarySearch(key)
	if !found {
		node.Unlock()
		node.Release()
		return ErrNotFoundEntry
	}

	old := node.GetDataEntry(off)
	if old.IsDead() {
		return ErrDeadEntry
	}

	// date entry
	de := nodes.NewDataEntry(ing.mmgr, tid, key, value)
	defer ing.mmgr.Free(de)

	return ing.updateDataEntry(node, off, de, stack)
}

func (ing *Ingens) set(tid base.TransactionId, key, value []byte) error {
	// lock entry
	if ok := ing.lmgr.Lock(key); ok {
		return ErrLockEntryTimeout
	} else {
		defer ing.lmgr.Unlock(key)
	}

	// search node
	node, stack, err := ing.search(key)
	if err != nil {
		return err
	}

	// 释放读锁，获取写锁
	node.RUnlock()
	node.Lock()

	// 右移
	node, err = ing.moveRightForDown(node, key, true)
	if err != nil {
		return err
	}

	// date entry
	de := nodes.NewDataEntry(ing.mmgr, tid, key, value)
	defer ing.mmgr.Free(de)

	// search
	off, found := node.BinarySearch(key)
	if found {
		return ing.updateDataEntry(node, off, de, stack)
	} else {
		return ing.insertDataEntry(node, off, de, stack)
	}
}

func (ing *Ingens) delete(tid base.TransactionId, key []byte) error {
	// lock entry
	if ok := ing.lmgr.Lock(key); ok {
		return ErrLockEntryTimeout
	} else {
		defer ing.lmgr.Unlock(key)
	}

	node, _, err := ing.search(key)
	if err != nil {
		return err
	}

	// 释放读锁，获取写锁
	node.RUnlock()
	node.Lock()

	// 右移
	node, err = ing.moveRightForDown(node, key, true)
	if err != nil {
		return err
	}

	// search
	off, found := node.BinarySearch(key)
	if !found {
		node.Unlock()
		node.Release()
		return ErrNotFoundEntry
	}

	// 获取entry
	entry := node.GetDataEntry(off)
	if entry.IsDead() {
		node.Unlock()
		node.Release()
		return ErrDeadEntry
	}

	// 生成回滚记录
	undoRecPtr := ing.umgr.NewUndoRecordPtr(tid, entry)

	// update entry
	entry.UpdateUndoRecordPtr(undoRecPtr)
	entry.UpdateTid(tid)
	entry.MarkDead()

	// finish
	node.Unlock()
	node.Release()
	return nil
}

// 遍历

func (ing *Ingens) search(key []byte) (*nodes.Node, *list.List, error) {
	stack := list.New()
	node, err := ing.getRoot()
	if err != nil {
		return nil, nil, err
	}

	// 循环，下降
	for {
		node, err := ing.moveRightForDown(node, key, false)
		if err != nil {
			return nil, nil, err
		}

		if node.IsLeaf() {
			break
		}

		off, _ := node.BinarySearch(key)
		if node.IsRightmost() && off >= node.GetEndOff() {
			off -= nodes.EntryPtrSize
		}

		stack.PushBack(node.GetPageId())

		// non-leaf Node, entry is entryIndex
		node, err = ing.moveDown(node, off)
		if err != nil {
			return nil, nil, err
		}
	}

	// 返回叶子节点和栈，此时叶子节点持有读锁
	return node, stack, nil
}

// move right to right brother node
func (ing *Ingens) moveRightForDown(n *nodes.Node, key []byte, isWrite bool) (*nodes.Node, error) {
	for {
		// 如果是最右节点，停止右移
		if n.IsRightmost() {
			return n, nil
		}

		// 如果key值不大于页面的最大key，停止右移
		if bytes.Compare(key, n.GetHighKey()) <= 0 {
			return n, nil
		}

		// 获取右节点
		rp := n.GetRight()
		rn, err := ing.getNode(rp)
		if err != nil {
			if isWrite {
				n.Unlock()
			} else {
				n.RUnlock()
			}
			n.Release()
			return nil, err
		}

		// 释放前一节点锁，获取新节点锁
		if isWrite {
			n.Unlock()
			rn.Lock()
		} else {
			n.RUnlock()
			rn.RLock()
		}

		n.Release()
		n = rn
	}
}

// move right to right brother node
func (ing *Ingens) moveRightForUp(n *nodes.Node, pageId base.PageNumber) (*nodes.Node, error) {
	for {
		// 如果是最右节点，停止右移
		if n.IsRightmost() {
			return n, nil
		}

		// 如果找到对应的IndexEntry，停止右移
		if n.IsExistIndexEntry(pageId) {
			return n, nil
		}

		// 获取右节点
		rp := n.GetRight()
		rn, err := ing.getNode(rp)
		if err != nil {
			n.Unlock()
			n.Release()
			return nil, err
		}

		// 释放前一节点锁，获取新节点锁
		n.Unlock()
		rn.Lock()

		n.Release()
		n = rn
	}
}

// move down to child node
func (ing *Ingens) moveDown(n *nodes.Node, off base.OffsetNumber) (*nodes.Node, error) {
	entry := n.GetIndexEntry(off)
	pageId := entry.Value()
	cn, err := ing.getNode(pageId)
	if err != nil {
		n.RUnlock()
		n.Release()
		return nil, err
	}
	n.RUnlock()
	cn.RLock()

	n.Release()
	return cn, nil
}

// move up to parent node
// redirect index entry
func (ing *Ingens) moveUp(node *nodes.Node, pageId base.PageNumber, rpageId base.PageNumber, stack *list.List, elem *list.Element) (*nodes.Node, error) {
	// 获取父节点，3种情况
	// 1. 成功从栈中获取，非根节点
	// 2. 栈为空，当前节点为根节点，此时生成新的根节点
	// 3. 栈为空，但是此时已有其他线程创建了根节点，所以当前节点不为根节点
	// 这个时候通过levels获取上一层的最左侧节点
	var pnode *nodes.Node
	if elem == nil && ing.root == node.GetPageId() {
		// 不存在父节点，即当前节点为根节点，情况2

		// 创建根节点，并加写锁
		pnode, err := ing.newRoot(node.GetLevel() + 1)
		if err != nil {
			return nil, err
		}
		ie := nodes.NewIndexEntry(ing.mmgr, node.GetHighKey(), rpageId)
		defer ing.mmgr.Free(ie)

		pnode.Insert(pnode.GetEndOff(), ie)
	} else {
		// 已经存在父节点，情况3
		if elem == nil {
			elem = stack.PushFront(ing.levels[node.GetLevel()+1])
		}

		// 情况1
		ppageId := elem.Value.(base.PageNumber)
		pnode, err := ing.getNode(ppageId)
		if err != nil {
			return nil, err
		}
		pnode.Lock()

		// 右移
		pnode, err = ing.moveRightForUp(pnode, rpageId)
		if err != nil {
			return nil, err
		}

		// 将原本指向node的IndexEntry指向rnode
		err = pnode.RedirectEntry(pageId, rpageId)
		if err != nil {
			return nil, err
		}
	}
	return pnode, nil
}

// data entry

// insert data entry
func (ing *Ingens) insertDataEntry(node *nodes.Node, off base.OffsetNumber, entry nodes.DataEntry, stack *list.List) error {
	// 节点未满,直接插入
	if entry.Size() <= node.FreeSpaceSize()-nodes.EntryPtrSize {
		node.Insert(off, entry)
		node.Unlock()
		node.Release()
		return nil
	}

	// 节点已满则拆分节点
	rpageId, err := ing.splitNode(node, off, entry.Size(), entry, nodes.SPLIT_INSERT)
	if err != nil {
		return err
	}

	elem := stack.Back()
	pnode, err := ing.moveUp(node, node.GetPageId(), rpageId, stack, elem)

	ie := nodes.NewIndexEntry(ing.mmgr, node.GetHighKey(), node.GetPageId())
	defer ing.mmgr.Free(ie)

	return ing.insertIndexEntry(pnode, ie, stack, elem)
}

// update data entry
func (ing *Ingens) updateDataEntry(node *nodes.Node, off base.OffsetNumber, entry nodes.DataEntry, stack *list.List) error {
	// 节点未满,直接插入
	if entry.Size() <= node.FreeSpaceSize()-nodes.EntryPtrSize {
		node.Insert(off, entry)
		node.Unlock()
		node.Release()
		return nil
	}

	// 节点已满则拆分节点
	rpageId, err := ing.splitNode(node, off, entry.Size(), entry, nodes.SPLIT_UPDATE)
	if err != nil {
		return err
	}

	elem := stack.Back()
	pnode, err := ing.moveUp(node, node.GetPageId(), rpageId, stack, elem)

	ie := nodes.NewIndexEntry(ing.mmgr, node.GetHighKey(), node.GetPageId())
	defer ing.mmgr.Free(ie)

	return ing.insertIndexEntry(pnode, ie, stack, elem)
}

// insert index entry
func (ing *Ingens) insertIndexEntry(node *nodes.Node, entry nodes.IndexEntry, stack *list.List, elem *list.Element) error {
	off, found := node.BinarySearch(entry.Key())
	if found {
		return nil
	}

	// 节点未满,直接插入
	if entry.Size() <= node.FreeSpaceSize()-nodes.EntryPtrSize {
		node.Insert(off, entry)
		node.Unlock()
		node.Release()
		return nil
	}

	// 节点已满则拆分节点
	rpageId, err := ing.splitNode(node, off, entry.Size(), entry, nodes.SPLIT_INSERT)
	if err != nil {
		return err
	}

	pnode, err := ing.moveUp(node, node.GetPageId(), rpageId, stack, elem)

	ie := nodes.NewIndexEntry(ing.mmgr, node.GetHighKey(), node.GetPageId())
	defer ing.mmgr.Free(ie)

	return ing.insertIndexEntry(pnode, ie, stack, elem.Prev())
}

func (ing *Ingens) splitNode(node *nodes.Node, off base.OffsetNumber, size base.OffsetNumber, entry []byte, opr uint8) (base.PageNumber, error) {
	rnode, err := ing.newNode()
	if err != nil {
		return base.InvalidPageId, err
	}

	err = node.Split(rnode, off, size, entry, opr)
	if err != nil {
		return base.InvalidPageId, err
	}

	rnode.Release()
	return rnode.GetPageId(), nil
}

// node

// getNode
func (ing *Ingens) getNode(pageId base.PageNumber) (*nodes.Node, error) {
	n, err := ing.bmgr.GetBufferData(fmt.Sprintf("%v", pageId), false)
	if err != nil {
		return nil, err
	}
	return n.(*nodes.Node), nil
}

// 获取根节点，加读锁
func (ing *Ingens) getRoot() (*nodes.Node, error) {
	n, err := ing.getNode(ing.root)
	if err != nil {
		return nil, err
	}
	n.RLock()
	return n, nil
}

func (ing *Ingens) newNode() (*nodes.Node, error) {
	pageId := base.PageNumber(atomic.AddUint64((*uint64)(&ing.pageNum), 1))
	bd, err := ing.bmgr.GetBufferData(fmt.Sprintf("%v", pageId), true)
	if err != nil {
		return nil, err
	}

	return bd.(*nodes.Node), nil
}

func (ing *Ingens) newRoot(level uint16) (*nodes.Node, error) {
	pageId := base.PageNumber(atomic.AddUint64((*uint64)(&ing.pageNum), 1))
	n, err := ing.newNode()
	if err != nil {
		return nil, err
	}

	n.Init(pageId, level)
	return n, nil
}
