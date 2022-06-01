package ingens

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"github/suixinpr/ingens/base"
	"github/suixinpr/ingens/btnode"
	"github/suixinpr/ingens/transaction/undo"
	"sync/atomic"
)

const (
	levelSize = 16
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

type btree struct {
	ing *Ingens

	root    base.PageNumber
	pageNum base.PageNumber

	levelNum uint64
	levels   []base.PageNumber
}

// operate

func (bt *btree) get(snapshot base.TransactionId, key []byte) ([]byte, error) {
	node, _, err := bt.search(key)
	if err != nil {
		return nil, err
	}

	return bt.scan(node, snapshot, key)
}

func (bt *btree) setnx(key, value []byte) error {
	// lock entry
	if ok := bt.ing.resManager.LockEntry(key); ok {
		return ErrLockEntryTimeout
	} else {
		defer bt.ing.resManager.UnlockEntry(key)
	}

	de := btnode.FormDataEntry(bt.ing.memManager, key, value)
	return bt.insertDataEntryIntoNode(de)
}

func (bt *btree) delete(tid base.TransactionId, key []byte) error {
	// lock entry
	if ok := bt.ing.resManager.LockEntry(key); ok {
		return ErrLockEntryTimeout
	} else {
		defer bt.ing.resManager.UnlockEntry(key)
	}
	return bt.deleteDataEntry(tid, key)
}

// 遍历

// 扫描同一层获取key值对应的value
func (bt *btree) scan(node *btnode.Node, snapshot base.TransactionId, key []byte) ([]byte, error) {
	node, err := bt.moveRightForDown(node, key, false)
	if err != nil {
		return nil, err
	}

	off, found := node.BinarySearch(key)
	if !found {
		node.Unlock()
		node.Release()
		return nil, ErrNotFoundEntry
	}

	entry := node.GetDataEntry(off)
	if entry.IsDead() {
		node.Unlock()
		node.Release()
		return nil, ErrDeadEntry
	}

	value := entry.Value()
	result := make([]byte, len(value))
	copy(result, value)

	node.RUnlock()
	node.Release()
	return result, nil
}

func (bt *btree) search(key []byte) (*btnode.Node, *list.List, error) {
	stack := list.New()
	node, err := bt.getRoot()
	if err != nil {
		return nil, nil, err
	}

	// 循环，下降
	for {
		node, err := bt.moveRightForDown(node, key, false)
		if err != nil {
			return nil, nil, err
		}

		if node.IsLeaf() {
			break
		}

		off, _ := node.BinarySearch(key)
		if node.IsRightmost() && off >= node.GetEndOff() {
			off -= btnode.EntryPtrSize
		}

		stack.PushBack(node.GetPageId())

		// non-leaf Node, entry is entryIndex
		node, err = bt.moveDown(node, off)
		if err != nil {
			return nil, nil, err
		}
	}

	// 返回叶子节点和栈，此时叶子节点持有读锁
	return node, stack, nil
}

// move right to right brother node
func (bt *btree) moveRightForDown(n *btnode.Node, key []byte, isWrite bool) (*btnode.Node, error) {
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
		rn, err := bt.getNode(rp)
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
func (bt *btree) moveRightForUp(n *btnode.Node, pageId base.PageNumber) (*Node, error) {
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
		rn, err := bt.getNode(rp)
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
func (bt *btree) moveDown(n *btnode.Node, off base.OffsetNumber) (*btnode.Node, error) {
	entry := n.GetIndexEntry(off)
	pageId := entry.Value()
	cn, err := bt.getNode(pageId)
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
func (bt *btree) MoveUp(node *btnode.Node, entry btnode.IndexEntry, stack *list.List, elem *list.Element) (*btnode.Node, error) {
	// 获取父节点，3种情况
	// 1. 成功从栈中获取，非根节点
	// 2. 栈为空，当前节点为根节点，此时生成新的根节点
	// 3. 栈为空，但是此时已有其他线程创建了根节点，所以当前节点不为根节点
	// 这个时候通过levels获取上一层的最左侧节点
	var pnode *btnode.Node
	if elem == nil && bt.root == node.GetPageId() {
		// 不存在父节点，即当前节点为根节点，情况2

		// 创建根节点，并加写锁
		pnode, err := bt.newRoot(node.GetLevel() + 1)
		if err != nil {
			return nil, err
		}
		pnode.Insert(pnode.GetEndOff(), entry)
	} else {
		// 已经存在父节点，情况3
		if elem == nil {
			elem = stack.PushFront(bt.levels[node.GetLevel()+1])
		}

		// 情况1
		ppageId := elem.Value.(base.PageNumber)
		pnode, err := bt.getNode(ppageId)
		if err != nil {
			return nil, err
		}
		pnode.Lock()

		// 右移
		pnode, err = bt.moveRightForUp(pnode, rpageId)
		if err != nil {
			return nil, err
		}

		// 将原本指向node的IndexEntry指向rnode
		pnode.RedirectEntry(node.GetPageId(), rpageId)
	}
	return pnode, nil
}

// insert

// insert data entry
func (bt *btree) insertDataEntryIntoNode(entry btnode.DataEntry) error {
	node, stack, err := bt.search(entry.Key())
	if err != nil {
		return err
	}

	// 释放读锁，获取写锁
	node.RUnlock()
	node.Lock()

	// 右移
	node, err = bt.moveRightForDown(node, entry.Key(), true)
	if err != nil {
		return err
	}

	// search
	off, found := node.BinarySearch(entry.Key())
	if found {
		e := node.GetDataEntry(off)
		if e.IsDead() {
			// undoRecordPtr := FromUndoRecord
			// entry.SetUndoRecord(undoRecordPtr)
			if entry.Size() > e.Size() {
				node.Update(off, entry)
				return nil
			}
			node.Delete(off)
		} else {
			node.Unlock()
			node.Release()
			return ErrRepeatedEntry
		}
	}

	// 节点未满,直接插入
	if entry.Size() <= node.FreeSpaceSize()-btnode.EntryPtrSize {
		node.Insert(off, entry)
		node.Unlock()
		node.Release()
		return nil
	}

	// 节点已满则拆分节点
	rpageId, err := bt.splitLeafNode(node, entry)
	if err != nil {
		return err
	}
	ie = btnode.FormIndexEntry(bt.ing.memManager, node.GetHighKey(), node.GetPageId())

	elem := stack.Back()
	pnode, err := bt.MoveUp(node, nil, stack, elem)
	return bt.insertIndexEntryIntoNode(pnode, ie, stack, elem)
}

// insert index entry
func (bt *btree) insertIndexEntryIntoNode(node *btnode.Node, entry btnode.IndexEntry, stack *list.List, elem *list.Element) error {
	// 节点未满,直接插入
	if entry.Size() <= node.FreeSpaceSize()-EntryPtrSize {
		err := node.InsertEntry(entry)
		node.Unlock()
		node.Release()
		return err
	}

	// 在拆分时会将entry插入，并生成rnode，
	// 前往父节点中修改原本指向node的IndexEntry，使其指向rnode，
	// 生成node的IndexEntry
	// 我们还需要将node的IndexEntry插入父节点
	// 通过递归调用实现
	// rnode未持有锁
	rpageId, err := bt.split(node, entry)
	if err != nil {
		return err
	}
	entry = FormIndexEntry(node.GetHighKey(), node.GetPageId())

	// 获取父节点，3种情况
	// 1. 成功从栈中获取，非根节点
	// 2. 栈为空，当前节点为根节点，此时生成新的根节点
	// 3. 栈为空，但是此时已有其他线程创建了根节点，所以当前节点不为根节点
	// 这个时候通过levels获取上一层的最左侧节点
	var pnode *Node
	if elem == nil && bt.root == node.GetPageId() {
		// 不存在父节点，即当前节点为根节点，情况2

		// 创建根节点
		pnode, err = bt.newRoot(node.GetLevel() + 1)
		if err != nil {
			return err
		}
		pnode.InsertEntry(entry)
	} else {
		// 已经存在父节点，情况3
		if elem == nil {
			elem = stack.PushFront(bt.levels[node.GetLevel()+1])
		}

		// 情况1
		ppageId := elem.Value.(base.PageNumber)
		pnode, err := bt.getNode(ppageId)
		if err != nil {
			return err
		}
		pnode.Lock()

		// 右移
		pnode, err = bt.moveRightForUp(pnode, rpageId)
		if err != nil {
			return err
		}

		// 将原本指向node的IndexEntry指向rnode
		pnode.RedirectEntry(node.GetPageId(), rpageId)
	}

	node.Unlock()
	node.Release()
	// 将指向node的新的entry插入父节点
	return bt.insertIntoNode(pnode, entry, stack, elem.Prev())
}

func (bt *btree) deleteDataEntry(tid base.TransactionId, key []byte) error {
	node, _, err := bt.search(key)
	if err != nil {
		return err
	}

	// 释放读锁，获取写锁
	node.RUnlock()
	node.Lock()

	// 右移
	node, err = bt.moveRightForDown(node, key, true)
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
	undoRecPtr := undo.FromUndoRecord()

	// update entry
	entry.UpdateUndoRecordPtr(undoRecPtr)
	entry.UpdateTid(tid)
	entry.MarkDead()

	node.Unlock()
	node.Release()
	return nil
}

// 拆分节点
func (bt *btree) splitLeafNode(node *btnode.Node, entry btnode.DataEntry) (base.PageNumber, error) {
	pageId := base.PageNumber(atomic.AddUint64((*uint64)(&bt.pageNum), 1))
	rpage, err := node.Split(pageId, entry)
	if err != nil {
		return base.InvalidPageId, err
	}

	err = WritePage(bt.ing.file, rpage)
	if err != nil {
		return base.InvalidPageId, err
	}

	return pageId, nil
}

// node

// getNode
func (bt *btree) getNode(pageId base.PageNumber) (*btnode.Node, error) {
	tag := btnode.NodeTag{PageId: pageId}
	n, err := bt.ing.bufManager.GetBufferData(fmt.Sprintf("%v", tag), false)
	if err != nil {
		return nil, err
	}
	return n.(*btnode.Node), nil
}

// 获取根节点，加读锁
func (bt *btree) getRoot() (*btnode.Node, error) {
	n, err := bt.getNode(bt.root)
	if err != nil {
		return nil, err
	}
	n.RLock()
	return n, nil
}

func (bt *btree) newNode(pageId base.PageNumber) (*btnode.Node, error) {
	tag := btnode.NodeTag{PageId: pageId}
	n, err := bt.ing.bufManager.GetBufferData(fmt.Sprintf("%v", tag), true)
	if err != nil {
		return nil, err
	}
	return n.(*btnode.Node), nil
}

func (bt *btree) newRoot(level uint16) (*btnode.Node, error) {
	pageId := base.PageNumber(atomic.AddUint64((*uint64)(&bt.pageNum), 1))
	page := btnode.EmptryPage(pageId, level)

	err := WritePage(bt.ing.file, page)
	if err != nil {
		return nil, err
	}

	node, err := bt.newNode(pageId, page)
	if err != nil {
		return nil, err
	}

	return node, err
}
