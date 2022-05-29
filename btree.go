package ingens

import (
	"bytes"
	"container/list"
	"errors"
	"github/suixinpr/ingens/base"
	"github/suixinpr/ingens/bufnode"
	"sync/atomic"
)

const (
	levelSize = 16
)

var (
	ErrLockEntryTimeout = errors.New("acquire lock timeout")
)

// 操作

type btree struct {
	ing *Ingens

	root    base.PageNumber
	pageNum base.PageNumber

	levelNum uint64
	levels   []base.PageNumber
}

func (bt *btree) get(key []byte) ([]byte, error) {
	node, _, err := bt.search(key)
	if err != nil {
		return nil, err
	}

	return bt.scan(node, key)
}

func (bt *btree) setnx(key, value []byte) error {
	// lock entry
	if ok := bt.ing.resManager.LockEntry(key); ok {
		return ErrLockEntryTimeout
	} else {
		defer bt.ing.resManager.UnlockEntry(key)
	}

	de := bufnode.FormDataEntry(bt.ing.memManager, key, value)
	return bt.insertDataEntry(de)
}

func (bt *btree) delete(key []byte) error {
	return nil
}

// 遍历

// 扫描同一层获取key值对应的value
func (bt *btree) scan(node *bufnode.Node, key []byte) ([]byte, error) {
	node, err := bt.moveRightForDown(node, key, false)
	if err != nil {
		return nil, err
	}

	off, found := node.BinarySearch(key)
	if !found {
		return nil, nil
	}

	entry := node.GetDataEntry(off)
	value := entry.Value()

	result := make([]byte, len(value))
	copy(result, value)

	node.RUnlock()
	return result, nil
}

func (bt *btree) search(key []byte) (*Node, *list.List, error) {
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
			off -= bufnode.EntryPtrSize
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
func (bt *btree) moveRightForDown(n *bufnode.Node, key []byte, isWrite bool) (*bufnode.Node, error) {
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
func (bt *btree) moveRightForUp(n *Node, pageId base.PageNumber) (*Node, error) {
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
func (bt *btree) moveDown(n *Node, off OffsetNumber) (*Node, error) {
	entry := n.GetEntry(off)
	pageId := entry.(*IndexEntry).Value()
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

// move down to parent node
func (bt *btree) MoveUp() {}

// node

// getNode
func (bt *btree) getNode(pageId base.PageNumber) (*Node, error) {
	return bt.bufPool.GetNode(pageId, nil, bt.ing.file)
}

// 获取根节点，加读锁
func (bt *btree) getRoot() (*Node, error) {
	n, err := bt.getNode(bt.root)
	if err != nil {
		return nil, err
	}
	n.RLock()
	return n, nil
}

func (bt *btree) newNode(pageId base.PageNumber, page *Page) (*Node, error) {
	return bt.bufPool.GetNode(pageId, page, nil)
}

func (bt *btree) newRoot(level uint64) (*Node, error) {
	pageId := base.PageNumber(atomic.AddUint64((*uint64)(&bt.pageNum), 1))
	page := EmptryPage(pageId, level)

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

// insert

// insert data entry
func (bt *btree) insertDataEntry(entry bufnode.DataEntry) error {
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

	// lock entry

	// 节点未满,直接插入
	if entry.Size() <= node.FreeSpaceSize()-EntryPtrSize {
		err := node.InsertEntry(entry)
		node.Unlock()
		node.Release()
		return err
	}

	return bt.insertIntoNode(node, entry, stack, stack.Back())
}

// insert index entry
func (bt *btree) insertIndexEntry(entry IndexEntry) error {

}

// node 正确的被插入节点，不再需要右移
// entry 插入的数据
// elem 该node在父亲节点中的位置
func (bt *btree) insertIntoNode(node *Node, entry Entry, stack *list.List, elem *list.Element) error {
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

// 拆分节点
func (bt *btree) split(node *Node, entry Entry) (base.PageNumber, error) {
	pageId := base.PageNumber(atomic.AddUint64((*uint64)(&bt.pageNum), 1))
	rpage, err := node.Split(pageId, entry)
	if err != nil {
		return InvalidPageId, err
	}

	err = WritePage(bt.ing.file, rpage)
	if err != nil {
		return InvalidPageId, err
	}

	return pageId, nil
}
