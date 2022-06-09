package nodes

import (
	"github/suixinpr/ingens/base"
	"io"
	"os"
)

type StorageManager struct {
	file os.File
}

// io 操作，从文件读取页面
func (smgr *StorageManager) Read(data []byte, pageId uint64) error {
	// 读取数据
	off := int64(pageId) * int64(base.PageSize)
	n, err := smgr.file.ReadAt(data, off)

	// 读取失败
	if err != nil {
		return err
	}

	// 读取数据长度不对
	if n != base.PageSize {
		return io.ErrUnexpectedEOF
	}

	return nil
}

// io 操作，将页面写入文件
func (smgr *StorageManager) Write(data []byte, pageId uint64) error {
	// 写入数据
	off := int64(pageId) * int64(base.PageSize)
	n, err := smgr.file.WriteAt(data, off)

	// 写入失败
	if err != nil {
		return err
	}

	// 写入数据长度不对
	if n != base.PageSize {
		return io.ErrShortWrite
	}

	return nil
}
