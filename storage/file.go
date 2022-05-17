package storage

import (
	"hash/fnv"
	"io"
	"os"
)

// 打开文件，如果不存在则创建文件
func Open(path string, name string) (*os.File, error) {
	// 检查目录是否存在，如果不存在则创建目录
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 检查文件是否存在，如果不存在则创建文件
	file, err := os.OpenFile(path+name, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	return file, err
}

// 删除文件
func Remove(file *os.File) error {
	if err := file.Close(); err != nil {
		return err
	}
	return os.Remove(file.Name())
}

func Read(file *os.File, offset int64, size int) ([]byte, error) {
	buf := make([]byte, size)
	n, err := file.ReadAt(buf, offset)

	// 读取失败
	if err != nil {
		return nil, err
	}

	// 读取数据长度不对
	if n != size {
		return nil, io.ErrUnexpectedEOF
	}

	return buf, nil
}

func Write(file *os.File, offset int64, buf []byte) error {
	n, err := file.WriteAt(buf, offset)

	// 写入失败
	if err != nil {
		return err
	}

	// 写入数据长度不对
	if n != len(buf) {
		return io.ErrShortWrite
	}

	return nil
}

func Sum64(buf []byte) uint64 {
	f := fnv.New64()
	f.Write(buf)
	return f.Sum64()
}
