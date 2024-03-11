package merkledag

import (
	"encoding/json"
	"hash"
)

type Link struct {
	Name string
	Hash []byte
	Size int
}

type Object struct {
	Links []Link
	Data  []byte
}

func Add(store KVStore, node Node, h hash.Hash) []byte {
	// TODO 将分片写入到KVStore中，并返回Merkle Root
	switch node.Type() {
	case FILE:
		file := node.(File)
		dealFile(store, file, h)
	case DIR:
		dir := node.(Dir)
		dealDir(store, dir, h)
	}
	return h.Sum(nil)
}

func dealFile(store KVStore, node File, h hash.Hash) *Object {
	if node.Size() <= 256 {
		data := node.Bytes()
		blob := Object{
			Links: nil,
			Data:  data,
		}
		jsonMarshal, _ := json.Marshal(blob)
		h.Write(jsonMarshal)
		store.Put(h.Sum(nil), data)
		return &blob
	} else {
		res := dealFile_(node, store, h)
		return res
	}

}

func dealDir(store KVStore, node Dir, h hash.Hash) *Object {
	iter := node.It()
	treeObject := &Object{}
	for iter.Next() {
		node := iter.Node()
		if node.Type() == FILE {
			//是文件
			file := node.(File)
			tmp := dealFile(store, file, h)
			jsonMarshal, _ := json.Marshal(tmp)
			h.Write(jsonMarshal)
			treeObject.Links = append(treeObject.Links, Link{
				Hash: h.Sum(nil),
				Size: int(file.Size()),
				Name: file.Name(),
			})
			typeName := "link"
			if tmp.Links == nil {
				typeName = "blob"
			}
			treeObject.Data = append(treeObject.Data, []byte(typeName)...)
		} else {
			//是文件夹 递归迭代到文件
			dir := node.(Dir)
			tmp := dealDir(store, dir, h)
			jsonMarshal, _ := json.Marshal(tmp)
			h.Write(jsonMarshal)
			treeObject.Links = append(treeObject.Links, Link{
				Hash: h.Sum(nil),
				Size: int(dir.Size()),
				Name: dir.Name(),
			})
			typeName := "tree"
			treeObject.Data = append(treeObject.Data, []byte(typeName)...)
		}
	}
	jsonMarshal, _ := json.Marshal(treeObject)
	h.Write(jsonMarshal)
	store.Put(h.Sum(nil), jsonMarshal)
	return treeObject
}

func dealFile_(node File, store KVStore, h hash.Hash) *Object {
	links := &Object{}
	for i := 0; i < int(node.Size()/256)+1; i += 256 { //有几个256 分多一块
		//分片  需要link
		//end 每次 + 256  最后一片不满256  end = node.Size()
		//data 取 [i:end]
		end := i + 256
		if int(node.Size()) < end {
			end = int(node.Size())
		}
		data := node.Bytes()[i:end]
		blob := Object{
			Links: nil,
			Data:  data,
		}
		jsonMarshal, _ := json.Marshal(blob)
		h.Write(jsonMarshal)
		store.Put(h.Sum(nil), data)

		//分片写入links 每个ipfs就代指一片
		links.Links = append(links.Links, Link{
			Name: node.Name(),
			Hash: h.Sum(nil),
			Size: len(data),
		})
		links.Data = append(links.Data, []byte("blob")...)
	}
	//links写入KVStore
	jsonMarshal, _ := json.Marshal(links)
	h.Write(jsonMarshal)
	store.Put(h.Sum(nil), jsonMarshal)
	return links
}
