### PagePool

由于 fuse 提供的 write 接口长下面这样：

```
func (fh *RegularFileHandler) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
...
}
```
我们如果每次的文件写入请求，都直接刷写到对象存储 minio 中，有些浪费 IO 性能。
因此我们通过一个 PagePool 将所有的写入的数据缓冲到内存中，然后定期刷写。

在 tinygitfs 中，每个打开的文件都会拥有一个 pagepool，每个 pagepool 中主要就是一个的页面数据 lru 缓存。
这个缓存中存放多个 1M 的文件页面数据（和 minio 中的文件 chunk 大小相同）。

#### READ
每次我们读取文件数据，首先会去在文件的 pagepool 查找是否存在对应偏移量的 page，如果存在则拷贝返回，如果不存在则从 minio 加载对应的 chunk，并将这个 chunk 放入缓存中。

#### WRITE
每次我们写文件数据，首先会去在文件的 pagepool 查找是否存在对应偏移量的 page，如果存在则将数据写入其中，如果不存在则分一块空页放入缓存中，然后我们将数据写入这个数据页。

#### FSYNC
pagepool 启动了一个后台协程，定期将脏的数据页进行刷写。

#### MemAttr
另外在 pagepool 中还额外维护了一个 memattr，也就是文件的元数据缓存。这个文件元数据会在打开文件的时候加载到内存中，之后文件的一些元数据修改首先会写入到 memattr 中，然后 memattr 也会定期刷写到 redis 中。同时读取也是优先读取 memattr。

总而言之，pagepool 起了文件数据和元数据缓存的作用。

