### TinyGitfs

A little tiny gitfs

#### Goal
Explore the possibility of Git's storage and computation separation.

#### Function
Use [Fuse](https://en.wikipedia.org/wiki/Filesystem_in_Userspace) to route git repository data to different storage media.
Specifically, the file system's file metadata and directory data are written to a metadata storage such as Redis, while file data is written to an object storage such as MinIO.
From the perspective of a Git repository, all of its data is saved to a data store, except for Git's loose and symbolic references which are saved to a metadata store as key-value (KV) pairs.

#### How to use
```shell
# run metadata storage redis
$ docker run -d --name redis \
        -v $PWD/redis-data:/data \
        -p 6379:6379 \
        --restart unless-stopped \
        redis redis-server --appendonly yes
   
# run data storage minio       
$ docker run -d --name minio \
        -v $PWD/minio-data:/data \
        -p 9000:9000 \
        --restart unless-stopped \
        minio/minio server /data

# run tinygitfs   
$ go build
$ mkdir /tmp/tinygitfs
$ ./tinygitfs mount /tmp/tinygitfs --metadata="redis://127.0.0.1:6379/2"  --endpoint=http://127.0.0.1:9000 --bucket=gitfs --access_key=minioadmin --secret_key=minioadmin
$ ls -ali /tmp/tinygitfs
total 16
    0 drwxr-xr-x   9 adl   staff  4096 Jan  4 00:16 .
26346 drwxrwxrwt  17 root  wheel   544 Jan  7 19:52 ..
   57 drwxr-xr-x   5 adl   staff  4096 Jan  7 17:04 d4
   60 -rw-r--r--   1 adl   staff     0 Jan  7 18:06 f3
```

#### architecture
![img.png](doc/resource/arch.png)