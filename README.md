### TinyGitfs

A little tiny gitfs

#### How to use
```shell
# run redis
$ docker run -d --name redis \
        -v redis-data:/data \
        -p 6379:6379 \
        --restart unless-stopped \
        redis redis-server --appendonly yes
   
# run minio       
$ docker run -d --name minio \
        -v $PWD/minio-data:/data \
        -p 9000:9000 \
        --restart unless-stopped \
        minio/minio server /data

# run tinygitfs   
$ go build
$ ./tinygitfs mount /tmp/tinygitfs --metadata="redis://127.0.0.1:6379/2"  --endpoint=http://127.0.0.1:9000 --bucket=gitfs --access_key=minioadmin --secret_key=minioadmin
$ ls -ali /tmp/tinygitfs
total 16
    0 drwxr-xr-x   9 adl   staff  4096 Jan  4 00:16 .
26346 drwxrwxrwt  17 root  wheel   544 Jan  7 19:52 ..
   57 drwxr-xr-x   5 adl   staff  4096 Jan  7 17:04 d4
   60 -rw-r--r--   1 adl   staff     0 Jan  7 18:06 f3
```