package test

import (
	"context"
	"github.com/adlternative/tinygitfs/pkg/data"
	"github.com/adlternative/tinygitfs/pkg/gitfs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"os"

	tcminio "github.com/romnn/testcontainers/minio"
	tcredis "github.com/romnn/testcontainers/redis"
	"testing"
)

type TestStorage struct {
	minioC tcminio.Container
	redisC tcredis.Container
}

func (ts *TestStorage) GetMinioURI() string {
	return ts.minioC.ConnectionURI()
}

func (ts *TestStorage) GetRedisURI() string {
	return ts.redisC.ConnectionURI()
}

func (ts *TestStorage) Cleanup(ctx context.Context, t *testing.T) {
	ts.redisC.Terminate(ctx)
	ts.minioC.Terminate(ctx)
}

func CreateTestStorage(ctx context.Context, t *testing.T) *TestStorage {
	minioC, err := tcminio.Start(ctx, tcminio.Options{
		ImageTag:     "latest",
		RootUser:     "minioadmin",
		RootPassword: "minioadmin",
	})
	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}
	redisC, err := tcredis.Start(ctx, tcredis.Options{
		ImageTag: "latest",
	})
	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}

	return &TestStorage{
		minioC: minioC,
		redisC: redisC,
	}
}

type TestEnv struct {
	testStorage *TestStorage
	mntDir      string
	testServer  *fuse.Server
}

func (te *TestEnv) Root() string {
	return te.mntDir
}

func (te *TestEnv) Cleanup(ctx context.Context, t *testing.T) {
	err := te.testServer.Unmount()
	if err != nil {
		t.Fatal(err)
	}
	te.testStorage.Cleanup(ctx, t)
	err = os.RemoveAll(te.mntDir)
	if err != nil {
		t.Fatal(err)
	}
}

func CreateTestEnvironment(ctx context.Context, t *testing.T) *TestEnv {
	testStorage := CreateTestStorage(ctx, t)

	tempMntDir, err := os.MkdirTemp("/tmp", "tinygitfs-*")
	if err != nil {
		t.Fatal(err)
	}

	server, err := gitfs.Mount(ctx, tempMntDir, false, "redis://"+testStorage.GetRedisURI(), &data.Option{
		EndPoint:  "http://" + testStorage.GetMinioURI(),
		Bucket:    "gitfs",
		Accesskey: "minioadmin",
		SecretKey: "minioadmin",
	})
	if err != nil {
		t.Fatal(err)
	}

	return &TestEnv{
		testStorage: testStorage,
		mntDir:      tempMntDir,
		testServer:  server,
	}
}
