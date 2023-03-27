package test

import (
	"context"
	"github.com/adlternative/tinygitfs/pkg/data"
	"github.com/adlternative/tinygitfs/pkg/gitfs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"os"

	tcminio "github.com/romnn/testcontainers/minio"
	tcredis "github.com/romnn/testcontainers/redis"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)

	redisC, err := tcredis.Start(ctx, tcredis.Options{
		ImageTag: "latest",
	})
	require.NoError(t, err)

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
	require.NoError(t, te.testServer.Unmount())
	te.testStorage.Cleanup(ctx, t)
	require.NoError(t, os.RemoveAll(te.mntDir))
}

func CreateTestEnvironment(ctx context.Context, t *testing.T) *TestEnv {
	testStorage := CreateTestStorage(ctx, t)

	tempMntDir, err := os.MkdirTemp("/tmp", "tinygitfs-*")
	require.NoError(t, err)

	server, err := gitfs.Mount(ctx, tempMntDir, false, "redis://"+testStorage.GetRedisURI(), &data.Option{
		EndPoint:  "http://" + testStorage.GetMinioURI(),
		Bucket:    "gitfs",
		Accesskey: "minioadmin",
		SecretKey: "minioadmin",
	})
	require.NoError(t, err)

	return &TestEnv{
		testStorage: testStorage,
		mntDir:      tempMntDir,
		testServer:  server,
	}
}
