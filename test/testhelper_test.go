package test

import (
	"context"

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

func CreateTestEnvironment(ctx context.Context, t *testing.T) *TestStorage {
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
