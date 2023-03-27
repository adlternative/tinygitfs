package test

import (
	"context"
	"github.com/adlternative/tinygitfs/pkg/data"
	"github.com/adlternative/tinygitfs/pkg/gitfs"
	"os"
	"testing"
)

func TestMount(t *testing.T) {
	ctx := context.Background()

	testStorage := CreateTestEnvironment(ctx, t)
	defer testStorage.Cleanup(ctx, t)

	tempMntDir, err := os.MkdirTemp("/tmp", "tinygitfs-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempMntDir)

	server, err := gitfs.Mount(ctx, tempMntDir, false, "redis://"+testStorage.GetRedisURI(), &data.Option{
		EndPoint:  "http://" + testStorage.GetMinioURI(),
		Bucket:    "gitfs",
		Accesskey: "minioadmin",
		SecretKey: "minioadmin",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Unmount()
}
