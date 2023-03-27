package test

import (
	"context"
	"github.com/adlternative/tinygitfs/pkg/data"
	"github.com/adlternative/tinygitfs/pkg/gitfs"
	"os"
	"path/filepath"
	"testing"
)

func TestMount(t *testing.T) {
	ctx := context.Background()

	testStorage := CreateTestStorage(ctx, t)
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

func TestCreateFile(t *testing.T) {
	ctx := context.Background()

	testEnv := CreateTestEnvironment(ctx, t)
	defer testEnv.Cleanup(ctx, t)

	type TestCases = []struct {
		fileName   string
		expectSize int64
	}
	testCases := TestCases{
		{
			fileName:   "abc",
			expectSize: 0,
		},
		{
			fileName:   "dfe",
			expectSize: 0,
		},
	}
	for _, tc := range testCases {
		fileName := filepath.Join(testEnv.Root(), tc.fileName)
		file, err := os.Create(fileName)
		if err != nil {
			t.Fatal(err)
		}
		file.Close()

		fileInfo, err := os.Stat(fileName)
		if err != nil {
			t.Fatal(err)
		}
		if tc.expectSize != fileInfo.Size() {
			t.Fatalf("file size not equal")
		}
		if tc.fileName != fileInfo.Name() {
			t.Fatalf("file name not equal")
		}
		if !fileInfo.Mode().IsRegular() {
			t.Fatalf("file is not regular")
		}
	}
}
