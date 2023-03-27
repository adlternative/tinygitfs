package test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/adlternative/tinygitfs/pkg/data"
	"github.com/adlternative/tinygitfs/pkg/gitfs"
	"github.com/stretchr/testify/require"
)

func TestMount(t *testing.T) {
	ctx := context.Background()

	testStorage := CreateTestStorage(ctx, t)
	defer testStorage.Cleanup(ctx, t)

	tempMntDir, err := os.MkdirTemp("/tmp", "tinygitfs-*")
	require.NoError(t, err)
	defer require.NoError(t, os.RemoveAll(tempMntDir))

	server, err := gitfs.Mount(ctx, tempMntDir, false, "redis://"+testStorage.GetRedisURI(), &data.Option{
		EndPoint:  "http://" + testStorage.GetMinioURI(),
		Bucket:    "gitfs",
		Accesskey: "minioadmin",
		SecretKey: "minioadmin",
	})
	require.NoError(t, err)
	defer require.NoError(t, server.Unmount())
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
		require.NoError(t, err)
		require.NoError(t, file.Close())

		fileInfo, err := os.Stat(fileName)
		require.NoError(t, err)
		require.Equalf(t, tc.expectSize, fileInfo.Size(), "file size wrong")
		require.Equalf(t, tc.fileName, fileInfo.Name(), "file name wrong")
		require.Truef(t, fileInfo.Mode().IsRegular(), "file mode wrong")
	}
}
