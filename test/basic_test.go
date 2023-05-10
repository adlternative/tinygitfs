package test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/adlternative/tinygitfs/pkg/cmd"
	"github.com/adlternative/tinygitfs/pkg/data"
	"github.com/adlternative/tinygitfs/pkg/gitfs"
)

func TestMount(t *testing.T) {
	ctx := context.Background()

	testStorage := CreateTestStorage(ctx, t)
	defer testStorage.Cleanup(ctx, t)

	tempMntDir, err := os.MkdirTemp("/tmp", "tinygitfs-*")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(tempMntDir))
	}()

	server, err := gitfs.Mount(ctx, tempMntDir, false, "redis://"+testStorage.GetRedisURI(), &data.Option{
		EndPoint:  "http://" + testStorage.GetMinioURI(),
		Bucket:    "gitfs",
		Accesskey: "minioadmin",
		SecretKey: "minioadmin",
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, server.Unmount())
	}()
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

func TestCreateFileWithContent(t *testing.T) {
	ctx := context.Background()

	testEnv := CreateTestEnvironment(ctx, t)
	defer testEnv.Cleanup(ctx, t)

	type TestCases = []struct {
		fileName   string
		expectSize int64
		content    []byte
	}
	testCases := TestCases{
		{
			fileName:   "abc",
			content:    []byte("test message"),
			expectSize: 12,
		},
		{
			fileName:   "dfe",
			content:    []byte("test message2"),
			expectSize: 13,
		},
	}
	for _, tc := range testCases {
		fileName := filepath.Join(testEnv.Root(), tc.fileName)
		file, err := os.Create(fileName)
		require.NoError(t, err)

		_, err = io.Copy(file, bytes.NewReader(tc.content))
		require.NoError(t, err)

		require.NoError(t, file.Close())

		fileInfo, err := os.Stat(fileName)
		require.NoError(t, err)
		require.Equalf(t, tc.expectSize, fileInfo.Size(), "file size wrong")
		require.Equalf(t, tc.fileName, fileInfo.Name(), "file name wrong")
		require.Truef(t, fileInfo.Mode().IsRegular(), "file mode wrong")

		content, err := os.ReadFile(fileName)
		require.NoError(t, err)
		require.Equalf(t, tc.content, content, "file data wrong")
	}
}

func TestGitInit(t *testing.T) {
	ctx := context.Background()

	testEnv := CreateTestEnvironment(ctx, t)

	log.Debugf("test dir: %s", testEnv.Root())
	defer testEnv.Cleanup(ctx, t)

	type TestCases = []struct {
		repoName string
		bare     bool
	}
	testCases := TestCases{
		{
			repoName: "test-repo-1",
		},
		{
			repoName: "test-repo-2",
		},
	}
	for _, tc := range testCases {
		repoPath := filepath.Join(testEnv.Root(), tc.repoName)

		gitCmd := cmd.NewGitCommand("init").WithArgs(repoPath)

		if tc.bare {
			gitCmd.WithOptions("--bare")
		}

		require.NoError(t, gitCmd.Start(ctx))
		require.NoError(t, gitCmd.Wait())

		if tc.bare {
			checkBareRepoExists(t, repoPath)
		} else {
			require.DirExists(t, repoPath)
			checkBareRepoExists(t, fmt.Sprintf("%s/.git", repoPath))
		}
	}
}

func checkBareRepoExists(t *testing.T, repoPath string) {
	require.DirExists(t, repoPath)
	require.DirExists(t, fmt.Sprintf("%s/objects", repoPath))
	require.DirExists(t, fmt.Sprintf("%s/refs", repoPath))
	require.FileExists(t, fmt.Sprintf("%s/HEAD", repoPath))
	require.FileExists(t, fmt.Sprintf("%s/config", repoPath))
}
