package test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/adlternative/tinygitfs/pkg/cmd"
	"github.com/adlternative/tinygitfs/pkg/data"
	"github.com/adlternative/tinygitfs/pkg/gitfs"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
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
			bare:     true,
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

func TestGitAdd(t *testing.T) {
	ctx := context.Background()

	testEnv := CreateTestEnvironment(ctx, t)

	log.Debugf("test dir: %s", testEnv.Root())
	defer testEnv.Cleanup(ctx, t)

	repoName := "test-repo"

	type TestCases = []struct {
		fileName string
		content  []byte
	}
	testCases := TestCases{
		{
			fileName: "test-file-1",
			content:  []byte("test-message"),
		},
		{
			fileName: "test-file-2",
			content:  []byte("test-message-2"),
		},
	}
	for _, tc := range testCases {
		repoPath := filepath.Join(testEnv.Root(), repoName)

		gitInit(ctx, t, repoPath)
		gitAdd(ctx, t, repoPath, tc.fileName, tc.content)

		var oidBuf strings.Builder

		lsFilesCmd := cmd.NewGitCommand("ls-files").
			WithGitDir(path.Join(repoPath, ".git")).
			WithWorkTree(repoPath).
			WithOptions("--format=%(objectname)").WithArgs(tc.fileName).WithStdout(&oidBuf)

		require.NoError(t, lsFilesCmd.Start(ctx))
		require.NoError(t, lsFilesCmd.Wait())

		var contentBuf bytes.Buffer

		catFileCmd := cmd.NewGitCommand("cat-file").
			WithGitDir(path.Join(repoPath, ".git")).
			WithOptions("-p").WithArgs(strings.TrimRight(oidBuf.String(), "\n")).WithStdout(&contentBuf)

		require.NoError(t, catFileCmd.Start(ctx))
		require.NoError(t, catFileCmd.Wait())

		require.Equal(t, contentBuf.Bytes(), tc.content)
	}
}

func gitAdd(ctx context.Context, t *testing.T, repoPath string, fileName string, content []byte) {
	filePath := path.Join(repoPath, fileName)

	file, err := os.Create(filePath)
	require.NoError(t, err)

	_, err = io.Copy(file, bytes.NewReader(content))
	require.NoError(t, err)

	require.NoError(t, file.Close())

	gitCmd := cmd.NewGitCommand("add").WithWorkTree(repoPath).
		WithGitDir(path.Join(repoPath, ".git")).WithArgs(filePath)

	require.NoError(t, gitCmd.Start(ctx))
	require.NoError(t, gitCmd.Wait())
}

func gitInit(ctx context.Context, t *testing.T, repoPath string) {
	gitCmd := cmd.NewGitCommand("init").WithArgs(repoPath)

	require.NoError(t, gitCmd.Start(ctx))
	require.NoError(t, gitCmd.Wait())

	require.DirExists(t, repoPath)
	checkBareRepoExists(t, fmt.Sprintf("%s/.git", repoPath))

}

func checkBareRepoExists(t *testing.T, repoPath string) {
	require.DirExists(t, repoPath)
	require.DirExists(t, fmt.Sprintf("%s/objects", repoPath))
	require.DirExists(t, fmt.Sprintf("%s/refs", repoPath))
	require.FileExists(t, fmt.Sprintf("%s/HEAD", repoPath))
	require.FileExists(t, fmt.Sprintf("%s/config", repoPath))
}
