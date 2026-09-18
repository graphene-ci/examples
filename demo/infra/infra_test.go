package infra

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	t.Parallel()
	out := "...                                                                      [100%]\n3 passed in 2.10s\n{\"cpus\": 2, \"diskMBps\": 312.5, \"connectP99Ms\": 0.41}\n"
	r := Parse(0, out)
	require.True(t, r.Passed)
	require.Equal(t, "3 passed in 2.10s", r.Summary)
	require.Equal(t, 2, r.CPUs)
	require.InDelta(t, 312.5, r.DiskMBps, 0.001)
	require.InDelta(t, 0.41, r.ConnectP99Ms, 0.001)

	failed := Parse(1, "=========== 1 failed, 2 passed in 3.4s ===========\n{\"cpus\": 2, \"passed\": true}\n")
	require.False(t, failed.Passed, "the exit status decides, not the JSON")
	require.Equal(t, "1 failed, 2 passed in 3.4s", failed.Summary)
}

func TestSuiteIsEmbedded(t *testing.T) {
	t.Parallel()
	raw, err := Tests.ReadFile(SuitePath)
	require.NoError(t, err)
	require.Contains(t, string(raw), "def test_disk_fsync_throughput")
}
