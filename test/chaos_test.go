package test

import (
	"fmt"
	"net/rpc"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/Mazennaji/raftkv/client"
)

type testCluster struct {
	procs []*exec.Cmd
	dir   string
}

func startCluster(t *testing.T, n int) *testCluster {
	dir := t.TempDir()
	tc := &testCluster{dir: dir}

	for i := 1; i <= n; i++ {
		walPath := fmt.Sprintf("%s/node%d.wal", dir, i)
		cmd := exec.Command("./raftkv.exe", fmt.Sprintf("%d", i), walPath)
		cmd.Dir = ".."
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("failed to start node %d: %v", i, err)
		}
		tc.procs = append(tc.procs, cmd)
	}

	time.Sleep(3 * time.Second)
	return tc
}

func (tc *testCluster) killNode(i int) {
	tc.procs[i-1].Process.Kill()
	tc.procs[i-1].Wait()
}

func (tc *testCluster) restartNode(i int) {
	walPath := fmt.Sprintf("%s/node%d.wal", tc.dir, i)
	cmd := exec.Command("./raftkv.exe", fmt.Sprintf("%d", i), walPath)
	cmd.Dir = ".."
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Start()
	tc.procs[i-1] = cmd
}

func (tc *testCluster) shutdown() {
	for _, p := range tc.procs {
		if p.Process != nil {
			p.Process.Kill()
			p.Wait()
		}
	}
}

func addresses() []string {
	return []string{"127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"}
}

func TestKillLeaderMidOperation(t *testing.T) {
	tc := startCluster(t, 3)
	defer tc.shutdown()

	c := client.New(addresses())

	if err := c.Put("key1", "value1"); err != nil {
		t.Fatalf("initial put failed: %v", err)
	}

	leaderAddr := findLeaderIndex(t, c)
	tc.killNode(leaderAddr)

	time.Sleep(2 * time.Second)

	if err := c.Put("key2", "value2"); err != nil {
		t.Fatalf("put after leader kill failed: %v", err)
	}

	val, found, err := c.Get("key1")
	if err != nil || !found || val != "value1" {
		t.Fatalf("data loss after leader kill: val=%s found=%v err=%v", val, found, err)
	}

	val, found, err = c.Get("key2")
	if err != nil || !found || val != "value2" {
		t.Fatalf("new write didn't take after leader kill: val=%s found=%v err=%v", val, found, err)
	}
}

func TestRestartAfterCrash(t *testing.T) {
	tc := startCluster(t, 3)
	defer tc.shutdown()

	c := client.New(addresses())

	if err := c.Put("durable", "data"); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	tc.killNode(2)
	time.Sleep(500 * time.Millisecond)
	tc.restartNode(2)
	time.Sleep(2 * time.Second)

	val, found, err := c.Get("durable")
	if err != nil || !found || val != "data" {
		t.Fatalf("data lost after restart: val=%s found=%v err=%v", val, found, err)
	}
}

func TestMinorityCannotServeWrites(t *testing.T) {
	tc := startCluster(t, 3)
	defer tc.shutdown()

	c := client.New(addresses())
	c.Put("seed", "value")

	tc.killNode(1)
	tc.killNode(2)
	time.Sleep(1 * time.Second)

	err := c.Put("should_fail", "value")
	if err == nil {
		t.Fatalf("minority partition accepted a write — split-brain bug")
	}
}

func findLeaderIndex(t *testing.T, c *client.Client) int {
	return 1
}

func setNetworkEnabled(address string, enabled bool) error {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer client.Close()
	return client.Call("Raft.SetNetworkEnabled", &raft.SetNetworkArgs{Enabled: enabled}, &raft.SetNetworkReply{})
}

func TestNetworkPartitionIsolatesMinority(t *testing.T) {
	tc := startCluster(t, 3)
	defer tc.shutdown()

	c := client.New(addresses())

	if err := c.Put("before_partition", "value1"); err != nil {
		t.Fatalf("initial put failed: %v", err)
	}

	if err := setNetworkEnabled("127.0.0.1:8003", false); err != nil {
		t.Fatalf("failed to partition node 3: %v", err)
	}

	time.Sleep(1 * time.Second)

	if err := c.Put("during_partition", "value2"); err != nil {
		t.Fatalf("majority side should still accept writes: %v", err)
	}

	if err := setNetworkEnabled("127.0.0.1:8003", true); err != nil {
		t.Fatalf("failed to heal partition: %v", err)
	}

	time.Sleep(2 * time.Second)

	val, found, err := c.Get("during_partition")
	if err != nil || !found || val != "value2" {
		t.Fatalf("write made during partition was lost: val=%s found=%v err=%v", val, found, err)
	}
}
