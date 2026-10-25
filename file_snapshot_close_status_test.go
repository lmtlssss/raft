package raft

import (
    "os"
    "runtime"
    "testing"
)

// The first Close of a failed snapshot must never be followed by a successful
// Close on the same sink, because a successful Close is the durability signal.
func TestFileSnapshotSinkFailedCloseIsNotLaterReportedSuccessful(t *testing.T) {
    if runtime.GOOS == "windows" {
        t.Skip("cannot remove open snapshot file on Windows")
    }
    store, err := NewFileSnapshotStoreWithLogger(t.TempDir(), 1, newTestLogger(t))
    if err != nil {
        t.Fatal(err)
    }
    _, trans := NewInmemTransport(NewInmemAddr())
    generic, err := store.Create(SnapshotVersionMax, 10, 3, Configuration{}, 0, trans)
    if err != nil {
        t.Fatal(err)
    }
    sink := generic.(*FileSnapshotSink)
    if _, err := sink.Write([]byte("synthetic payload")); err != nil {
        t.Fatal(err)
    }
    // Remove the temporary directory without closing its already-open state
    // handle, so finalization can finish but metadata publication cannot.
    if err := os.RemoveAll(sink.dir); err != nil {
        t.Fatal(err)
    }
    if err := sink.Close(); err == nil {
        t.Fatal("expected first Close to reject missing metadata directory")
    }
    if err := sink.Close(); err == nil {
        t.Fatal("repeated Close returned success after snapshot creation failed")
    }
}
