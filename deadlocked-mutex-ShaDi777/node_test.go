package node

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/nikitakosatka/hive/pkg/failure"
	"github.com/nikitakosatka/hive/pkg/hive"
	"github.com/nikitakosatka/hive/pkg/network"
	timemodel "github.com/nikitakosatka/hive/pkg/time"
)

func TestMaekawaMutexNode_MutualExclusion(t *testing.T) {
	t.Parallel()

	netCfg := network.NewReliableNetwork()
	timeCfg := timemodel.NewTime(
		timemodel.Synchronous,
		&timemodel.ConstantLatency{Latency: 5 * time.Millisecond},
		0.0,
	)
	failCfg := failure.NewCrashStop(0.0)
	config := hive.NewConfig(
		hive.WithNetwork(netCfg),
		hive.WithTime(timeCfg),
		hive.WithNodesFailures(failCfg),
	)

	tests := []struct {
		name    string
		nodeIDs []string
	}{
		{
			name:    "single_node_lock_unlock",
			nodeIDs: []string{"A"},
		},
		{
			name:    "two_nodes_with_contention",
			nodeIDs: []string{"A", "B"},
		},
		{
			name:    "three_nodes_with_contention",
			nodeIDs: []string{"A", "B", "C"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sim := hive.NewSimulator(config)
			defer sim.Stop()

			nodes := make(map[string]MutexNode, len(tt.nodeIDs))
			for _, id := range tt.nodeIDs {
				n := NewMaekawaMutexNode(id, tt.nodeIDs)
				nodes[id] = n
				require.NoErrorf(t, sim.AddNode(n), "AddNode(%s) error", id)
			}

			for _, n := range nodes {
				require.NoErrorf(t, n.Start(context.Background()), "Start(%s) error", n.ID())
			}
			require.NoError(t, sim.Start(), "sim.Start error")

			// Sequentially let each node acquire and release the lock multiple
			// times. Maekawa's base algorithm can deadlock under full
			// contention, so we avoid starting all Lock() calls concurrently in
			// this correctness test. Instead we verify that whenever a node
			// reports being in the critical section, all others are outside.
			for round := 0; round < 3; round++ {
				for _, id := range tt.nodeIDs {
					n := nodes[id]

					require.NoErrorf(t, n.Lock(), "Lock error at node %s", n.ID())

					// Check mutual exclusion via InCriticalSection across all nodes.
					for otherID, other := range nodes {
						inCS := other.InCriticalSection()
						if otherID == id {
							require.Truef(t, inCS, "node %s should be in critical section", id)
						}
						if otherID != id {
							require.Falsef(t, inCS, "node %s should NOT be in critical section while %s holds the lock", otherID, id)
						}
					}

					// Small sleep to allow simulator/network to process any
					// internal messages while in the critical section.
					time.Sleep(5 * time.Millisecond)

					require.NoErrorf(t, n.Unlock(), "Unlock error at node %s", n.ID())
				}
			}

			for _, n := range nodes {
				require.NoErrorf(t, n.Stop(), "Stop(%s) error", n.ID())
			}
		})
	}
}

// TestSnapshot exercises Snapshot over real nodes in the
// simulator to ensure the marker-based Chandy-Lamport protocol completes and
// produces sensible cuts.
func TestSnapshot(t *testing.T) {
	t.Parallel()

	netCfg := network.NewReliableNetwork()
	timeCfg := timemodel.NewTime(
		timemodel.Synchronous,
		&timemodel.ConstantLatency{Latency: 5 * time.Millisecond},
		0.0,
	)
	failCfg := failure.NewCrashStop(0.0)
	config := hive.NewConfig(
		hive.WithNetwork(netCfg),
		hive.WithTime(timeCfg),
		hive.WithNodesFailures(failCfg),
	)

	tests := []struct {
		name    string
		nodeIDs []string
		hook    func(nodes []MutexNode)
	}{
		{
			name:    "two_nodes_no_traffic",
			nodeIDs: []string{"A", "B"},
		},
		{
			name:    "three_nodes_after_some_locks",
			nodeIDs: []string{"A", "B", "C"},
			hook: func(nodes []MutexNode) {
				for _, n := range nodes {
					if err := n.Lock(); err != nil {
						t.Fatalf("Lock error at node %s: %v", n.ID(), err)
					}
					time.Sleep(10 * time.Millisecond)
					if err := n.Unlock(); err != nil {
						t.Fatalf("Unlock error at node %s: %v", n.ID(), err)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sim := hive.NewSimulator(config)
			defer sim.Stop()

			ids := tt.nodeIDs
			nodes := make([]MutexNode, 0, len(ids))
			for _, id := range ids {
				n := NewMaekawaMutexNode(id, ids)
				require.NoError(t, sim.AddNode(n))
				defer n.Stop()
				nodes = append(nodes, n)
			}

			ctx := context.Background()

			for _, n := range nodes {
				require.NoError(t, n.Start(ctx))
			}
			require.NoError(t, sim.Start())

			if tt.hook != nil {
				tt.hook(nodes)
			}

			gs, err := Snapshot(nodes)
			require.NoError(t, err)

			require.Equal(t, len(gs.States), len(ids))
			for _, id := range ids {
				st, ok := gs.States[id]
				if !ok {
					t.Fatalf("snapshot missing state for node %s", id)
				}
				require.Truef(t, ok, "snapshot missing state for node %s", id)
				require.Equalf(t, st.ID, id, "state ID mismatch: have %s, want %s", st.ID, id)
			}

			require.Falsef(t, gs.HasDeadlock(), "snapshot should not contain a deadlock for test %q", tt.name)
		})
	}
}

func TestSnapshotDeadlockDetection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		snapshot     GlobalState
		wantDeadlock bool
	}{
		{
			name: "empty_snapshot",
			snapshot: GlobalState{
				AllNodeIDs: nil,
				States:     map[string]LocalState{},
			},
			wantDeadlock: false,
		},
		{
			name: "no_requests_no_deadlock",
			snapshot: GlobalState{
				AllNodeIDs: []string{"A", "B"},
				States: map[string]LocalState{
					"A": {ID: "A", Events: nil},
					"B": {ID: "B", Events: nil},
				},
			},
			wantDeadlock: false,
		},
		{
			name: "five_nodes_no_requests",
			snapshot: GlobalState{
				AllNodeIDs: []string{"A", "B", "C", "D", "E"},
				States: map[string]LocalState{
					"A": {ID: "A", Events: nil},
					"B": {ID: "B", Events: nil},
					"C": {ID: "C", Events: nil},
					"D": {ID: "D", Events: nil},
					"E": {ID: "E", Events: nil},
				},
			},
			wantDeadlock: false,
		},
		{
			name: "waiting_but_voter_free_no_deadlock",
			snapshot: GlobalState{
				AllNodeIDs: []string{"A", "B"},
				States: map[string]LocalState{
					"A": {
						ID: "A",
						Events: []Event{
							// A sent REQUEST to B, but B's vote is free (no GRANT send).
							{NodeID: "A", Index: 0, Kind: EventSend, From: "A", To: "B", MsgType: msgLock, Requester: "A", Timestamp: 1},
						},
					},
					"B": {
						ID:     "B",
						Events: nil, // B hasn't granted its vote to anyone.
					},
				},
			},
			wantDeadlock: false,
		},
		{
			name: "linear_wait_chain_no_cycle",
			snapshot: GlobalState{
				AllNodeIDs: []string{"A", "B", "C"},
				States: map[string]LocalState{
					"A": {
						ID: "A",
						Events: []Event{
							// A REQUESTs B and is still waiting.
							{NodeID: "A", Index: 0, Kind: EventSend, From: "A", To: "B", MsgType: msgLock, Requester: "A", Timestamp: 1},
						},
					},
					"B": {
						ID: "B",
						Events: []Event{
							// B's vote is locked for C.
							{NodeID: "B", Index: 0, Kind: EventSend, From: "B", To: "C", MsgType: msgOk, Requester: "C", Timestamp: 2},
						},
					},
					"C": {
						ID:     "C",
						Events: nil, // C not waiting on anyone.
					},
				},
			},
			wantDeadlock: false,
		},
		{
			name: "four_nodes_two_disjoint_waits_no_cycle",
			snapshot: GlobalState{
				AllNodeIDs: []string{"A", "B", "C", "D"},
				States: map[string]LocalState{
					"A": {
						ID: "A",
						Events: []Event{
							// A waits for B.
							{NodeID: "A", Index: 0, Kind: EventSend, From: "A", To: "B", MsgType: msgLock, Requester: "A", Timestamp: 1},
						},
					},
					"B": {
						ID:     "B",
						Events: nil, // B's vote is free (no GRANTs).
					},
					"C": {
						ID: "C",
						Events: []Event{
							// C waits for D.
							{NodeID: "C", Index: 0, Kind: EventSend, From: "C", To: "D", MsgType: msgLock, Requester: "C", Timestamp: 2},
						},
					},
					"D": {
						ID:     "D",
						Events: nil, // D's vote is free.
					},
				},
			},
			wantDeadlock: false,
		},
		{
			name: "three_nodes_two_node_cycle_deadlock",
			snapshot: GlobalState{
				AllNodeIDs: []string{"A", "B", "C"},
				States: map[string]LocalState{
					"A": {
						ID: "A",
						Events: []Event{
							// Two-node cycle between A and B, with idle C.
							{NodeID: "A", Index: 0, Kind: EventSend, From: "A", To: "B", MsgType: msgLock, Requester: "A", Timestamp: 1},
							{NodeID: "A", Index: 1, Kind: EventSend, From: "A", To: "A", MsgType: msgOk, Requester: "A", Timestamp: 2},
						},
					},
					"B": {
						ID: "B",
						Events: []Event{
							{NodeID: "B", Index: 0, Kind: EventSend, From: "B", To: "A", MsgType: msgLock, Requester: "B", Timestamp: 1},
							{NodeID: "B", Index: 1, Kind: EventSend, From: "B", To: "B", MsgType: msgOk, Requester: "B", Timestamp: 2},
						},
					},
					"C": {
						ID:     "C",
						Events: nil,
					},
				},
			},
			wantDeadlock: true,
		},
		{
			name: "four_nodes_two_node_cycle_deadlock",
			snapshot: GlobalState{
				AllNodeIDs: []string{"A", "B", "C", "D"},
				States: map[string]LocalState{
					"A": {
						ID: "A",
						Events: []Event{
							// Two-node cycle between A and B, with idle C and D.
							{NodeID: "A", Index: 0, Kind: EventSend, From: "A", To: "B", MsgType: msgLock, Requester: "A", Timestamp: 1},
							{NodeID: "A", Index: 1, Kind: EventSend, From: "A", To: "A", MsgType: msgOk, Requester: "A", Timestamp: 2},
						},
					},
					"B": {
						ID: "B",
						Events: []Event{
							{NodeID: "B", Index: 0, Kind: EventSend, From: "B", To: "A", MsgType: msgLock, Requester: "B", Timestamp: 1},
							{NodeID: "B", Index: 1, Kind: EventSend, From: "B", To: "B", MsgType: msgOk, Requester: "B", Timestamp: 2},
						},
					},
					"C": {ID: "C", Events: nil},
					"D": {ID: "D", Events: nil},
				},
			},
			wantDeadlock: true,
		},
		{
			name: "two_node_cycle_deadlock",
			snapshot: GlobalState{
				AllNodeIDs: []string{"A", "B"},
				States: map[string]LocalState{
					"A": {
						ID: "A",
						Events: []Event{
							// A sent REQUEST to B and never received GRANT from B -> A waits for B.
							{NodeID: "A", Index: 0, Kind: EventSend, From: "A", To: "B", MsgType: msgLock, Requester: "A", Timestamp: 1},
							// A's vote is locked for A.
							{NodeID: "A", Index: 1, Kind: EventSend, From: "A", To: "A", MsgType: msgOk, Requester: "A", Timestamp: 2},
						},
					},
					"B": {
						ID: "B",
						Events: []Event{
							// B sent REQUEST to A and never received GRANT from A -> B waits for A.
							{NodeID: "B", Index: 0, Kind: EventSend, From: "B", To: "A", MsgType: msgLock, Requester: "B", Timestamp: 1},
							// B's vote is locked for B.
							{NodeID: "B", Index: 1, Kind: EventSend, From: "B", To: "B", MsgType: msgOk, Requester: "B", Timestamp: 2},
						},
					},
				},
			},
			wantDeadlock: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.snapshot.HasDeadlock()
			require.Equalf(t, tt.wantDeadlock, got, "HasDeadlock() mismatch")
		})
	}
}
