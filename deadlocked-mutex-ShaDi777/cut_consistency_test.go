package node

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSnapshotIsConsistentCut focuses solely on validating whether a snapshot
// represents a consistent cut in the Chandy-Lamport sense (no receive without
// a matching send in the cut).
func TestSnapshotIsConsistentCut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		snapshot       GlobalState
		wantConsistent bool
	}{
		{
			name: "empty_snapshot",
			snapshot: GlobalState{
				AllNodeIDs: nil,
				States:     map[string]LocalState{},
			},
			wantConsistent: true,
		},
		{
			name: "single_send_only_consistent",
			snapshot: GlobalState{
				AllNodeIDs: []string{"A", "B"},
				States: map[string]LocalState{
					"A": {
						ID: "A",
						Events: []Event{
							{NodeID: "A", Index: 0, Kind: EventSend, From: "A", To: "B", MsgType: msgLock, Requester: "A", Timestamp: 1},
						},
					},
					"B": {ID: "B", Events: nil},
				},
			},
			wantConsistent: true,
		},
		{
			name: "matching_send_and_receive_consistent",
			snapshot: GlobalState{
				AllNodeIDs: []string{"A", "B"},
				States: map[string]LocalState{
					"A": {
						ID: "A",
						Events: []Event{
							{NodeID: "A", Index: 0, Kind: EventSend, From: "A", To: "B", MsgType: msgLock, Requester: "A", Timestamp: 1},
						},
					},
					"B": {
						ID: "B",
						Events: []Event{
							{NodeID: "B", Index: 0, Kind: EventReceive, From: "A", To: "B", MsgType: msgLock, Requester: "A", Timestamp: 1},
						},
					},
				},
			},
			wantConsistent: true,
		},
		{
			name: "receive_without_send_inconsistent",
			snapshot: GlobalState{
				AllNodeIDs: []string{"A", "B"},
				States: map[string]LocalState{
					"A": {
						ID:     "A",
						Events: []Event{
							// No send from B->A exists in the cut.
						},
					},
					"B": {
						ID: "B",
						Events: []Event{
							{NodeID: "B", Index: 0, Kind: EventReceive, From: "A", To: "B", MsgType: msgLock, Requester: "A", Timestamp: 1},
						},
					},
				},
			},
			wantConsistent: false,
		},
		{
			name: "multi_node_all_channels_consistent",
			snapshot: GlobalState{
				AllNodeIDs: []string{"A", "B", "C"},
				States: map[string]LocalState{
					"A": {
						ID: "A",
						Events: []Event{
							{NodeID: "A", Index: 0, Kind: EventSend, From: "A", To: "B", MsgType: msgLock, Requester: "A", Timestamp: 1},
							{NodeID: "A", Index: 1, Kind: EventSend, From: "A", To: "C", MsgType: msgLock, Requester: "A", Timestamp: 2},
						},
					},
					"B": {
						ID: "B",
						Events: []Event{
							{NodeID: "B", Index: 0, Kind: EventReceive, From: "A", To: "B", MsgType: msgLock, Requester: "A", Timestamp: 1},
						},
					},
					"C": {
						ID: "C",
						Events: []Event{
							{NodeID: "C", Index: 0, Kind: EventReceive, From: "A", To: "C", MsgType: msgLock, Requester: "A", Timestamp: 2},
						},
					},
				},
			},
			wantConsistent: true,
		},
		{
			name: "multi_node_one_inconsistent_channel",
			snapshot: GlobalState{
				AllNodeIDs: []string{"A", "B", "C"},
				States: map[string]LocalState{
					"A": {
						ID: "A",
						Events: []Event{
							// Send to B exists, but not to C.
							{NodeID: "A", Index: 0, Kind: EventSend, From: "A", To: "B", MsgType: msgLock, Requester: "A", Timestamp: 1},
						},
					},
					"B": {
						ID: "B",
						Events: []Event{
							{NodeID: "B", Index: 0, Kind: EventReceive, From: "A", To: "B", MsgType: msgLock, Requester: "A", Timestamp: 1},
						},
					},
					"C": {
						ID: "C",
						Events: []Event{
							// Receive from A, but no corresponding send A->C in the cut.
							{NodeID: "C", Index: 0, Kind: EventReceive, From: "A", To: "C", MsgType: msgLock, Requester: "A", Timestamp: 2},
						},
					},
				},
			},
			wantConsistent: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.wantConsistent, tt.snapshot.IsConsistentCut())
		})
	}
}
