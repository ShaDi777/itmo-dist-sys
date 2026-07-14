package node

import (
	"github.com/nikitakosatka/hive/pkg/hive"
	"math"
	"slices"
	"sync"
	"time"
)

// MutexNode is a hive.Node that exposes a distributed mutual exclusion API.
type MutexNode interface {
	hive.Node

	// Lock blocks until the node enters the critical section.
	Lock() error

	// Unlock releases the critical section.
	Unlock() error

	// InCriticalSection reports whether this node currently holds the mutex.
	InCriticalSection() bool
}

// EventKind distinguishes between send and receive events.
type EventKind int

const (
	EventSend EventKind = iota
	EventReceive
)

// Event represents an application-level event used for building consistent cuts.
// For Chandy-Lamport style reasoning, we track send/receive of Maekawa
// REQUEST messages.
type Event struct {
	NodeID string
	Index  int

	Kind EventKind

	From string
	To   string

	MsgType   messageType
	Requester string
	Timestamp int
}

func (n *MaekawaMutexNode) recordEvent(kind EventKind, from, to string, mt messageType, requester string, ts int) {
	e := Event{
		NodeID:    n.ID(),
		Index:     len(n.events),
		Kind:      kind,
		From:      from,
		To:        to,
		MsgType:   mt,
		Requester: requester,
		Timestamp: ts,
	}
	n.events = append(n.events, e)

	if len(n.snapshotMarkers) != 0 {
		_, isMarked := n.snapshotMarkers[from]
		if !isMarked {
			n.snapshotChannels[from] = append(n.snapshotChannels[from], e)
		}
	}
}

// messageType enumerates logical message types used by the Maekawa
// algorithm. They are carried inside hive.Message.Payload.
type messageType int

const (
	msgLock messageType = iota + 1
	msgOk
	msgUnlock
)

type maekawaPayload struct {
	messageType messageType
	requesterID string
}

const (
	VoteNone string = ""
	TS              = 0
)

// MaekawaMutexNode implements MutexNode using Maekawa's quorum–based mutex.
type MaekawaMutexNode struct {
	*hive.BaseNode

	mu sync.Mutex

	allNodeIDs    []string
	quorum        map[string]bool
	quorumNodeIDs []string

	voteFor             string
	isInCriticalSection bool

	events    []Event
	voteQueue []string

	snapshotEvents   []Event
	snapshotMarkers  map[string]struct{}
	snapshotChannels map[string][]Event
}

// NewMaekawaMutexNode creates a new node with a Maekawa quorum constructed
// from the full list of node IDs.
func NewMaekawaMutexNode(id string, allNodeIDs []string) MutexNode {

	// QUORUM begin
	// Building sqrt(N) quorum
	total := len(allNodeIDs)
	sqrtTotal := int(math.Sqrt(float64(total))) + 1
	quorum := make([]string, 0, sqrtTotal)

	self := slices.Index(allNodeIDs, id)
	if self == -1 {
		panic("should not happen: self id not found in allNodeIDs")
	}
	quorum = append(quorum, allNodeIDs[self])

	row := self / sqrtTotal
	col := self % sqrtTotal

	// fixed column + all possible row value
	for ri := 0; ri < sqrtTotal; ri++ {
		idx := ri*sqrtTotal + col
		if idx >= total || idx == self {
			continue
		}
		quorum = append(quorum, allNodeIDs[idx])
	}

	// fixed row + all possible col value
	for ci := 0; ci < sqrtTotal; ci++ {
		idx := row*sqrtTotal + ci
		if idx >= total || idx == self {
			continue
		}
		quorum = append(quorum, allNodeIDs[idx])
	}

	quorumMap := make(map[string]bool)
	for _, id := range quorum {
		quorumMap[id] = false
	}
	// QUORUM end

	return &MaekawaMutexNode{
		BaseNode: hive.NewBaseNode(id),

		mu: sync.Mutex{},

		allNodeIDs:    allNodeIDs,
		quorum:        quorumMap,
		quorumNodeIDs: quorum,

		voteFor:             VoteNone,
		isInCriticalSection: false,

		events:    make([]Event, 0),
		voteQueue: make([]string, 0),

		snapshotEvents:   make([]Event, 0),
		snapshotMarkers:  make(map[string]struct{}),
		snapshotChannels: make(map[string][]Event),
	}
}

// Lock requests the distributed mutex using Maekawa's algorithm. It blocks
// until this node has obtained all votes in its quorum.
func (n *MaekawaMutexNode) Lock() error {
	if n.isInCriticalSection {
		panic("should not happen: Lock while already being in critical section")
	}

	n.mu.Lock()
	for _, qNodeID := range n.quorumNodeIDs {
		payload := maekawaPayload{
			messageType: msgLock,
			requesterID: n.ID(),
		}
		_ = n.Send(qNodeID, payload)
		n.recordEvent(EventSend, n.ID(), qNodeID, payload.messageType, payload.requesterID, TS)
	}
	n.mu.Unlock()

	for {
		n.mu.Lock()
		allVotesReceived := true
		for _, qNodeID := range n.quorumNodeIDs {
			if !n.quorum[qNodeID] {
				allVotesReceived = false
				break
			}
		}
		if allVotesReceived {
			n.isInCriticalSection = true
			n.mu.Unlock()
			break
		}
		n.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}

	return nil
}

// Unlock releases the distributed mutex and returns votes to quorum members.
func (n *MaekawaMutexNode) Unlock() error {
	if !n.isInCriticalSection {
		panic("should not happen: Unlock while not being in critical section")
	}

	n.mu.Lock()

	n.isInCriticalSection = false
	for _, qNodeID := range n.quorumNodeIDs {
		n.quorum[qNodeID] = false

		payload := maekawaPayload{
			messageType: msgUnlock,
			requesterID: n.ID(),
		}
		_ = n.Send(qNodeID, payload)
		n.recordEvent(EventSend, n.ID(), qNodeID, payload.messageType, payload.requesterID, TS)
	}

	n.mu.Unlock()
	return nil
}

// InCriticalSection reports whether this node currently holds the mutex.
func (n *MaekawaMutexNode) InCriticalSection() bool {
	return n.isInCriticalSection
}

// Receive handles Maekawa protocol messages and snapshot markers.
func (n *MaekawaMutexNode) Receive(msg *hive.Message) error {
	mark, isMarker := msg.Payload.(marker)
	if isMarker {
		n.onMarkerReceive(mark)
		return nil
	}

	payload, ok := msg.Payload.(maekawaPayload)
	if !ok {
		panic("should not happen: Unknown message received")
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// sub func for giving vote to a Node that requested lock
	giveVote := func(requester string) {
		n.voteFor = requester

		response := maekawaPayload{
			messageType: msgOk,
			requesterID: requester,
		}
		_ = n.Send(payload.requesterID, response)
		n.recordEvent(EventSend, msg.From, msg.To, msgOk, requester, TS)
	}

	// process different messageTypes
	switch payload.messageType {
	case msgLock:
		if n.voteFor == VoteNone {
			giveVote(payload.requesterID)
		} else {
			n.voteQueue = append(n.voteQueue, payload.requesterID)
		}
	case msgOk:
		n.quorum[msg.From] = true
	case msgUnlock:
		if n.voteFor != payload.requesterID {
			break
		}

		n.voteFor = VoteNone
		if len(n.voteQueue) > 0 {
			requester := n.voteQueue[0]
			n.voteQueue = n.voteQueue[1:]
			giveVote(requester)
		}
	}

	n.recordEvent(EventReceive, msg.From, msg.To, payload.messageType, payload.requesterID, TS)

	return nil
}

// LocalState captures the event prefix of a node at some instant.
type LocalState struct {
	ID string

	// Events is the sequence of events recorded up to the snapshot cut.
	Events []Event
}

// GlobalState represents a consistent cut of the system at some instant.
type GlobalState struct {
	States map[string]LocalState
	// AllNodeIDs contains the IDs of all nodes participating in the snapshot.
	AllNodeIDs []string
}

type marker struct {
	From string
}

func (n *MaekawaMutexNode) onMarkerReceive(m marker) {

	if len(n.snapshotMarkers) == 0 {
		n.mu.Lock()
		n.snapshotEvents = make([]Event, len(n.events))
		copy(n.snapshotEvents, n.events)
		n.mu.Unlock()

		for _, nodeID := range n.allNodeIDs {
			_ = n.Send(nodeID, marker{From: n.ID()})
		}
	}

	n.snapshotMarkers[m.From] = struct{}{}
}

func (n *MaekawaMutexNode) retrieveLocalState() (localState *LocalState, ready bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if len(n.snapshotMarkers) != len(n.allNodeIDs) {
		ready = false
		return
	}
	ready = true

	// clear snapshot fields
	n.snapshotMarkers = map[string]struct{}{}
	n.snapshotChannels = map[string][]Event{}
	localSnapshotEvents := make([]Event, len(n.snapshotEvents))
	copy(localSnapshotEvents, n.snapshotEvents)
	n.snapshotEvents = make([]Event, 0)

	localState = &LocalState{
		ID:     n.ID(),
		Events: localSnapshotEvents,
	}
	return
}

// Snapshot initiates a Chandy-Lamport snapshot from the first node in the
// slice and waits until all nodes have completed the snapshot. It then
// collects the per-node event prefixes that form the cut.
func Snapshot(nodes []MutexNode) (GlobalState, error) {
	states := make(map[string]LocalState)

	initiatorNode := nodes[0].(*MaekawaMutexNode)
	initiatorNode.onMarkerReceive(marker{From: initiatorNode.ID()})

	for _, n := range nodes {
		node := n.(*MaekawaMutexNode)

		for {
			localState, ready := node.retrieveLocalState()
			if ready {
				states[node.ID()] = *localState
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	ids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.ID())
	}

	return GlobalState{
		States:     states,
		AllNodeIDs: ids,
	}, nil
}

// HasDeadlock checks whether the wait-for graph constructed from this snapshot
// contains a cycle.
func (s GlobalState) HasDeadlock() bool {

	// build WaitForGraph
	waitForGraph := map[string][]string{}
	for nodeID, localState := range s.States {

		waitingFor := map[string]bool{}

		for _, e := range localState.Events {

			if e.Kind == EventSend && e.MsgType == msgLock {
				waitingFor[e.To] = true
			}

			if e.Kind == EventReceive && e.MsgType == msgOk {
				waitingFor[e.From] = false
			}
		}

		for quorumNodeID := range waitingFor {

			if !waitingFor[quorumNodeID] {
				continue
			}

			waitForGraph[nodeID] = append(waitForGraph[nodeID], quorumNodeID)
		}
	}

	// detect cycle in WaitForGraph
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	var dfs func(node string) bool
	dfs = func(node string) bool {
		visited[node] = true
		recStack[node] = true

		for _, neighbour := range waitForGraph[node] {
			if recStack[neighbour] {
				return true
			}

			if !visited[neighbour] {
				if dfs(neighbour) {
					return true
				}
			}
		}

		recStack[node] = false
		return false
	}

	for node := range waitForGraph {
		if !visited[node] {
			if dfs(node) {
				return true
			}
		}
	}

	return false
}

// IsConsistentCut checks a Chandy-Lamport style consistency property on this
// snapshot's events: for every receive event in the cut, there must exist a
// matching send event for the same message in the cut.
func (s GlobalState) IsConsistentCut() bool {
	type key struct {
		from string
		to   string
		mt   messageType
	}

	sent := map[key]bool{}

	// gather all sent events
	for _, st := range s.States {
		for _, e := range st.Events {
			if e.Kind == EventSend {
				k := key{e.From, e.To, e.MsgType}
				sent[k] = true
			}
		}
	}

	// try to find received event that was not sent
	for _, st := range s.States {
		for _, e := range st.Events {
			if e.Kind == EventReceive {
				k := key{e.From, e.To, e.MsgType}
				if !sent[k] {
					return false
				}
			}
		}
	}

	return true
}
