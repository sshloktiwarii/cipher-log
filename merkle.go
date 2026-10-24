package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
)

// GenesisMerkleHash is the deterministic root hash for block 0
const GenesisMerkleHash = "0000000000000000000000000000000000000000000000000000000000000000"

// MerkleChainTracker maintains the cryptographically linked hash chain for telemetry logs
type MerkleChainTracker struct {
	mu       sync.Mutex
	lastHash string
}

func NewMerkleChainTracker(initialHash string) *MerkleChainTracker {
	if initialHash == "" {
		initialHash = GenesisMerkleHash
	}
	return &MerkleChainTracker{
		lastHash: initialHash,
	}
}

// ComputeNextHash computes SHA256(prev_hash || timestamp || source || event_type || metrics || metadata)
func ComputeNextHash(prevHash, timestamp, source, eventType, metricsJSON, metaJSON string) string {
	payload := fmt.Sprintf("%s|%s|%s|%s|%s|%s", prevHash, timestamp, source, eventType, metricsJSON, metaJSON)
	h := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(h[:])
}

func (m *MerkleChainTracker) Append(timestamp, source, eventType, metricsJSON, metaJSON string) (prevHash string, currentHash string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	prevHash = m.lastHash
	currentHash = ComputeNextHash(prevHash, timestamp, source, eventType, metricsJSON, metaJSON)
	m.lastHash = currentHash
	return prevHash, currentHash
}

func (m *MerkleChainTracker) CurrentRoot() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastHash
}
