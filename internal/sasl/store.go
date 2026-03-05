package sasl

import (
	"sync"
)

// Store holds all SASL credentials. Thread-safe for concurrent reads/writes.
type Store struct {
	mu       sync.RWMutex
	plain    map[string]string    // username -> password
	scram256 map[string]ScramAuth // username -> SCRAM-SHA-256 credential
	scram512 map[string]ScramAuth // username -> SCRAM-SHA-512 credential
}

// NewStore creates an empty credential store.
func NewStore() *Store {
	return &Store{
		plain:    make(map[string]string),
		scram256: make(map[string]ScramAuth),
		scram512: make(map[string]ScramAuth),
	}
}

// Empty returns true if no credentials are configured.
func (s *Store) Empty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.plain) == 0 && len(s.scram256) == 0 && len(s.scram512) == 0
}

// AddPlain adds a PLAIN credential.
func (s *Store) AddPlain(user, pass string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.plain[user] = pass
}

// LookupPlain checks PLAIN credentials. Returns true if valid.
func (s *Store) LookupPlain(user, pass string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.plain[user]
	return ok && p == pass
}

// AddScram256 adds a SCRAM-SHA-256 credential.
func (s *Store) AddScram256(user string, auth ScramAuth) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scram256[user] = auth
}

// AddScram512 adds a SCRAM-SHA-512 credential.
func (s *Store) AddScram512(user string, auth ScramAuth) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scram512[user] = auth
}

// LookupScram256 looks up a SCRAM-SHA-256 credential by username.
func (s *Store) LookupScram256(user string) (ScramAuth, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.scram256[user]
	return a, ok
}

// LookupScram512 looks up a SCRAM-SHA-512 credential by username.
func (s *Store) LookupScram512(user string) (ScramAuth, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.scram512[user]
	return a, ok
}

// DeleteScram256 removes a SCRAM-SHA-256 credential. Returns false if not found.
func (s *Store) DeleteScram256(user string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.scram256[user]; !ok {
		return false
	}
	delete(s.scram256, user)
	return true
}

// DeleteScram512 removes a SCRAM-SHA-512 credential. Returns false if not found.
func (s *Store) DeleteScram512(user string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.scram512[user]; !ok {
		return false
	}
	delete(s.scram512, user)
	return true
}

// ScramUserInfo holds mechanism and iteration count for a SCRAM user.
type ScramUserInfo struct {
	Mechanism  int8 // 1=SHA-256, 2=SHA-512
	Iterations int32
}

// ListScramUsers returns all SCRAM users with their credential info.
func (s *Store) ListScramUsers() map[string][]ScramUserInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string][]ScramUserInfo)
	for u, a := range s.scram256 {
		result[u] = append(result[u], ScramUserInfo{Mechanism: 1, Iterations: int32(a.Iterations)})
	}
	for u, a := range s.scram512 {
		result[u] = append(result[u], ScramUserInfo{Mechanism: 2, Iterations: int32(a.Iterations)})
	}
	return result
}

// ScramUserInfoFor returns SCRAM credential info for a specific user.
func (s *Store) ScramUserInfoFor(user string) []ScramUserInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var infos []ScramUserInfo
	if a, ok := s.scram256[user]; ok {
		infos = append(infos, ScramUserInfo{Mechanism: 1, Iterations: int32(a.Iterations)})
	}
	if a, ok := s.scram512[user]; ok {
		infos = append(infos, ScramUserInfo{Mechanism: 2, Iterations: int32(a.Iterations)})
	}
	return infos
}
