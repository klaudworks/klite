package sasl

import (
	"sync"
)

type Store struct {
	mu       sync.RWMutex
	plain    map[string]string    // username -> password
	scram256 map[string]ScramAuth // username -> SCRAM-SHA-256 credential
	scram512 map[string]ScramAuth // username -> SCRAM-SHA-512 credential
}

func NewStore() *Store {
	return &Store{
		plain:    make(map[string]string),
		scram256: make(map[string]ScramAuth),
		scram512: make(map[string]ScramAuth),
	}
}

func (s *Store) Empty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.plain) == 0 && len(s.scram256) == 0 && len(s.scram512) == 0
}

func (s *Store) AddPlain(user, pass string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.plain[user] = pass
}

func (s *Store) LookupPlain(user, pass string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.plain[user]
	return ok && p == pass
}

func (s *Store) AddScram256(user string, auth ScramAuth) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scram256[user] = auth
}

func (s *Store) AddScram512(user string, auth ScramAuth) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scram512[user] = auth
}

func (s *Store) LookupScram256(user string) (ScramAuth, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.scram256[user]
	return a, ok
}

func (s *Store) LookupScram512(user string) (ScramAuth, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.scram512[user]
	return a, ok
}

func (s *Store) DeleteScram256(user string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.scram256[user]; !ok {
		return false
	}
	delete(s.scram256, user)
	return true
}

func (s *Store) DeleteScram512(user string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.scram512[user]; !ok {
		return false
	}
	delete(s.scram512, user)
	return true
}

type ScramUserInfo struct {
	Mechanism  int8 // 1=SHA-256, 2=SHA-512
	Iterations int32
}

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
