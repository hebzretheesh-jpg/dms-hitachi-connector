package storage

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type downloadCache struct {
	dir      string
	maxFiles int

	mu       sync.Mutex
	lru      *list.List
	entries  map[string]*list.Element
	inflight map[string]chan struct{}
}

type cacheEntry struct {
	key  string
	path string
}

func newDownloadCache(dir string, maxFiles int) (*downloadCache, error) {
	if maxFiles <= 0 || dir == "" {
		return nil, nil
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create download cache dir: %w", err)
	}

	cache := &downloadCache{
		dir:      dir,
		maxFiles: maxFiles,
		lru:      list.New(),
		entries:  make(map[string]*list.Element),
		inflight: make(map[string]chan struct{}),
	}

	if err := cache.loadExistingFiles(); err != nil {
		return nil, err
	}

	cache.mu.Lock()
	cache.evictLocked()
	cache.mu.Unlock()

	return cache, nil
}

func (c *downloadCache) GetOrCreate(ctx context.Context, key string, fill func(string) error) (string, error) {
	cacheKey := c.cacheKey(key)
	finalPath := filepath.Join(c.dir, cacheKey)

	for {
		c.mu.Lock()

		if _, err := os.Stat(finalPath); err == nil {
			c.touchLocked(cacheKey, finalPath)
			c.mu.Unlock()
			return finalPath, nil
		}

		if waitCh, ok := c.inflight[key]; ok {
			c.mu.Unlock()

			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-waitCh:
				continue
			}
		}

		waitCh := make(chan struct{})
		c.inflight[key] = waitCh
		c.mu.Unlock()

		err := c.fillCacheFile(finalPath, fill)

		c.mu.Lock()
		delete(c.inflight, key)
		close(waitCh)

		if err == nil {
			c.touchLocked(cacheKey, finalPath)
			c.evictLocked()
		}

		c.mu.Unlock()

		if err != nil {
			return "", err
		}

		return finalPath, nil
	}
}

func (c *downloadCache) fillCacheFile(finalPath string, fill func(string) error) error {
	tmpFile, err := os.CreateTemp(c.dir, "download-cache-*")
	if err != nil {
		return fmt.Errorf("create cache temp file: %w", err)
	}

	tmpPath := tmpFile.Name()
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close cache temp file: %w", err)
	}

	defer os.Remove(tmpPath)

	if err := fill(tmpPath); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("move cache file into place: %w", err)
	}

	return nil
}

func (c *downloadCache) touchLocked(key, cachedPath string) {
	if elem, ok := c.entries[key]; ok {
		entry := elem.Value.(*cacheEntry)
		entry.path = cachedPath
		c.lru.MoveToFront(elem)
		return
	}

	c.entries[key] = c.lru.PushFront(&cacheEntry{
		key:  key,
		path: cachedPath,
	})
}

func (c *downloadCache) evictLocked() {
	for len(c.entries) > c.maxFiles {
		elem := c.lru.Back()
		if elem == nil {
			return
		}

		entry := elem.Value.(*cacheEntry)
		delete(c.entries, entry.key)
		c.lru.Remove(elem)
		_ = os.Remove(entry.path)
	}
}

func (c *downloadCache) cachePath(key string) string {
	return filepath.Join(c.dir, c.cacheKey(key))
}

func (c *downloadCache) cacheKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func (c *downloadCache) loadExistingFiles() error {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return fmt.Errorf("read download cache dir: %w", err)
	}

	type cachedFile struct {
		name    string
		modTime int64
	}

	files := make([]cachedFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("read download cache entry info: %w", err)
		}

		files = append(files, cachedFile{
			name:    entry.Name(),
			modTime: info.ModTime().UnixNano(),
		})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].modTime > files[j].modTime
	})

	for _, file := range files {
		c.entries[file.name] = c.lru.PushBack(&cacheEntry{
			key:  file.name,
			path: filepath.Join(c.dir, file.name),
		})
	}

	return nil
}
