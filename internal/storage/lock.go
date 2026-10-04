package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const scheduleLockFileName = "schedule.lock"

// FileLock coordinates scheduler passes across multiple controller
// processes that share one data directory. It uses an exclusive,
// non-blocking flock: a waking controller that cannot acquire the lock
// simply skips its pass because another controller is already scanning.
// The kernel releases the lock when the process exits, including after a
// crash.
type FileLock struct {
	handle *os.File
}

// OpenScheduleLock creates (but does not acquire) the lock file beside the
// state snapshot.
func OpenScheduleLock(dataDir string) (*FileLock, error) {
	handle, err := os.OpenFile(
		filepath.Join(dataDir, scheduleLockFileName),
		os.O_CREATE|os.O_RDWR, 0o644,
	)
	if err != nil {
		return nil, fmt.Errorf("open schedule lock: %w", err)
	}
	return &FileLock{handle: handle}, nil
}

// TryLock acquires the exclusive lock without waiting. It reports false
// when another process already holds it.
func (l *FileLock) TryLock() (bool, error) {
	if err := syscall.Flock(int(l.handle.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if err == syscall.EWOULDBLOCK {
			return false, nil
		}
		return false, fmt.Errorf("acquire schedule lock: %w", err)
	}
	return true, nil
}

// Unlock releases the lock.
func (l *FileLock) Unlock() error {
	if err := syscall.Flock(int(l.handle.Fd()), syscall.LOCK_UN); err != nil {
		return fmt.Errorf("release schedule lock: %w", err)
	}
	return nil
}

// Close releases the lock (if held) and closes the file handle.
func (l *FileLock) Close() error {
	unlockErr := l.Unlock()
	closeErr := l.handle.Close()
	if unlockErr != nil {
		return unlockErr
	}
	return closeErr
}
