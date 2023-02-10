//go:build !nosystemd && linux
// +build !nosystemd,linux

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/coreos/go-systemd/v22/sdjournal"
)

// timeNow is a test fake injection point.
var timeNow = time.Now

// A SystemdLogSource reads log records from the given Systemd
// journal.
type SystemdLogSource struct {
	jReader *sdjournal.JournalReader
	path    string
	untilCh chan time.Time
	entryCh chan string
	errCh   chan error
}

// NewSystemdLogSource returns a log source for reading Systemd
// journal entries. `unit` and `slice` provide filtering if non-empty
// (with `slice` taking precedence).
func NewSystemdLogSource(path, unit, slice string) (*SystemdLogSource, error) {
	jrConfig := sdjournal.JournalReaderConfig{
		NumFromTail: 1,
		Path:        path,
		Formatter: func(e *sdjournal.JournalEntry) (string, error) {
			ts := time.Unix(0, int64(e.RealtimeTimestamp)*int64(time.Microsecond))
			return fmt.Sprintf(
				"%s %s %s[%s]: %s",
				ts.Format(time.Stamp),
				e.Fields["_HOSTNAME"],
				e.Fields["SYSLOG_IDENTIFIER"],
				e.Fields["_PID"],
				e.Fields["MESSAGE"],
			), nil
		},
	}

	if slice != "" {
		jrConfig.Matches = []sdjournal.Match{{Field: "_SYSTEMD_SLICE", Value: slice}}
	} else if unit != "" {
		jrConfig.Matches = []sdjournal.Match{{Field: "_SYSTEMD_UNIT", Value: unit}}
	}

	jr, err := sdjournal.NewJournalReader(jrConfig)
	if err != nil {
		return nil, err
	}

	logSrc := &SystemdLogSource{
		jReader: jr,
		path:    path,
		untilCh: make(chan time.Time),
		entryCh: make(chan string),
		errCh:   make(chan error),
	}

	go func() {
		w := journalEntryWriter{logSrc.entryCh}
		if err := logSrc.jReader.Follow(logSrc.untilCh, w); err != nil {
			logSrc.errCh <- err
		}
	}()

	return logSrc, nil
}

func (s *SystemdLogSource) Close() error {
	// Stop JournalReader follower
	s.untilCh <- time.Now()
	return s.jReader.Close()
}

func (s *SystemdLogSource) Path() string {
	return s.path
}

func (s *SystemdLogSource) Read(ctx context.Context) (string, error) {
	select {
	case e := <-s.entryCh:
		return e, nil
	case err := <-s.errCh:
		return "", err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// A systemdLogSourceFactory is a factory that can create
// SystemdLogSources from command line flags.
type systemdLogSourceFactory struct {
	enable            bool
	unit, slice, path string
}

func (f *systemdLogSourceFactory) Init(app *kingpin.Application) {
	app.Flag("systemd.enable", "Read from the systemd journal instead of log").Default("false").BoolVar(&f.enable)
	app.Flag("systemd.unit", "Name of the Postfix systemd unit.").Default("postfix.service").StringVar(&f.unit)
	app.Flag("systemd.slice", "Name of the Postfix systemd slice. Overrides the systemd unit.").Default("").StringVar(&f.slice)
	app.Flag("systemd.journal_path", "Path to the systemd journal").Default("").StringVar(&f.path)
}

func (f *systemdLogSourceFactory) New(ctx context.Context) (LogSourceCloser, error) {
	if !f.enable {
		return nil, nil
	}

	log.Println("Reading log events from systemd")
	return NewSystemdLogSource(f.path, f.unit, f.slice)
}

// journalEntryWriter implements the io.Writer interface.
type journalEntryWriter struct {
	entryCh chan string
}

func (w journalEntryWriter) Write(b []byte) (int, error) {
	w.entryCh <- string(b)
	return len(b), nil
}

func init() {
	RegisterLogSourceFactory(&systemdLogSourceFactory{})
}
