package fusefs

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestSuppressWriter_DropsUnimplementedOpcode(t *testing.T) {
	var buf bytes.Buffer
	sw := suppressWriter{}
	// Redirect slog output to buf for testing.
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))

	lines := []string{
		"Unimplemented opcode OPCODE-60",
		"Unimplemented opcode OPCODE-61",
		"some other message",
	}
	for _, line := range lines {
		n, err := sw.Write([]byte(line))
		if err != nil {
			t.Fatalf("Write error: %v", err)
		}
		if n != len(line) {
			t.Errorf("Write returned %d, want %d", n, len(line))
		}
	}

	output := buf.String()
	if strings.Contains(output, "OPCODE-60") || strings.Contains(output, "OPCODE-61") {
		t.Errorf("suppressed opcodes leaked to slog: %s", output)
	}
	if !strings.Contains(output, "some other message") {
		t.Errorf("non-suppressed message not forwarded: %s", output)
	}
}

func TestSuppressWriter_PassesThroughNormalMessages(t *testing.T) {
	var buf bytes.Buffer
	sw := suppressWriter{}
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))

	n, err := sw.Write([]byte("normal log message"))
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if n != len("normal log message") {
		t.Errorf("Write returned %d, want %d", n, len("normal log message"))
	}

	if !strings.Contains(buf.String(), "normal log message") {
		t.Errorf("expected message in output, got: %s", buf.String())
	}
}

func TestNewSuppressUnimplementedLogger(t *testing.T) {
	logger := newSuppressUnimplementedLogger()
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}

	// Verify the logger writes through suppressWriter without panicking.
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))

	logger.Printf("Unimplemented opcode OPCODE-60")
	logger.Printf("some debug message")

	if strings.Contains(buf.String(), "OPCODE-60") {
		t.Errorf("suppressed opcode leaked: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "some debug message") {
		t.Errorf("normal message not forwarded: %s", buf.String())
	}
}

func TestSuppressWriter_DropsLogDefaultWithLstdFlags(t *testing.T) {
	// log.Default() uses LstdFlags which prepends date/time. Verify that
	// suppressWriter still matches "Unimplemented opcode" in the middle
	// of a line (not just at the start).
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))

	sw := suppressWriter{}

	// Simulate what log.Default().Printf would write: date + message.
	line := "2026/05/25 10:58:02 Unimplemented opcode OPCODE-60\n"
	n, err := sw.Write([]byte(line))
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if n != len(line) {
		t.Errorf("Write returned %d, want %d", n, len(line))
	}

	// Also test a normal log line with LstdFlags prefix.
	normalLine := "2026/05/25 10:58:02 some normal message\n"
	n, err = sw.Write([]byte(normalLine))
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if n != len(normalLine) {
		t.Errorf("Write returned %d, want %d", n, len(normalLine))
	}

	output := buf.String()
	if strings.Contains(output, "OPCODE-60") {
		t.Errorf("suppressed opcode leaked: %s", output)
	}
	if !strings.Contains(output, "some normal message") {
		t.Errorf("normal message not forwarded: %s", output)
	}
}

func TestSuppressWriter_DropsWithDatePrefix(t *testing.T) {
	// log.Default() uses LstdFlags which adds "YYYY/MM/DD HH:MM:SS " prefix.
	// suppressWriter must catch "Unimplemented opcode" even when it appears
	// after a date/time prefix, not just at the start of the line.
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	sw := suppressWriter{}

	// Simulate what log.Default().Printf would write with LstdFlags:
	// "2026/05/25 10:58:02 Unimplemented opcode OPCODE-60\n"
	lines := []string{
		"2026/05/25 10:58:02 Unimplemented opcode OPCODE-60\n",
		"2026/05/25 10:58:02 Unimplemented opcode OPCODE-61\n",
		"2026/05/25 10:58:02 some normal message\n",
	}
	for _, line := range lines {
		sw.Write([]byte(line))
	}

	output := buf.String()
	if strings.Contains(output, "OPCODE-60") || strings.Contains(output, "OPCODE-61") {
		t.Errorf("suppressed opcodes leaked through: %s", output)
	}
	if !strings.Contains(output, "some normal message") {
		t.Errorf("normal message not forwarded: %s", output)
	}
}