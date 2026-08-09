package restapi_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// decodeErrLogLine returns the first JSON log record whose msg is want.
func decodeErrLogLine(t *testing.T, buf *bytes.Buffer, want string) map[string]any {
	t.Helper()
	for _, line := range strings.Split(buf.String(), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			continue
		}
		if rec["msg"] == want {
			return rec
		}
	}
	t.Fatalf("no log line with msg=%q in:\n%s", want, buf.String())
	return nil
}

// decodeErrEnv builds a v2 env whose logger writes JSON into buf, so a test can
// assert the LEVEL a line was emitted at and not just that it was emitted.
func decodeErrEnv(t *testing.T, buf *bytes.Buffer) *v2Env {
	t.Helper()
	h := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return buildV2(t, v2Opts{logger: slog.New(h)}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateUnderImplementation, "implement"))
}

func decodeErrBody(t *testing.T, raw []byte) string {
	t.Helper()
	var body map[string]string
	require.NoError(t, json.Unmarshal(raw, &body))
	return body["error"]
}

// A body the operator cannot decode is the CALLER's mistake, and #558 measured
// both halves of what that costs when the operator treats it as its own.
//
// The 400 said only "invalid JSON body", so the agent re-guessed the payload
// shape 5 times over 48s without ever being told which key was wrong; and every
// one of those attempts was logged at ERROR, so 5 caller mistakes counted as 5
// operator errors and held up "Tatara operator error recurring".
func TestPostNote_DecodeErrorNamesTheUnknownField(t *testing.T) {
	var buf bytes.Buffer
	e := decodeErrEnv(t, &buf)

	w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":"note","body":"x","extra":1}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, decodeErrBody(t, w.Body.Bytes()), `"extra"`,
		"the 400 must name the offending field, else the caller retries blind")
}

func TestPostNote_DecodeErrorIsNotAnOperatorError(t *testing.T) {
	var buf bytes.Buffer
	e := decodeErrEnv(t, &buf)

	w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":"note","body":"x","extra":1}`)
	require.Equal(t, http.StatusBadRequest, w.Code)

	rec := decodeErrLogLine(t, &buf, "restapi: decode body failed")
	require.NotEqual(t, slog.LevelError.String(), rec["level"],
		"a 4xx caused by the caller's own bytes is not an operator ERROR")
	require.Equal(t, "/tasks/t1/notes", rec["path"], "the line must still say which endpoint")
}

// A type mismatch names the field too - it is the other decoder rejection the
// wire actually produces, and it is just as blind without the field name.
func TestPostNote_DecodeErrorNamesTheMistypedField(t *testing.T) {
	var buf bytes.Buffer
	e := decodeErrEnv(t, &buf)

	w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":"note","body":42}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, decodeErrBody(t, w.Body.Bytes()), `"body"`,
		"a type mismatch must name the field whose type is wrong")
}

// Everything the decoder reports that is NOT a recognised, caller-derived shape
// degrades to the old generic text. A syntax error's message carries a byte
// offset into the caller's body and nothing else useful, and echoing raw
// decoder output is how internal type detail leaks to a caller.
func TestPostNote_DecodeErrorWithholdsUnrecognisedDecoderDetail(t *testing.T) {
	var buf bytes.Buffer
	e := decodeErrEnv(t, &buf)

	w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Equal(t, "invalid JSON body", decodeErrBody(t, w.Body.Bytes()))
}
