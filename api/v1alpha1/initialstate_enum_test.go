package v1alpha1_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// THE TWO initialState ENUMS ARE ONE FACT, AND enqueue.go IS WHY (#604).
//
// QueuedEventPayload.InitialState is copied VERBATIM onto TaskSpec.InitialState
// by internal/queue/enqueue.go. So a value the payload admits but the Task
// rejects does not fail at the queue - it fails at the Task CREATE, on every
// reconcile pass, forever. That is the stranded-object shape #604 fixed for the
// takeover Task, reproduced one enforcement site later and against an object
// nobody is watching.
//
// This reads the MARKERS rather than the generated CRDs on purpose: the markers
// are what a person edits, and `make manifests` regenerates the charts from
// them, so a drift introduced by hand is caught here before it is baked in.
func TestInitialStateEnumsAgreeAcrossTaskAndQueuedEvent(t *testing.T) {
	enumRe := regexp.MustCompile(`\+kubebuilder:validation:Enum=([^\s]+)\n[^\n]*\+optional\n[^\n]*InitialState string`)

	read := func(path string) string {
		t.Helper()
		b, err := os.ReadFile(path)
		require.NoErrorf(t, err, "read %s", path)
		m := enumRe.FindSubmatch(b)
		require.NotNilf(t, m, "no InitialState Enum marker found in %s", path)
		return string(m[1])
	}

	task := read("task_types.go")
	queued := read("queuedevent_types.go")

	require.Equalf(t, task, queued,
		"TaskSpec.InitialState admits %q but QueuedEventPayload.InitialState admits %q. "+
			"enqueue.go copies the payload value onto the Task VERBATIM, so a value only the "+
			"payload admits builds a Task the API server rejects on every reconcile pass forever",
		task, queued)

	// And `refined` is gone from both: it is the value #604 removed, because the
	// (create) -> refined edge no longer exists and an admitted spec whose edge
	// is missing is a permanent IllegalTransitionError loop.
	require.NotContainsf(t, task, "refined",
		"`refined` is back in the initialState enum. There is no (create) -> refined edge, so a "+
			"Task minted there fails stage.Enter on every pass and increments "+
			"operator_illegal_state_transition_total - whose alert blames the TABLE, not the spec (#604)")
}

// NO PROSE MAY RESTATE THE RETIRED initialState ENUM (#604 review, rounds 2 and
// 6). The test above pins the two markers against each other; this one pins the
// COMMENTS against the markers, because that is where the fact actually rotted.
//
// The enum lost its middle value when #604 deleted the (create) -> refined edge,
// and the old three-value spelling survived in doc blocks that no compiler,
// linter or `make manifests` reads - three of them, found in one review round
// after a grep the author had run and believed clean.
//
// WHAT IT CATCHES AND WHAT IT DOES NOT, stated because a guard nobody can scope
// is a guard people trust too far. It catches the exact retired spelling in .go
// and .md at ANY line wrapping - the content is stripped of whitespace and
// comment slashes before matching, because these copies live in hard-wrapped //
// blocks and a copy broken across two lines is exactly what the line-oriented
// grep missed. It does NOT catch a reworded restatement ("new, refined or
// under-implementation"), a restatement of the CURRENT enum that will rot at the
// next narrowing, or anything outside .go/.md. It is a ratchet against one known
// drift, not a general prohibition on copying facts into prose. Stripping the
// slashes also means it can fire on a PATH or on two unrelated adjacent comment
// lines; that direction is a loud false failure, not a silent miss.
//
// Deliberately NOT a search for `refined` at large: the state is alive and
// correctly named throughout, including in Status.State's own enum, which begins
// with the same three values and continues - hence the requirement that the
// sequence END at under-implementation.
func TestNoProseRestatesTheRetiredInitialStateEnum(t *testing.T) {
	root := moduleRoot(t)

	// The pattern below is itself the string, so this file has to be skipped. A
	// path literal rather than runtime.Caller: a rename then fails the test on
	// itself, which is noisy but safe, whereas the Caller form breaks SILENTLY if
	// -trimpath is ever added to the test target. The doc block and the failure
	// message are deliberately written so that they do NOT need the exemption.
	const self = "api/v1alpha1/initialstate_enum_test.go"

	// Strip whitespace and comment slashes so a hard-wrapped copy still matches.
	strip := regexp.MustCompile(`[\s/]+`)
	retired := regexp.MustCompile(`new;refined;under-implementation($|[^;])`)

	var found []string
	require.NoError(t, filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		if d.IsDir() {
			// docs/superpowers/plans is an archive: a frozen record of what was
			// true when it was written, not a claim about today.
			if rel == ".git" || rel == "bin" || rel == "docs/superpowers/plans" {
				return filepath.SkipDir
			}
			return nil
		}
		if rel == self {
			return nil
		}
		switch filepath.Ext(path) {
		case ".go", ".md":
		default:
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if retired.MatchString(strip.ReplaceAllString(string(b), "")) {
			found = append(found, rel)
		}
		return nil
	}))

	require.Emptyf(t, found,
		"these restate the RETIRED initialState enum, the three-value one that still listed "+
			"`refined`, which #604 narrowed when it deleted the (create) -> refined edge: %s.\n"+
			"Do not repair them by writing the new value list - that is the same copy, one "+
			"narrowing behind. POINT AT the kubebuilder Enum marker on TaskSpec.InitialState, "+
			"which is the fact", strings.Join(found, ", "))
}

// moduleRoot walks up from the test's working directory to the directory holding
// go.mod. This package runs in api/v1alpha1; the fact it guards is repo-wide.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqualf(t, parent, dir, "no go.mod above %s", dir)
		dir = parent
	}
}
