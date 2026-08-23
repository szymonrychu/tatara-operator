package restapi_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/restapi"
)

// taskListResp mirrors the #641 envelope. It is redeclared here rather than
// exported from the package: the wire shape is the contract, and a test that
// unmarshals the same struct the handler marshals proves nothing about it.
type taskListResp struct {
	Tasks     []restapi.TaskDTO `json:"tasks"`
	Total     int               `json:"total"`
	Returned  int               `json:"returned"`
	Offset    int               `json:"offset"`
	Truncated bool              `json:"truncated"`
}

// goalBytes is the CRD ceiling on spec.goal. The population that produced the
// 725 KB response had a median goal of 9,985 B against this max.
const goalBytes = 16384

// fatTask is one Task shaped like the ones actually in the tatara namespace:
// a goal at the CRD ceiling, a full note history, and status conditions.
func fatTask(name, projectRef string, createdAt time.Time) *tatarav1alpha1.Task {
	head := "# " + name + " long goal\n"
	goal := head + strings.Repeat("x", goalBytes-len(head))
	notes := make([]tatarav1alpha1.Note, 0, 8)
	for i := 0; i < 8; i++ {
		notes = append(notes, tatarav1alpha1.Note{
			ID:    fmt.Sprintf("n-%s-%d", name, i),
			At:    metav1.NewTime(createdAt.Add(time.Duration(i) * time.Minute)),
			Agent: "implement",
			Kind:  "note",
			Body:  strings.Repeat("n", 2000),
		})
	}
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "tatara",
			CreationTimestamp: metav1.NewTime(createdAt),
		},
		Spec: tatarav1alpha1.TaskSpec{ProjectRef: projectRef, RepositoryRef: "repo", Goal: goal},
		Status: tatarav1alpha1.TaskStatus{
			State: "implementing", AgentKind: "implement", Notes: notes,
			Conditions: []metav1.Condition{{
				Type: "Ready", Status: metav1.ConditionTrue, Reason: "Running",
				Message: strings.Repeat("c", 200), LastTransitionTime: metav1.NewTime(createdAt),
			}},
		},
	}
}

func fatTasks(n int, projectRef string) []client.Object {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	out := make([]client.Object, 0, n+1)
	out = append(out, project(projectRef))
	for i := 0; i < n; i++ {
		out = append(out, fatTask(fmt.Sprintf("t%03d", i), projectRef, base.Add(time.Duration(i)*time.Hour)))
	}
	return out
}

func getTaskList(t *testing.T, r *chi.Mux, query string) (taskListResp, int) {
	t.Helper()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/projects/alpha/tasks"+query, nil))
	require.Equal(t, http.StatusOK, w.Code)
	var out taskListResp
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	return out, w.Body.Len()
}

// TestListTasks_StaysUnderBundleBudget is the reproduction. These 120 fixtures
// measured 4,096,562 bytes through the pre-#641 handler; the bound is
// prompt.DefaultMaxBundleBytes (400000), the budget the platform already owns
// for delivering the same Task free text through the other channel.
//
// The assertion is on the response's BYTE LENGTH, not its field set: a field-set
// assertion passes the day someone adds a new long field.
//
// What it actually guards is the PROJECTION. Reverting A1 while keeping the page
// fails it (100 unprojected rows is ~3.4 MB); reverting the page while keeping
// the projection does not (120 projected rows is ~85 KB), and that direction is
// caught by TestListTasks_Accounting and TestListTasks_LimitBounds instead.
func TestListTasks_StaysUnderBundleBudget(t *testing.T) {
	r := buildRouter(t, fatTasks(120, "alpha")...)
	_, n := getTaskList(t, r, "")
	require.Less(t, n, 400_000, "task_list response must fit the operator's own 400000-byte budget")
}

func TestListTasks_Projection(t *testing.T) {
	r := buildRouter(t, fatTasks(1, "alpha")...)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/projects/alpha/tasks", nil))
	require.Equal(t, http.StatusOK, w.Code)

	var raw struct {
		Tasks []map[string]any `json:"tasks"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &raw))
	require.Len(t, raw.Tasks, 1)
	row := raw.Tasks[0]

	require.NotContains(t, row, "goal")
	require.Contains(t, row, "title")
	require.Contains(t, row, "body")

	status, ok := row["status"].(map[string]any)
	require.True(t, ok)
	require.NotContains(t, status, "notes")
	require.NotContains(t, status, "conditions")
	require.Equal(t, "implementing", status["state"])
}

// TestGetTask_KeepsFullGoal is the pre-mortem 1 guard: the list projection must
// not reach toTaskDTO, whose five single-Task callers return the goal on purpose.
func TestGetTask_KeepsFullGoal(t *testing.T) {
	objs := fatTasks(1, "alpha")
	r := buildRouter(t, objs...)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/tasks/t000", nil))
	require.Equal(t, http.StatusOK, w.Code)

	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Len(t, out.Goal, goalBytes)
	require.Len(t, out.Status.Notes, 8)
	require.Len(t, out.Status.Conditions, 1)
}

func TestListTasks_Accounting(t *testing.T) {
	tests := []struct {
		name          string
		tasks         int
		query         string
		wantReturned  int
		wantOffset    int
		wantTruncated bool
	}{
		{"under limit", 7, "", 7, 0, false},
		{"exactly the limit", 100, "", 100, 0, false},
		{"over the limit", 137, "", 100, 0, true},
		{"second page", 137, "?offset=100", 37, 100, false},
		{"page in the middle", 137, "?limit=10&offset=50", 10, 50, true},
		{"offset past the end", 7, "?offset=99", 0, 99, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := buildRouter(t, fatTasks(tc.tasks, "alpha")...)
			got, _ := getTaskList(t, r, tc.query)
			require.Equal(t, tc.tasks, got.Total)
			require.Equal(t, tc.wantReturned, got.Returned)
			require.Len(t, got.Tasks, tc.wantReturned)
			require.Equal(t, tc.wantOffset, got.Offset)
			require.Equal(t, tc.wantTruncated, got.Truncated)
		})
	}
}

// TestListTasks_NewestFirst pins the ordering to prompt.RenderIndex's: newest
// first. Oldest-first-then-truncate is #636's defect one endpoint over, and on a
// retention-bound population the oldest rows are the parked ones nobody asked for.
func TestListTasks_NewestFirst(t *testing.T) {
	r := buildRouter(t, fatTasks(5, "alpha")...)
	got, _ := getTaskList(t, r, "?limit=3")
	require.Equal(t, []string{"t004", "t003", "t002"},
		[]string{got.Tasks[0].Name, got.Tasks[1].Name, got.Tasks[2].Name})
}

func TestListTasks_LimitBounds(t *testing.T) {
	r := buildRouter(t, fatTasks(510, "alpha")...)
	for _, tc := range []struct {
		query string
		want  int
	}{
		{"", 100},
		{"?limit=0", 100},
		{"?limit=-5", 100},
		{"?limit=nonsense", 100},
		{"?limit=9999", 500},
		{"?limit=500", 500},
		{"?offset=-1&limit=1", 1},
	} {
		got, _ := getTaskList(t, r, tc.query)
		require.Equal(t, tc.want, got.Returned, "query %q", tc.query)
		require.Equal(t, 510, got.Total)
	}
}

func TestListTasks_FiltersByProjectBeforeCounting(t *testing.T) {
	objs := fatTasks(3, "alpha")
	objs = append(objs, project("beta"), fatTask("b000", "beta", time.Now()))
	r := buildRouter(t, objs...)
	got, _ := getTaskList(t, r, "")
	require.Equal(t, 3, got.Total)
	require.Equal(t, 3, got.Returned)
	for _, d := range got.Tasks {
		require.Equal(t, "alpha", d.ProjectRef)
	}
}

// TestResponseBytesObserved is D1. It pins all three claims the middleware
// makes: every route in the group is metered, the byte count is exact, and the
// label is the chi route TEMPLATE - so the series count stays at routes()' 16
// entries and never grows with a Task or Project name. That last one is the
// regression that matters: replacing RoutePattern() with r.URL.Path, or moving
// the middleware above the routing, mints one series per URL param value.
func TestResponseBytesObserved(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := obs.NewOperatorMetrics(reg)

	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	fc := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(fatTasks(3, "alpha")...).
		WithStatusSubresource(&tatarav1alpha1.Project{}, &tatarav1alpha1.Task{}).
		Build()
	s := restapi.NewServer(restapi.Config{Client: fc, Namespace: "tatara", Metrics: m})
	r := chi.NewRouter()
	s.Mount(r, nil)

	get := func(path string) int {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		require.Equal(t, http.StatusOK, w.Code, path)
		return w.Body.Len()
	}
	listBytes := get("/projects/alpha/tasks")
	// Three DIFFERENT param values on one template, plus a second template.
	taskBytes := get("/tasks/t000") + get("/tasks/t001") + get("/tasks/t002")

	count, sum := histogramFor(t, reg, "operator_restapi_response_bytes", "/projects/{p}/tasks")
	require.Equal(t, uint64(1), count)
	require.Equal(t, float64(listBytes), sum)

	count, sum = histogramFor(t, reg, "operator_restapi_response_bytes", "/tasks/{t}")
	require.Equal(t, uint64(3), count, "three param values must share one template series")
	require.Equal(t, float64(taskBytes), sum)

	require.Equal(t, []string{"/projects/{p}/tasks", "/tasks/{t}"},
		routeLabels(t, reg, "operator_restapi_response_bytes"),
		"the label must be the route template, never an interpolated URL")
}

func routeLabels(t *testing.T, reg *prometheus.Registry, name string) []string {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	out := []string{}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "route" {
					out = append(out, l.GetValue())
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

func histogramFor(t *testing.T, reg *prometheus.Registry, name, route string) (uint64, float64) {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "route" && l.GetValue() == route {
					return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
				}
			}
		}
	}
	t.Fatalf("no %s series with route=%q", name, route)
	return 0, 0
}
