package controller

import (
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// terminalTaskAt builds an in-memory terminal (Succeeded) cron Task for the
// (repo,number) with the given creation time. mirrors mkCronTask + timestamp.
func terminalTaskAt(repo string, number int, created time.Time) tatarav1alpha1.Task {
	tk := mkCronTask(repo, number, "issueLifecycle", "", "Succeeded")
	tk.CreationTimestamp = metav1.NewTime(created)
	return tk
}

func TestLastTerminalNoLabelTask(t *testing.T) {
	base := time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC)
	managed := managedPhaseLabels(nil)

	t.Run("PR candidate -> nil", func(t *testing.T) {
		existing := []tatarav1alpha1.Task{terminalTaskAt("o/r", 5, base)}
		c := candidate{repo: "o/r", number: 5, isPR: true}
		if lastTerminalNoLabelTask(c, existing, managed) != nil {
			t.Fatal("PR candidate must not gate")
		}
	})

	t.Run("managed label present -> nil", func(t *testing.T) {
		existing := []tatarav1alpha1.Task{terminalTaskAt("o/r", 5, base)}
		c := candidate{repo: "o/r", number: 5, labels: []string{"tatara-implementation"}}
		if lastTerminalNoLabelTask(c, existing, managed) != nil {
			t.Fatal("managed label must not gate (orphan path owns it)")
		}
	})

	t.Run("non-terminal task present -> nil", func(t *testing.T) {
		existing := []tatarav1alpha1.Task{mkCronTask("o/r", 5, "issueLifecycle", "", "Planning")}
		c := candidate{repo: "o/r", number: 5}
		if lastTerminalNoLabelTask(c, existing, managed) != nil {
			t.Fatal("a non-terminal task must not gate (handled in-flight)")
		}
	})

	t.Run("no matching task -> nil", func(t *testing.T) {
		existing := []tatarav1alpha1.Task{terminalTaskAt("o/r", 99, base)}
		c := candidate{repo: "o/r", number: 5}
		if lastTerminalNoLabelTask(c, existing, managed) != nil {
			t.Fatal("genuinely-new issue must not gate")
		}
	})

	t.Run("single terminal, no label -> returns it", func(t *testing.T) {
		existing := []tatarav1alpha1.Task{terminalTaskAt("o/r", 5, base)}
		c := candidate{repo: "o/r", number: 5}
		got := lastTerminalNoLabelTask(c, existing, managed)
		if got == nil || !got.CreationTimestamp.Time.Equal(base) {
			t.Fatalf("want terminal task at %v, got %+v", base, got)
		}
	})

	t.Run("multiple terminal -> returns latest by creation", func(t *testing.T) {
		latest := base.Add(2 * time.Hour)
		existing := []tatarav1alpha1.Task{
			terminalTaskAt("o/r", 5, base),
			terminalTaskAt("o/r", 5, latest),
			terminalTaskAt("o/r", 5, base.Add(time.Hour)),
		}
		c := candidate{repo: "o/r", number: 5}
		got := lastTerminalNoLabelTask(c, existing, managed)
		if got == nil || !got.CreationTimestamp.Time.Equal(latest) {
			t.Fatalf("want latest terminal task at %v, got %+v", latest, got)
		}
	})
}
