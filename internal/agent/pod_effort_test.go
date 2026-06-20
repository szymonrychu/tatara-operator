package agent

import (
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

func envValue(env []corev1.EnvVar, name string) (string, bool) {
	for _, e := range env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

func TestBuildPod_SetsEffortEnv(t *testing.T) {
	tests := []struct {
		name   string
		effort string
		want   string
	}{
		{"xhigh default", "xhigh", "xhigh"},
		{"max", "max", "max"},
		{"empty still emitted", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proj := &tatarav1alpha1.Project{}
			proj.Name = "p"
			proj.Spec.Agent.Effort = tc.effort
			task := &tatarav1alpha1.Task{}
			task.Name = "t"
			pod := BuildPod(proj, nil, task, nil, "http://mem", PodConfig{})
			got, ok := envValue(pod.Spec.Containers[0].Env, "EFFORT")
			if !ok {
				t.Fatalf("EFFORT env not set")
			}
			if got != tc.want {
				t.Fatalf("EFFORT = %q, want %q", got, tc.want)
			}
		})
	}
}
