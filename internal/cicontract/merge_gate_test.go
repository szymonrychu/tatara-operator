package cicontract

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const ciSharedPath = "../../.github/workflows/ci-shared.yml"

type workflowStep struct {
	Run string `yaml:"run"`
	Env map[string]string
}

type workflowJob struct {
	If    string         `yaml:"if"`
	Needs any            `yaml:"needs"`
	Steps []workflowStep `yaml:"steps"`
}

type workflow struct {
	Jobs map[string]workflowJob `yaml:"jobs"`
}

func loadCIShared(t *testing.T) workflow {
	t.Helper()
	b, err := os.ReadFile(ciSharedPath)
	if err != nil {
		t.Fatalf("read %s: %v", ciSharedPath, err)
	}
	var wf workflow
	if err := yaml.Unmarshal(b, &wf); err != nil {
		t.Fatalf("parse %s: %v", ciSharedPath, err)
	}
	if len(wf.Jobs) == 0 {
		t.Fatalf("%s declares no jobs", ciSharedPath)
	}
	return wf
}

// script is every `run:` block in the job, concatenated.
func (j workflowJob) script() string {
	var sb strings.Builder
	for _, s := range j.Steps {
		sb.WriteString(s.Run)
		sb.WriteString("\n")
	}
	return sb.String()
}

// buildsDockerfile reports whether the job hands the Dockerfile to buildkit.
func (j workflowJob) buildsDockerfile() bool {
	s := j.script()
	return strings.Contains(s, "buildctl") && strings.Contains(s, "filename=Dockerfile")
}

// pushOnly reports whether the job can ONLY run on a push event - the condition
// that made the Dockerfile invisible to the merge gate.
func (j workflowJob) pushOnly() bool {
	return strings.Contains(j.If, "github.event_name == 'push'")
}

// The merge gate must compile the Dockerfile. Before #556 the only job that ran
// buildctl was `image`, gated on `github.event_name == 'push'`, so the artifact
// the merge produces was first built AFTER the merge - a Go bump could pass six
// green jobs and red main, and did, in three repos for ten days.
func TestPullRequestGateCompilesTheDockerfile(t *testing.T) {
	wf := loadCIShared(t)
	for _, job := range wf.Jobs {
		if job.buildsDockerfile() && !job.pushOnly() {
			return
		}
	}
	var built []string
	for name, job := range wf.Jobs {
		if job.buildsDockerfile() {
			built = append(built, name)
		}
	}
	t.Fatalf("no job compiles the Dockerfile on the pull_request path; "+
		"every buildctl job is gated on push: %v", built)
}

// A PR-time build must not publish, and must not be able to: the point is to
// compile the Dockerfile, and release.yml plus the push `image` job own every
// tag that reaches Harbor. Exporting nothing also keeps registry credentials
// off the PR path entirely.
func TestPullRequestDockerfileBuildPublishesNothing(t *testing.T) {
	wf := loadCIShared(t)
	for name, job := range wf.Jobs {
		if !job.buildsDockerfile() || job.pushOnly() {
			continue
		}
		s := job.script()
		if !strings.Contains(s, "type=cacheonly") {
			t.Errorf("job %q builds the Dockerfile on a PR but does not use "+
				"--output type=cacheonly", name)
		}
		if strings.Contains(s, "push=true") {
			t.Errorf("job %q pushes from the pull_request path", name)
		}
		for _, step := range job.Steps {
			for k := range step.Env {
				if strings.HasPrefix(k, "HARBOR_") {
					t.Errorf("job %q takes %s on the pull_request path; a "+
						"non-publishing build needs no registry credentials", name, k)
				}
			}
		}
	}
}

// The PR build has to exercise the SAME build the push job runs, or it
// certifies something else. Both must resolve buildkit's remote git context
// with GIT_AUTH_TOKEN as a frontend secret; a build from a local checkout would
// leave the clone-and-token path untested and pass while the push build fails.
func TestPullRequestBuildUsesTheSameRemoteContextAsPush(t *testing.T) {
	wf := loadCIShared(t)
	for name, job := range wf.Jobs {
		if !job.buildsDockerfile() || job.pushOnly() {
			continue
		}
		// Quote-agnostic: the shell line is --opt context="https://...".
		s := strings.ReplaceAll(job.script(), `"`, "")
		if !strings.Contains(s, "--opt context=https://github.com/szymonrychu/${REPO}.git#") {
			t.Errorf("job %q does not use the remote git context the push build uses", name)
		}
		if !strings.Contains(s, "--secret id=GIT_AUTH_TOKEN,env=GITHUB_TOKEN") {
			t.Errorf("job %q does not exercise the GIT_AUTH_TOKEN clone path", name)
		}
	}
}

// The publishing job stays push-only. Loosening IT instead of adding a
// non-publishing sibling would push a :SHORT_SHA tag from an unmerged PR into
// Harbor, where tags are immutable.
func TestPublishingImageJobStaysPushOnly(t *testing.T) {
	wf := loadCIShared(t)
	for name, job := range wf.Jobs {
		if !job.buildsDockerfile() || !strings.Contains(job.script(), "push=true") {
			continue
		}
		if !job.pushOnly() {
			t.Errorf("job %q pushes an image but is not gated on "+
				"github.event_name == 'push'", name)
		}
	}
}
