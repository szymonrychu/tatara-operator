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

// script is every `run:` block in the job, concatenated, with shell COMMENT
// LINES REMOVED. The comments in these jobs discuss the very flags the tests
// match on - `image-verify` carries a paragraph about why it must not use
// `--output type=cacheonly` - so matching raw text would assert against prose
// and report the opposite of the truth.
func (j workflowJob) script() string {
	var sb strings.Builder
	for _, s := range j.Steps {
		for _, line := range strings.Split(s.Run, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "#") {
				continue
			}
			sb.WriteString(line)
			sb.WriteString("\n")
		}
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
func TestWorkflowText_PullRequestGateCompilesTheDockerfile(t *testing.T) {
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

// A PR-time build must not publish: release.yml plus the push `image` job own
// every tag that reaches Harbor, and Harbor tags are immutable.
//
// THIS IS THE TEST THAT FAILED TO DO ITS JOB, so it is worth being exact about
// what it now checks and why. It used to assert that the string
// `type=cacheonly` was present. That assertion was TRUE and the job was RED:
// buildkit v0.18.2's daemon has no `cacheonly` exporter and answered
// `exporter "cacheonly" could not be found` on every run. Probed against the
// live buildkitd on 2026-08-09 - cacheonly ABSENT; tar, image, local, oci
// present - and tar/oci stream the whole image to the client, which is real
// traffic per PR for nothing.
//
// So the verified shape is NO --output AT ALL: the graph is still solved in
// full and a failing step still exits non-zero (confirmed end to end against
// this repo's Dockerfile with the builder pinned below go.mod), while nothing
// is serialised and there is no exporter to later edit into a push.
//
// The test therefore asserts the ABSENCE of an exporter rather than the
// presence of a named one. That is deliberate: a name can only be trusted
// after it has been probed against the daemon, and this test cannot probe
// anything. Absence needs no daemon support to be correct. If a future change
// does add an exporter, this test fails and forces whoever adds it to justify
// it - and to go verify it against buildkitd, because nothing here can.
func TestWorkflowText_PullRequestDockerfileBuildPublishesNothing(t *testing.T) {
	wf := loadCIShared(t)
	for name, job := range wf.Jobs {
		if !job.buildsDockerfile() || job.pushOnly() {
			continue
		}
		s := job.script()
		if strings.Contains(s, "--output") {
			t.Errorf("job %q declares an --output on the pull_request path. The "+
				"verified non-publishing shape is no exporter at all; any "+
				"exporter name must first be probed against the buildkitd this "+
				"runs on (v0.18.2 has no `cacheonly`, which is how #556's first "+
				"cut shipped a job that was red on every PR): %s", name, s)
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
func TestWorkflowText_PullRequestBuildUsesTheSameRemoteContextAsPush(t *testing.T) {
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
func TestWorkflowText_PublishingImageJobStaysPushOnly(t *testing.T) {
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
