package controller

import (
	"context"
	"regexp"
	"strings"

	"github.com/szymonrychu/tatara-operator/internal/scm"
)

const (
	// helmfileRepoName is the terminal CD repo every component cascade ends at: a
	// successful apply.yaml run there is the authoritative cluster-applied signal.
	helmfileRepoName = "tatara-helmfile"
	// applyWorkflowFile is the tatara-helmfile push-to-main apply workflow.
	applyWorkflowFile = "apply.yaml"
)

// deployPinFiles are the tatara-helmfile files where component version pins land
// (the cd-release `bump` targets, parentMap). deploy-supervision reads them at a
// successful apply commit to confirm a Task's published version was applied. This
// is the one place the operator (the terminal watcher) couples to helmfile
// layout; keep it in lockstep with tatara-helmfile's pin locations.
var deployPinFiles = []string{
	"helmfile.yaml.gotmpl",
	"values/tatara-operator/common.yaml",
	"values/tatara-operator/default.yaml",
	"values/project-tatara/common.yaml",
	"values/project-infrastructure/common.yaml",
}

// mergePRSquash is the SINGLE writer.Merge call site in the controller. Every
// operator-driven merge funnels through here: the issueLifecycle drain
// (handleMerge), the review-approved deploy supervisor (superviseApprovedPRs) and
// the task-centric merge stage (StageDriver.ReconcileMerging). This keeps "agents
// never merge" structural (there is exactly one merge egress) and satisfies the
// acceptance grep. Squash is the fixed method: push-CD cuts one commit per merged
// change.
//
// expectedHeadSHA PINS the merge to the head the caller verified: the forge
// answers 409 when the head moved under it, which surfaces as scm.ErrHeadMoved so
// the caller re-reviews the new head instead of shipping unreviewed code. An
// EMPTY expectedHeadSHA means "no pin" - the two pre-cutover call sites pass it,
// preserving their behaviour exactly.
func mergePRSquash(ctx context.Context, writer scm.SCMWriter, repoURL, token string, number int, expectedHeadSHA string) (string, error) {
	return writer.Merge(ctx, repoURL, token, number, "squash", expectedHeadSHA)
}

// releaseArtifact maps a tatara-helmfile release name to the component artifact
// (repo) whose version its chart `version:` line pins. Chart-version pin lines
// carry no artifact token themselves (just `version: X.Y.Z`), so they are
// attributed to the artifact of the enclosing `- name: <release>` block during
// the apply sweep. Keep in lockstep with parentMap's helmfile chart pins.
var releaseArtifact = map[string]string{
	"tatara-operator":        "tatara-operator",
	"project-tatara":         "tatara-operator",
	"project-infrastructure": "tatara-operator",
}

// helmfileReleaseRe matches a `- name: <release>` line in helmfile.yaml.gotmpl so
// the chart `version:` pin that follows can be attributed to the right component.
var helmfileReleaseRe = regexp.MustCompile(`^\s*-\s*name:\s*(\S+)\s*$`)

// isVersionByte reports whether b can be part of a semver token (digit or dot),
// used as the token boundary so v1.4.1 does not match inside v1.4.10.
func isVersionByte(b byte) bool {
	return (b >= '0' && b <= '9') || b == '.'
}

// tokenMatch reports whether tok occurs in s as a whole version token: the
// characters immediately before and after the match must not be semver bytes.
// This is the substring fix - v1.4.1 no longer matches v1.4.10 (trailing '0' is a
// version byte) while v1.4.0 still matches `tag: "v1.4.0"` (trailing '"' is not).
func tokenMatch(s, tok string) bool {
	if tok == "" {
		return false
	}
	for idx := 0; ; {
		i := strings.Index(s[idx:], tok)
		if i < 0 {
			return false
		}
		i += idx
		var before, after byte = ' ', ' '
		if i > 0 {
			before = s[i-1]
		}
		if i+len(tok) < len(s) {
			after = s[i+len(tok)]
		}
		if !isVersionByte(before) && !isVersionByte(after) {
			return true
		}
		idx = i + 1
	}
}

// lineCarriesVersion reports whether line carries version as a whole token, in
// either the v-prefixed (image tag) or bare (chart version) form.
func lineCarriesVersion(line, version, bare string) bool {
	if tokenMatch(line, version) {
		return true
	}
	return bare != version && tokenMatch(line, bare)
}

// pinCarriesArtifactVersion reports whether the applied helmfile pin state
// carries version on a pin line that belongs to artifact (the component repo
// name). This scopes the apply-outcome match to the entry's OWN pin so a sibling
// component sharing the same version string (plausible while every repo is seeded
// near low semvers) cannot prematurely resolve the wrong Task. Two attribution
// rules cover every parentMap pin shape:
//
//   - image pins embed the artifact in the image path: a line containing
//     "/<artifact>:" with the version as a whole token (e.g.
//     ".../tatara-memory:v1.4.0"). The trailing ':' keeps tatara-memory from
//     matching tatara-memory-repo-ingester.
//   - chart-version pins carry no artifact token, so they are attributed to the
//     artifact of the enclosing helmfile `- name: <release>` block (the operator
//     chart's bare version equals its image version, so the chart line alone
//     confirms the operator cascade without needing the artifact-token-less
//     `tag:` line in values/tatara-operator/common.yaml).
func pinCarriesArtifactVersion(pinState, artifact, version string) bool {
	if version == "" || artifact == "" {
		return false
	}
	bare := strings.TrimPrefix(version, "v")
	imageToken := "/" + artifact + ":"
	currentRelease := ""
	for _, line := range strings.Split(pinState, "\n") {
		if m := helmfileReleaseRe.FindStringSubmatch(line); m != nil {
			currentRelease = m[1]
			continue
		}
		if strings.Contains(line, imageToken) && lineCarriesVersion(line, version, bare) {
			return true
		}
		if releaseArtifact[currentRelease] == artifact && lineCarriesVersion(line, version, bare) {
			return true
		}
	}
	return false
}

// helmfilePinState concatenates the deploy pin files at ref into one string so a
// version substring match confirms a pin was applied. Missing files (404) are
// skipped; GetFileContent returns "" for them.
func helmfilePinState(ctx context.Context, dw scm.DeployWatcher, owner, repo, ref string) (string, error) {
	var b strings.Builder
	for _, f := range deployPinFiles {
		content, err := dw.GetFileContent(ctx, owner, repo, f, ref)
		if err != nil {
			return "", err
		}
		b.WriteString(content)
		b.WriteByte('\n')
	}
	return b.String(), nil
}

// shortSHA trims a commit SHA to 7 chars for human-facing comments.
func shortSHA(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}
