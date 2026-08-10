package webhook_test

// The switch in handle() is the thing PR A actually repairs: a check_suite
// delivery reached `default: s.accept(..., "ignored")` and died there. Calling
// handleCIStatus directly (ci_status_test.go) proves the handler; only a real
// signed POST proves the ROUTE, and the route is what was broken.

import (
	"context"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func checkSuiteBody(headSHA, conclusion string) []byte {
	return []byte(`{"action":"completed","check_suite":{"head_sha":"` + headSHA +
		`","status":"completed","conclusion":"` + conclusion + `"},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
}

func mirroredMR(repoName, projName string, number int, headSHA string) *tatarav1.MergeRequest {
	return &tatarav1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1.MergeRequestName(repoName, number), Namespace: ns},
		Spec: tatarav1.MergeRequestSpec{
			RepositoryRef: repoName, ProjectRef: projName, Number: number,
			URL: "https://github.com/o/r/pull/" + strconv.Itoa(number),
		},
		Status: tatarav1.MergeRequestStatus{State: "open", HeadSHA: headSHA},
	}
}

func TestCheckSuiteDelivery_RoutesToTheCIStampAndIsNotIgnored(t *testing.T) {
	const secretVal = "whsec-ci1"
	const repoName = "repo-ci1"
	const projName = "cip1"
	mr := mirroredMR(repoName, projName, 60, "head-sha-1")
	c := seedClient(t,
		project(projName, projName+"-scm", "tatara"),
		secret(projName+"-scm", secretVal),
		repository(repoName, projName, "https://github.com/o/r.git", "main"),
		mr,
	)
	h := newServerWithSpiller(c)

	body := checkSuiteBody("head-sha-1", "failure")
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "check_suite")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))
	w := post(t, h, projName, hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var got tatarav1.MergeRequest
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: mr.Name}, &got))
	require.Equal(t, "red", got.Status.CIStatus,
		"a check_suite delivery must reach the mirror instead of dying on the ignored arm")
	require.NotNil(t, got.Status.CIUpdatedAt)
}

// A GitLab Pipeline Hook takes the same route through the GitLab provider.
func TestPipelineHookDelivery_RoutesToTheCIStamp(t *testing.T) {
	const secretVal = "whsec-ci2"
	const repoName = "repo-ci2"
	const projName = "cip2"
	mr := mirroredMR(repoName, projName, 61, "head-sha-2")
	c := seedClient(t,
		project(projName, projName+"-scm", "tatara"),
		secret(projName+"-scm", secretVal),
		repository(repoName, projName, "https://gitlab.com/g/p.git", "main"),
		mr,
	)
	h := newServerWithSpiller(c)

	body := []byte(`{"object_kind":"pipeline","object_attributes":{"id":9,"sha":"head-sha-2","status":"success"},` +
		`"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"}}`)
	hdr := http.Header{}
	hdr.Set("X-Gitlab-Event", "Pipeline Hook")
	hdr.Set("X-Gitlab-Token", secretVal)
	w := post(t, h, projName, hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var got tatarav1.MergeRequest
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: mr.Name}, &got))
	require.Equal(t, "green", got.Status.CIStatus)
}
