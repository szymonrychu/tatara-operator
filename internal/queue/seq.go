package queue

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// seqRetry is a backoff with enough steps for high-concurrency CAS loops.
// 30 steps at 5ms base with factor 1.5 covers bursts up to ~32 replicas
// contending on a single per-project counter; richer than retry.DefaultRetry
// (5 steps), which exhausts under modest concurrency and surfaces a Conflict.
var seqRetry = wait.Backoff{
	Duration: 5 * time.Millisecond,
	Factor:   1.5,
	Jitter:   0.2,
	Steps:    30,
}

const (
	seqConfigMapPrefix = "queue-seq-"
	seqDataKey         = "next"
)

// seqConfigMapName returns the per-project counter ConfigMap name. The project
// name is sanitized to a DNS-1123 label so the ConfigMap is always nameable even
// for projects whose name (already a k8s object name) is reused with a prefix
// that could overflow the 63-char limit.
func seqConfigMapName(project string) string {
	return seqConfigMapPrefix + sanitizeDNS1123(project)
}

// sanitizeDNS1123 lowercases s, collapses every run of non-[a-z0-9] into a single
// '-', trims leading/trailing '-', and caps the result at 53 chars so the
// "queue-seq-" prefix (10 chars) keeps the full name within the 63-char limit.
// Project names are themselves DNS-1123 object names, so this is near-identity in
// practice; a counter collision between two distinct projects is not reachable.
func sanitizeDNS1123(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	prevDash := false
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevDash = false
		} else if !prevDash {
			b.WriteByte('-')
			prevDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if len(out) > 53 {
		out = strings.Trim(out[:53], "-")
	}
	return out
}

// SeqSource allocates strictly-increasing int64 sequence numbers PER PROJECT,
// stored in a per-project ConfigMap (queue-seq-<sanitized-project>). Any replica
// can call Next concurrently; CAS via RetryOnConflict serialises the counter
// across replicas without a leader dependency and without any in-memory state.
type SeqSource struct {
	Client    client.Client
	Namespace string
}

// Next atomically allocates the next sequence number for the given project.
// The counters are independent per project (project A and project B each start
// at 1). The per-project ConfigMap is intentionally NOT owner-referenced to the
// Project: ProjectReconciler uses Owns(&corev1.ConfigMap{}), so an owned CM would
// trigger a reconcile on every CAS Update (storm). The CM is orphaned when its
// Project is deleted; that is acceptable (a stale counter at most).
func (s *SeqSource) Next(ctx context.Context, project string) (int64, error) {
	name := seqConfigMapName(project)
	var allocated int64
	err := retry.RetryOnConflict(seqRetry, func() error {
		var cm corev1.ConfigMap
		err := s.Client.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: name}, &cm)
		if apierrors.IsNotFound(err) {
			// First ever allocation for this project: try to create with next=1.
			newCM := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: s.Namespace,
				},
				Data: map[string]string{seqDataKey: "1"},
			}
			createErr := s.Client.Create(ctx, newCM)
			if apierrors.IsAlreadyExists(createErr) {
				// Lost the create race; return a synthetic Conflict so
				// RetryOnConflict re-Gets the winner's CM.
				return apierrors.NewConflict(corev1.Resource("configmaps"), name, createErr)
			}
			if createErr != nil {
				return createErr
			}
			allocated = 1
			return nil
		}
		if err != nil {
			return err
		}
		// CM exists: parse current value, increment, update.
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		// An empty/missing key is legitimately 0 (-> allocate 1). A present-but-
		// unparseable value is a hard error: do NOT swallow it (repo rule).
		val := cm.Data[seqDataKey]
		var cur int64
		if val != "" {
			parsed, parseErr := strconv.ParseInt(val, 10, 64)
			if parseErr != nil {
				return fmt.Errorf("queue: corrupt seq counter for project %s: %q: %w", project, val, parseErr)
			}
			cur = parsed
		}
		n := cur + 1
		cm.Data[seqDataKey] = strconv.FormatInt(n, 10)
		if updateErr := s.Client.Update(ctx, &cm); updateErr != nil {
			return updateErr
		}
		allocated = n
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("queue: allocate seq for project %s: %w", project, err)
	}
	return allocated, nil
}
