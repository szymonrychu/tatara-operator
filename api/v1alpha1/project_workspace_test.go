package v1alpha1

import "testing"

// The workspace is OPTIONAL but ON BY DEFAULT, for the same reason memory is:
// spec.workspace.enabled is a *bool with no kubebuilder default precisely so nil
// is distinguishable from an explicit false. Every Project that predates the
// field - and every Project that never mentions spec.workspace at all - keeps a
// workspace volume.
func TestWorkspaceEnabled_DefaultsOnWhenUnset(t *testing.T) {
	tr, fa := true, false
	cases := []struct {
		name string
		p    *Project
		want bool
	}{
		{"nil project", nil, true},
		{"no spec.workspace at all", &Project{}, true},
		{"spec.workspace present, enabled unset", &Project{Spec: ProjectSpec{Workspace: &WorkspaceSpec{Size: "20Gi"}}}, true},
		{"enabled explicitly true", &Project{Spec: ProjectSpec{Workspace: &WorkspaceSpec{Enabled: &tr}}}, true},
		{"enabled explicitly false", &Project{Spec: ProjectSpec{Workspace: &WorkspaceSpec{Enabled: &fa}}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.p.WorkspaceEnabled(); got != tc.want {
				t.Fatalf("WorkspaceEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

// The build caches are the whole point of the feature, so they are on by
// default too - and independently switchable, because a project whose repos
// build nothing has no use for a 50Gi cache volume.
func TestWorkspaceCacheEnabled_DefaultsOnWhenUnset(t *testing.T) {
	tr, fa := true, false
	cases := []struct {
		name string
		p    *Project
		want bool
	}{
		{"nil project", nil, true},
		{"no spec.workspace at all", &Project{}, true},
		{"spec.workspace present, cacheEnabled unset", &Project{Spec: ProjectSpec{Workspace: &WorkspaceSpec{Size: "20Gi"}}}, true},
		{"cacheEnabled explicitly true", &Project{Spec: ProjectSpec{Workspace: &WorkspaceSpec{CacheEnabled: &tr}}}, true},
		{"cacheEnabled explicitly false", &Project{Spec: ProjectSpec{Workspace: &WorkspaceSpec{CacheEnabled: &fa}}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.p.WorkspaceCacheEnabled(); got != tc.want {
				t.Fatalf("WorkspaceCacheEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

// A kubebuilder default only lands if the LIVE CRD carries it, and structural
// pruning yields "" for a field the live CRD does not know. The Go-side
// constants are therefore the authority, exactly as internal/memory carries its
// own defaults despite the markers on MemorySpec.
func TestWorkspaceStringAccessors_FallBackToTheGoDefaults(t *testing.T) {
	cases := []struct {
		name                      string
		p                         *Project
		wantSC, wantSize, wantCSz string
	}{
		{"nil project", nil, DefaultWorkspaceStorageClass, DefaultWorkspaceSize, DefaultWorkspaceCacheSize},
		{"no spec.workspace", &Project{}, DefaultWorkspaceStorageClass, DefaultWorkspaceSize, DefaultWorkspaceCacheSize},
		{
			"pruned to empty strings",
			&Project{Spec: ProjectSpec{Workspace: &WorkspaceSpec{}}},
			DefaultWorkspaceStorageClass, DefaultWorkspaceSize, DefaultWorkspaceCacheSize,
		},
		{
			"explicit values win",
			&Project{Spec: ProjectSpec{Workspace: &WorkspaceSpec{
				StorageClass: "cephfs-other", Size: "1Gi", CacheSize: "2Gi",
			}}},
			"cephfs-other", "1Gi", "2Gi",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := WorkspaceStorageClass(tc.p); got != tc.wantSC {
				t.Fatalf("WorkspaceStorageClass() = %q, want %q", got, tc.wantSC)
			}
			if got := WorkspaceSize(tc.p); got != tc.wantSize {
				t.Fatalf("WorkspaceSize() = %q, want %q", got, tc.wantSize)
			}
			if got := WorkspaceCacheSize(tc.p); got != tc.wantCSz {
				t.Fatalf("WorkspaceCacheSize() = %q, want %q", got, tc.wantCSz)
			}
		})
	}
}

// The storage class is PINNED, not inherited from the cluster default: the
// workspace needs ReadWriteMany and a cluster default that silently became an
// RBD class would stall every respawn in Multi-Attach.
func TestDefaultWorkspaceStorageClass_IsTheRWXCephClass(t *testing.T) {
	if DefaultWorkspaceStorageClass != "rook-ceph-rwx" {
		t.Fatalf("DefaultWorkspaceStorageClass = %q, want rook-ceph-rwx", DefaultWorkspaceStorageClass)
	}
}
