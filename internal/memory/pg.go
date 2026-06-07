package memory

import (
	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PGCluster builds the per-Project cnpg Cluster. cnpg's controller derives the
// mem-<proj>-pg-rw Service and the mem-<proj>-pg-app Secret (key "uri") that
// lightrag and tatara-memory consume. The vector extension is installed via
// postInitApplicationSQL on the tatara_memory database for lightrag's
// PGVectorStorage.
//
// cnpg v1.29.1 field adaptations vs the plan (written for v1.27.x):
// - Struct field names match exactly: Instances, StorageConfiguration.Size,
//   Bootstrap.InitDB.{Database,Owner,PostInitApplicationSQL}. No changes needed.
// - GroupVersion export: cnpgv1 exposes SchemeGroupVersion (not GroupVersion);
//   TypeMeta.APIVersion uses cnpgv1.SchemeGroupVersion.String().
func PGCluster(p *tatarav1alpha1.Project, cfg Config) *cnpgv1.Cluster {
	n := NamesFor(p.Name)
	return &cnpgv1.Cluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: cnpgv1.SchemeGroupVersion.String(),
			Kind:       "Cluster",
		},
		ObjectMeta: objectMeta(p, cfg, n.PGCluster),
		Spec: cnpgv1.ClusterSpec{
			Instances: pgInstances(p),
			StorageConfiguration: cnpgv1.StorageConfiguration{
				Size: pgStorage(p),
			},
			Bootstrap: &cnpgv1.BootstrapConfiguration{
				InitDB: &cnpgv1.BootstrapInitDB{
					Database: "tatara_memory",
					Owner:    "tatara_memory",
					PostInitApplicationSQL: []string{
						"CREATE EXTENSION IF NOT EXISTS vector",
					},
				},
			},
		},
	}
}
