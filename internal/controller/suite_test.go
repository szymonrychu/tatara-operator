package controller

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

var (
	testEnv   *envtest.Environment
	cfg       *rest.Config
	k8sClient client.Client
)

const (
	timeout  = 10 * time.Second
	interval = 250 * time.Millisecond
	testNS   = "tatara"
)

// TestMain boots a single envtest control plane for the whole controller
// package, registers the tatara.dev scheme and core types, creates the test
// namespace, and tears the control plane down at the end.
func TestMain(m *testing.M) {
	code := func() int {
		testEnv = &envtest.Environment{
			CRDDirectoryPaths: []string{
				filepath.Join("..", "..", "charts", "tatara-operator", "crds"),
				filepath.Join("..", "..", "charts", "tatara-operator", "crd-bases"),
			},
			ErrorIfCRDPathMissing: true,
		}
		var err error
		cfg, err = testEnv.Start()
		if err != nil {
			panic("start envtest: " + err.Error())
		}
		defer func() { _ = testEnv.Stop() }()

		if err := tataradevv1alpha1.AddToScheme(scheme.Scheme); err != nil {
			panic("add scheme: " + err.Error())
		}
		if err := cnpgv1.AddToScheme(scheme.Scheme); err != nil {
			panic("add cnpg scheme: " + err.Error())
		}

		k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
		if err != nil {
			panic("new client: " + err.Error())
		}

		ns := &corev1.Namespace{}
		ns.Name = testNS
		if err := k8sClient.Create(context.Background(), ns); err != nil {
			panic("create namespace: " + err.Error())
		}

		return m.Run()
	}()
	osExit(code)
}
