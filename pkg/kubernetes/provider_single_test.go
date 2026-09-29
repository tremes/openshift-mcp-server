package kubernetes

import (
	"errors"
	"net/http"
	"testing"

	"github.com/containers/kubernetes-mcp-server/internal/test"
	"github.com/containers/kubernetes-mcp-server/pkg/api"
	"github.com/containers/kubernetes-mcp-server/pkg/config"
	"github.com/stretchr/testify/suite"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

type ProviderSingleTestSuite struct {
	BaseProviderSuite
	originalIsInClusterConfig func() (*rest.Config, error)
	mockServer                *test.MockServer
	provider                  Provider
}

func (s *ProviderSingleTestSuite) SetupTest() {
	// Single cluster provider is used when in-cluster or when the multi-cluster feature is disabled.
	// For this test suite we simulate an in-cluster deployment backed by a mock API server.
	s.originalIsInClusterConfig = InClusterConfig
	s.mockServer = test.NewMockServer()
	// Default discovery simulates a vanilla (non-OpenShift) cluster.
	s.mockServer.Handle(test.NewDiscoveryClientHandler())
	InClusterConfig = func() (*rest.Config, error) {
		return s.mockServer.Config(), nil
	}
	provider, err := NewProvider(s.T().Context(), config.New())
	s.Require().NoError(err, "Expected no error creating provider")
	s.provider = provider
}

func (s *ProviderSingleTestSuite) TearDownTest() {
	InClusterConfig = s.originalIsInClusterConfig
	if s.mockServer != nil {
		s.mockServer.Close()
	}
}

func (s *ProviderSingleTestSuite) TestType() {
	s.IsType(&singleClusterProvider{}, s.provider)
}

func (s *ProviderSingleTestSuite) TestWithOpenShiftCluster() {
	// Serve the OpenShift discovery document so the Project GVK is present.
	s.mockServer.ResetHandlers()
	s.mockServer.Handle(test.NewInOpenShiftHandler())
	s.Run("has OpenShift Project GVK", func() {
		hasProjects := s.provider.AnyTargetHasGVKs(s.T().Context(), []schema.GroupVersionKind{
			{Group: "project.openshift.io", Version: "v1", Kind: "Project"},
		})
		s.True(hasProjects, "Expected provider to report OpenShift Project GVK available")
	})
}

func (s *ProviderSingleTestSuite) TestWithNonOpenShiftGVK() {
	s.Run("does not have non-existent GVK", func() {
		// Default (non-OpenShift) discovery returns a 404 for the missing GroupVersion.
		hasGVK := s.provider.AnyTargetHasGVKs(s.T().Context(), []schema.GroupVersionKind{
			{Group: "nonexistent.example.com", Version: "v1", Kind: "Foo"},
		})
		s.False(hasGVK, "Expected provider to report no nonexistent GVK")
	})
}

func (s *ProviderSingleTestSuite) TestGetNamedCustomResourceInstance() {
	s.mockServer.ResetHandlers()
	s.mockServer.Handle(test.NewDiscoveryClientHandler(metav1.APIResourceList{
		GroupVersion: "example.com/v1",
		APIResources: []metav1.APIResource{
			{Name: "widgets", Kind: "Widget", Namespaced: true, Verbs: metav1.Verbs{"get"}},
			{Name: "clusterwidgets", Kind: "ClusterWidget", Namespaced: false, Verbs: metav1.Verbs{"get"}},
		},
	}))
	s.mockServer.Handle(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/apis/example.com/v1/namespaces/demo/widgets/active":
			test.WriteObject(w, &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "example.com/v1",
				"kind":       "Widget",
				"metadata":   map[string]any{"name": "active", "namespace": "demo"},
				"spec":       map[string]any{"enabled": true},
			}})
		case "/apis/example.com/v1/clusterwidgets/global":
			test.WriteObject(w, &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "example.com/v1",
				"kind":       "ClusterWidget",
				"metadata":   map[string]any{"name": "global"},
			}})
		case "/apis/example.com/v1/namespaces/demo/widgets/missing":
			w.WriteHeader(http.StatusNotFound)
			test.WriteObject(w, &metav1.Status{
				TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
				Status: metav1.StatusFailure, Reason: metav1.StatusReasonNotFound, Code: http.StatusNotFound,
			})
		case "/apis/example.com/v1/namespaces/demo/widgets/forbidden":
			w.WriteHeader(http.StatusForbidden)
			test.WriteObject(w, &metav1.Status{
				TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
				Status: metav1.StatusFailure, Reason: metav1.StatusReasonForbidden, Code: http.StatusForbidden,
			})
		}
	}))

	instanceProvider, ok := s.provider.(api.ResourceInstanceProvider)
	s.Require().True(ok)
	widget := schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"}
	clusterWidget := schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "ClusterWidget"}

	s.Run("returns attributes of a namespaced instance", func() {
		instance, err := instanceProvider.AnyTargetGetResourceInstance(s.T().Context(), widget, "demo", "active")
		s.Require().NoError(err)
		s.Require().NotNil(instance)
		enabled, found, err := unstructured.NestedBool(instance.Object, "spec", "enabled")
		s.Require().NoError(err)
		s.True(found && enabled)
	})
	s.Run("returns nil for a missing instance", func() {
		instance, err := instanceProvider.AnyTargetGetResourceInstance(s.T().Context(), widget, "demo", "missing")
		s.Require().NoError(err)
		s.Nil(instance)
	})
	s.Run("requires a namespace for namespaced resources", func() {
		_, err := instanceProvider.AnyTargetGetResourceInstance(s.T().Context(), widget, "", "active")
		s.ErrorContains(err, "namespace is required")
	})
	s.Run("returns a cluster-scoped instance without a namespace", func() {
		instance, err := instanceProvider.AnyTargetGetResourceInstance(s.T().Context(), clusterWidget, "", "global")
		s.Require().NoError(err)
		s.Require().NotNil(instance)
		s.Equal("global", instance.GetName())
	})
	s.Run("rejects a namespace for cluster-scoped resources", func() {
		_, err := instanceProvider.AnyTargetGetResourceInstance(s.T().Context(), clusterWidget, "demo", "global")
		s.ErrorContains(err, "namespace must be empty")
	})
	s.Run("requires a resource name", func() {
		_, err := instanceProvider.AnyTargetGetResourceInstance(s.T().Context(), widget, "demo", "")
		s.ErrorContains(err, "resource name is required")
	})
	s.Run("returns lookup errors rather than reporting absence", func() {
		_, err := instanceProvider.AnyTargetGetResourceInstance(s.T().Context(), widget, "demo", "forbidden")
		s.Require().Error(err)
		s.True(apierrors.IsForbidden(err))
	})
}

func (s *ProviderSingleTestSuite) TestGetTargets() {
	s.Run("GetTargets returns single empty target", func() {
		targets, err := s.provider.GetTargets(s.T().Context())
		s.Require().NoError(err, "Expected no error from GetTargets")
		s.Len(targets, 1, "Expected 1 targets from GetTargets")
		s.Contains(targets, "", "Expected empty target from GetTargets")
	})
}

func (s *ProviderSingleTestSuite) TestGetDerivedKubernetes() {
	s.Run("GetDerivedKubernetes returns Kubernetes for empty target", func() {
		k8s, err := s.provider.GetDerivedKubernetes(s.T().Context(), "")
		s.Require().NoError(err, "Expected no error from GetDerivedKubernetes with empty target")
		s.NotNil(k8s, "Expected Kubernetes from GetDerivedKubernetes with empty target")
	})
	s.Run("GetDerivedKubernetes returns error for non-empty target", func() {
		k8s, err := s.provider.GetDerivedKubernetes(s.T().Context(), "non-empty-target")
		s.Require().Error(err, "Expected error from GetDerivedKubernetes with non-empty target")
		s.ErrorContains(err, "unable to get manager for other context/cluster with in-cluster strategy", "Expected error about trying to get other cluster")
		s.Nil(k8s, "Expected no Kubernetes from GetDerivedKubernetes with non-empty target")
	})
}

func (s *ProviderSingleTestSuite) TestGetDefaultTarget() {
	s.Run("GetDefaultTarget returns empty string", func() {
		s.Empty(s.provider.GetDefaultTarget(), "Expected empty string as default target")
	})
}

func (s *ProviderSingleTestSuite) TestGetTargetParameterName() {
	s.Empty(s.provider.GetTargetParameterName(), "Expected empty string as target parameter name")
}

func (s *ProviderSingleTestSuite) TestReloadConfigDoesNotRebuild() {
	k8s, err := s.provider.GetDerivedKubernetes(s.T().Context(), "")
	s.Require().NoError(err)
	s.Require().NotNil(k8s)

	InClusterConfig = func() (*rest.Config, error) {
		return nil, errors.New("in-cluster config unavailable")
	}
	s.Run("reload publishes config without rebuilding managers", func() {
		err := s.provider.ReloadConfig(s.T().Context(), config.New())
		s.NoError(err)
	})
	s.Run("previous manager still serves after reload", func() {
		k8s, err := s.provider.GetDerivedKubernetes(s.T().Context(), "")
		s.NoError(err)
		s.NotNil(k8s)
	})
}

func TestProviderSingle(t *testing.T) {
	suite.Run(t, new(ProviderSingleTestSuite))
}
