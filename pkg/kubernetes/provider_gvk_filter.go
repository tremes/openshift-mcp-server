package kubernetes

import (
	"context"
	"errors"
	"fmt"

	"github.com/containers/kubernetes-mcp-server/pkg/api"
	"github.com/containers/kubernetes-mcp-server/pkg/klogutil"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
)

// ProviderGVKFilter provides GVK-based filtering capabilities for providers.
// It can be embedded in provider implementations to add AnyTargetHasGVKs functionality.
type ProviderGVKFilter struct {
	managerProvider ManagerProvider
}

// NewProviderGVKFilter creates a new ProviderGVKFilter that wraps a ManagerProvider.
func NewProviderGVKFilter(mp ManagerProvider) *ProviderGVKFilter {
	return &ProviderGVKFilter{
		managerProvider: mp,
	}
}

// AnyTargetHasGVKs reports whether every GVK in gvks is available on at least one target
// exposed by this provider. Returns true if an error occurs during discovery to avoid
// excluding tools due to transient issues.
func (f *ProviderGVKFilter) AnyTargetHasGVKs(ctx context.Context, gvks []schema.GroupVersionKind) bool {
	if len(gvks) == 0 {
		return true
	}

	logger := klogutil.FromContext(ctx)
	mgrs, err := f.managerProvider.GetTargetManagers(ctx)
	// If an error occurs, don't exclude tools
	if err != nil {
		klogutil.LogWarn(logger, "AnyTargetHasGVKs couldn't retrieve target managers; assuming all GVKs are available", klogutil.Err(err))
		return true
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Buffered so workers can finish after we return on the first true.
	results := make(chan bool, len(mgrs))
	for _, mgr := range mgrs {
		go func() {
			results <- targetHasGVKs(ctx, logger, mgr, gvks)
		}()
	}

	for range mgrs {
		if <-results {
			// One target with all GVKs (or a discovery error) is enough.
			// Workers that have not reached HasGVKs yet will stop via the deferred cancel() above.
			// client-go's DiscoveryClient uses context.TODO() (at the time of this writing), so
			// in-flight ServerResourcesForGroupVersion HTTP is not aborted by this cancel.
			return true
		}
	}
	return false
}

func targetHasGVKs(ctx context.Context, logger klog.Logger, mgr *Manager, gvks []schema.GroupVersionKind) bool {
	// Context errors can happen two ways:
	// - Another thread returned `true`, triggering cancelation. It doesn't matter what this thread
	//   returns, as the caller short-circuits `true` regardless.
	// - The parent context was canceled for some other reason before anyone reported `true` but
	//   also before everyone finished. We want to treat that like "all threads errored" and fail
	//   *open* (don't hide tools).
	if ctx.Err() != nil {
		return true
	}

	k, err := mgr.Derived(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return true
		}
		// Can't get discovery client; assume target has the GVKs to avoid
		// hiding tools due to transient errors
		klogutil.LogWarn(logger, "AnyTargetHasGVKs couldn't derive a Kubernetes interface for a manager; assuming all GVKs are available", klogutil.Err(err))
		return true
	}

	hasGVKs, err := api.HasGVKs(k.DiscoveryClient(), gvks)
	if err != nil {
		if ctx.Err() != nil {
			return true
		}
		// Discovery error; assume target has the GVKs to avoid hiding tools
		klogutil.LogWarn(logger, "AnyTargetHasGVKs couldn't query a client; assuming all GVKs are available", klogutil.Err(err))
		return true
	}
	return hasGVKs
}

// AnyTargetGetResourceInstance returns the first matching named instance on any target.
// An empty namespace is valid only for cluster-scoped resources.
func (f *ProviderGVKFilter) AnyTargetGetResourceInstance(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) (*unstructured.Unstructured, error) {
	if name == "" {
		return nil, errors.New("resource name is required")
	}

	mgrs, err := f.managerProvider.GetTargetManagers(ctx)
	if err != nil {
		return nil, err
	}

	var lookupErrors []error
	for _, mgr := range mgrs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		k, err := mgr.Derived(ctx)
		if err != nil {
			lookupErrors = append(lookupErrors, err)
			continue
		}

		mapping, err := k.RESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version)
		if meta.IsNoMatchError(err) {
			continue
		}
		if err != nil {
			lookupErrors = append(lookupErrors, err)
			continue
		}
		switch mapping.Scope.Name() {
		case meta.RESTScopeNameNamespace:
			if namespace == "" {
				return nil, fmt.Errorf("namespace is required for namespaced resource %s", gvk)
			}
		case meta.RESTScopeNameRoot:
			if namespace != "" {
				return nil, fmt.Errorf("namespace must be empty for cluster-scoped resource %s", gvk)
			}
		default:
			return nil, fmt.Errorf("unsupported scope for resource %s", gvk)
		}

		instance, err := k.DynamicClient().Resource(mapping.Resource).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			return instance, nil
		}
		if !apierrors.IsNotFound(err) {
			lookupErrors = append(lookupErrors, err)
		}
	}
	return nil, errors.Join(lookupErrors...)
}
