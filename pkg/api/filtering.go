package api

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// FilteringProvider provides tool filtering capabilities based on cluster capabilities
// (GVKs, features, etc.). Toolsets use this interface to determine which tools should
// be exposed.
type FilteringProvider interface {
	IsTargetCompatibilityToolFiltersEnabled() bool
	// AnyTargetHasGVKs reports whether every GVK in gvks is available on at least one target
	// exposed by this provider. Providers that have not opted in to GVK discovery
	// should return true so existing tools remain visible.
	AnyTargetHasGVKs(context.Context, []schema.GroupVersionKind) bool
}

// ResourceInstanceProvider optionally supports looking up named resources
// across targets.
type ResourceInstanceProvider interface {
	// AnyTargetGetResourceInstance returns the first matching named instance
	// from any target, or nil if absent from all targets. Namespace must be
	// provided for namespaced resources and empty for cluster-scoped resources.
	AnyTargetGetResourceInstance(context.Context, schema.GroupVersionKind, string, string) (*unstructured.Unstructured, error)
}
