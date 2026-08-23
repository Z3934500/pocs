// Package v1alpha1 contains API Schema definitions for the jvm.oms.io v1alpha1 API group.
// +groupName=jvm.oms.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is group version used to register these objects.
	GroupVersion = schema.GroupVersion{Group: "jvm.oms.io", Version: "v1alpha1"}

	// SchemeBuilder is used to add functions to the scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
