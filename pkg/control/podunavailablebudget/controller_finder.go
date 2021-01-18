/*
Copyright 2021 The Kruise Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package podunavailablebudget

import (
	"context"

	appsv1alpha1 "github.com/openkruise/kruise/apis/apps/v1alpha1"
	appsv1beta1 "github.com/openkruise/kruise/apis/apps/v1beta1"

	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ScaleAndSelector is used to return (controller, scale, selector) fields from the
// controller finder functions.
type ScaleAndSelector struct {
	// controller UID, for example: rc, rs, deployment...
	types.UID
	// controller.spec.Replicas
	Scale int32
	// controller.spec.Selector
	Selector *metav1.LabelSelector
}

type ControllerReference struct {
	// API version of the referent.
	APIVersion string `json:"apiVersion" protobuf:"bytes,5,opt,name=apiVersion"`
	// Kind of the referent.
	Kind string `json:"kind" protobuf:"bytes,1,opt,name=kind"`
	// Name of the referent.
	Name string `json:"name" protobuf:"bytes,3,opt,name=name"`
	// UID of the referent.
	UID types.UID `json:"uid" protobuf:"bytes,4,opt,name=uid,casttype=k8s.io/apimachinery/pkg/types.UID"`
}

// PodControllerFinder is a function type that maps a pod to a list of
// controllers and their scale.
type PodControllerFinder func(ref ControllerReference, namespace string) (*ScaleAndSelector, error)

type ControllerFinder struct {
	client.Client
}

func NewControllerFinder(c client.Client) *ControllerFinder {
	return &ControllerFinder{
		Client: c,
	}
}

// TODO kruise UnitedDeployment
func (r *ControllerFinder) Finders() []PodControllerFinder {
	return []PodControllerFinder{r.getPodReplicationController, r.getPodDeployment, r.getPodReplicaSet,
		r.getPodStatefulSet, r.getPodKruiseCloneSet, r.getPodKruiseStatefulSet}
}

var (
	// kubernetes
	controllerKindSS  = apps.SchemeGroupVersion.WithKind("StatefulSet")
	controllerKindRC  = corev1.SchemeGroupVersion.WithKind("ReplicationController")
	controllerKindDep = apps.SchemeGroupVersion.WithKind("Deployment")

	// kruise
	controllerKruiseKindCS = appsv1alpha1.SchemeGroupVersion.WithKind("CloneSet")
	controllerKruiseKindSS = appsv1beta1.SchemeGroupVersion.WithKind("StatefulSet")
)

// getPodReplicaSet finds a replicaset which has no matching deployments.
func (r *ControllerFinder) getPodReplicaSet(ref ControllerReference, namespace string) (*ScaleAndSelector, error) {
	ok, err := verifyGroupKind(ref, ref.Kind, []string{"apps", "extensions"})
	if !ok || err != nil {
		return nil, err
	}
	replicaSet := &apps.ReplicaSet{}
	err = r.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: ref.Name}, replicaSet)
	if err != nil {
		// when error is NotFound, it is ok here.
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if ref.UID != "" && replicaSet.UID != ref.UID {
		return nil, nil
	}
	controllerRef := metav1.GetControllerOf(replicaSet)
	if controllerRef != nil && controllerRef.Kind == controllerKindDep.Kind {
		refSs := ControllerReference{
			APIVersion: controllerRef.APIVersion,
			Kind:       controllerRef.Kind,
			Name:       controllerRef.Name,
			UID:        controllerRef.UID,
		}
		return r.getPodDeployment(refSs, namespace)
	}
	return &ScaleAndSelector{
		UID:      replicaSet.UID,
		Scale:    *(replicaSet.Spec.Replicas),
		Selector: replicaSet.Spec.Selector,
	}, nil
}

// getPodStatefulSet returns the statefulset referenced by the provided controllerRef.
func (r *ControllerFinder) getPodStatefulSet(ref ControllerReference, namespace string) (*ScaleAndSelector, error) {
	ok, err := verifyGroupKind(ref, controllerKindSS.Kind, []string{"apps"})
	if !ok || err != nil {
		return nil, err
	}
	statefulSet := &apps.StatefulSet{}
	err = r.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: ref.Name}, statefulSet)
	if err != nil {
		// when error is NotFound, it is ok here.
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if ref.UID != "" && statefulSet.UID != ref.UID {
		return nil, nil
	}

	return &ScaleAndSelector{
		UID:      statefulSet.UID,
		Scale:    *(statefulSet.Spec.Replicas),
		Selector: statefulSet.Spec.Selector,
	}, nil
}

// getPodDeployments finds deployments for any replicasets which are being managed by deployments.
func (r *ControllerFinder) getPodDeployment(ref ControllerReference, namespace string) (*ScaleAndSelector, error) {
	ok, err := verifyGroupKind(ref, controllerKindDep.Kind, []string{"apps", "extensions"})
	if !ok || err != nil {
		return nil, err
	}
	deployment := &apps.Deployment{}
	err = r.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: ref.Name}, deployment)
	if err != nil {
		// when error is NotFound, it is ok here.
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if ref.UID != "" && deployment.UID != ref.UID {
		return nil, nil
	}
	return &ScaleAndSelector{
		UID:      deployment.UID,
		Scale:    *(deployment.Spec.Replicas),
		Selector: deployment.Spec.Selector,
	}, nil
}

func (r *ControllerFinder) getPodReplicationController(ref ControllerReference, namespace string) (*ScaleAndSelector, error) {
	ok, err := verifyGroupKind(ref, controllerKindRC.Kind, []string{""})
	if !ok || err != nil {
		return nil, err
	}
	rc := &corev1.ReplicationController{}
	err = r.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: ref.Name}, rc)
	if err != nil {
		// when error is NotFound, it is ok here.
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if ref.UID != "" && rc.UID != ref.UID {
		return nil, nil
	}
	return &ScaleAndSelector{
		UID:      rc.UID,
		Scale:    *(rc.Spec.Replicas),
		Selector: &metav1.LabelSelector{MatchLabels: rc.Spec.Selector},
	}, nil
}

// getPodStatefulSet returns the kruise cloneSet referenced by the provided controllerRef.
func (r *ControllerFinder) getPodKruiseCloneSet(ref ControllerReference, namespace string) (*ScaleAndSelector, error) {
	ok, err := verifyGroupKind(ref, controllerKruiseKindCS.Kind, []string{"apps.kruise.io"})
	if !ok || err != nil {
		return nil, err
	}
	cloneSet := &appsv1alpha1.CloneSet{}
	err = r.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: ref.Name}, cloneSet)
	if err != nil {
		// when error is NotFound, it is ok here.
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if ref.UID != "" && cloneSet.UID != ref.UID {
		return nil, nil
	}

	return &ScaleAndSelector{
		UID:      cloneSet.UID,
		Scale:    *(cloneSet.Spec.Replicas),
		Selector: cloneSet.Spec.Selector,
	}, nil
}

// getPodStatefulSet returns the kruise statefulset referenced by the provided controllerRef.
func (r *ControllerFinder) getPodKruiseStatefulSet(ref ControllerReference, namespace string) (*ScaleAndSelector, error) {
	ok, err := verifyGroupKind(ref, controllerKruiseKindSS.Kind, []string{"apps.kruise.io"})
	if !ok || err != nil {
		return nil, err
	}
	ss := &appsv1beta1.StatefulSet{}
	err = r.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: ref.Name}, ss)
	if err != nil {
		// when error is NotFound, it is ok here.
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if ref.UID != "" && ss.UID != ref.UID {
		return nil, nil
	}

	return &ScaleAndSelector{
		UID:      ss.UID,
		Scale:    *(ss.Spec.Replicas),
		Selector: ss.Spec.Selector,
	}, nil
}

func verifyGroupKind(ref ControllerReference, expectedKind string, expectedGroups []string) (bool, error) {
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return false, err
	}

	if ref.Kind != expectedKind {
		return false, nil
	}

	for _, group := range expectedGroups {
		if group == gv.Group {
			return true, nil
		}
	}

	return false, nil
}
