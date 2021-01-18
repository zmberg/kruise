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

package validating

import (
	"context"
	"fmt"
	"time"

	policyv1alpha1 "github.com/openkruise/kruise/apis/policy/v1alpha1"
	"github.com/openkruise/kruise/pkg/control/podunavailablebudget"

	admissionv1beta1 "k8s.io/api/admission/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/util/dryrun"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/apis/policy"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const (
	// MaxUnavailablePodSize is the max size of PUB.DisruptedPods + PUB.UnavailablePods.
	MaxUnavailablePodSize = 2000
)

var ConflictRetry = wait.Backoff{
	Steps:    4,
	Duration: 500 * time.Millisecond,
	Factor:   1.0,
	Jitter:   0.1,
}

func (p *PodCreateHandler) podUnavailableBudgetValidatingPod(ctx context.Context, req admission.Request) (allowed bool, reason string, err error) {
	isIgnore, newPod, oldPod, dryRun, err := p.shouldIgnore(ctx, req)
	if err != nil {
		return false, "", err
	}
	if isIgnore {
		return true, "", nil
	}

	isUpdated := req.AdmissionRequest.Operation == admissionv1beta1.Update
	// returns true for pod conditions that allow the operation for pod without checking PUB.
	if newPod.Status.Phase == corev1.PodSucceeded || newPod.Status.Phase == corev1.PodFailed ||
		newPod.Status.Phase == corev1.PodPending || !newPod.ObjectMeta.DeletionTimestamp.IsZero() {
		klog.V(3).Infof("pod(%s.%s) Status(%s) Deletion(%v), then admit", newPod.Namespace, newPod.Name, newPod.Status.Phase, !newPod.ObjectMeta.DeletionTimestamp.IsZero())
		return true, "", nil
	}

	pub, err := p.getPubForPod(newPod)
	if err != nil {
		return false, "", err
	}
	// if there is no matching PodUnavailableBudget, just return true
	if pub == nil {
		return true, "", nil
	}
	control := podunavailablebudget.NewPubControl(pub)
	klog.V(3).Infof("validating pod(%s.%s) operation(%s) for pub(%s.%s)", newPod.Namespace, newPod.Name, req.Operation, pub.Namespace, pub.Name)

	// this change to pod will not cause pod unavailability, then pass
	if isUpdated && !control.IsPodUnavailableChanged(oldPod, newPod) {
		klog.V(3).Infof("validate pod(%s.%s) changed cannot cause unavailability, then don't need check pub", newPod.Namespace, newPod.Name)
		return true, "", nil
	}

	// when update operation, we should check old pod consistent and ready
	var checkPod *corev1.Pod
	if isUpdated {
		checkPod = oldPod
	} else {
		checkPod = newPod
	}
	// If the pod is not consistent and ready, it doesn't count towards healthy and we should not decrement
	if !control.IsPodConsistentAndReady(checkPod) && pub.Status.CurrentAvailable >= pub.Status.DesiredAvailable && pub.Status.DesiredAvailable > 0 {
		klog.V(3).Infof("pod(%s.%s) is not ready, then don't need check pub", checkPod.Namespace, checkPod.Name)
		return true, "", nil
	}

	// pod is in pub.Status.DisruptedPods or pub.Status.UnavailablePods, then don't need check it
	if isPodRecordedInPub(checkPod.Name, pub) {
		klog.V(3).Infof("pod(%s.%s) already is recorded in pub(%s.%s)", checkPod.Namespace, checkPod.Name, pub.Namespace, pub.Name)
		return true, "", nil
	}

	pubClone := pub.DeepCopy()
	refresh := false
	err = retry.RetryOnConflict(ConflictRetry, func() error {
		if refresh {
			key := types.NamespacedName{
				Name:      pubClone.Name,
				Namespace: pubClone.Namespace,
			}
			if err := p.Client.Get(context.TODO(), key, pubClone); err != nil {
				klog.Errorf("Get PodUnavailableBudget(%s) failed: %s", key.String(), err.Error())
				return err
			}
		}
		// Try to verify-and-decrement
		// If it was false already, or if it becomes false during the course of our retries,
		err := p.checkAndDecrement(checkPod.Name, pubClone, isUpdated)
		if err != nil {
			return err
		}

		// If this is a dry-run, we don't need to go any further than that.
		if dryRun == true {
			klog.V(3).Infof("pod(%s) operation for pub(%s.%s) is a dry run", checkPod.Name, pubClone.Namespace, pubClone.Name)
			return nil
		}
		klog.V(3).Infof("pub(%s.%s) update status(disruptedPods:%d, unavailablePods:%d, expectedCount:%d, desiredAvailable:%d, currentAvailable:%d, unavailableAllowed:%d)",
			pubClone.Namespace, pubClone.Name, len(pubClone.Status.DisruptedPods), len(pubClone.Status.UnavailablePods), pubClone.Status.TotalReplicas, pubClone.Status.DesiredAvailable, pubClone.Status.CurrentAvailable, pubClone.Status.UnavailableAllowed)
		err = p.Client.Status().Update(context.TODO(), pubClone)
		if err == nil {
			return nil
		}

		refresh = true
		klog.Errorf("update pub(%s.%s) status failed:%s", pubClone.Namespace, pubClone.Name, err.Error())
		return err
	})
	if err != nil && err != wait.ErrWaitTimeout {
		klog.Errorf("pod(%s.%s) operation(%s) for pub(%s.%s) failed: %s", checkPod.Namespace, checkPod.Name, req.Operation, pub.Namespace, pub.Name, err.Error())
		return false, err.Error(), nil
	} else if err == wait.ErrWaitTimeout {
		err = errors.NewTimeoutError(fmt.Sprintf("couldn't update PodUnavailableBudget %s due to conflicts", pub.Name), 10)
		klog.Errorf("pod(%s.%s) operation(%s) failed: %s", checkPod.Namespace, checkPod.Name, req.Operation, err.Error())
		return false, err.Error(), nil
	}

	klog.V(3).Infof("admit pod(%s.%s) operation(%s) for pub(%s.%s)", newPod.Namespace, newPod.Name, req.Operation, pub.Namespace, pub.Name)
	return true, "", nil
}

func (p *PodCreateHandler) shouldIgnore(ctx context.Context, req admission.Request) (
	// isIgnore indicates whether further logic need to be performed to check PUB
	isIgnore bool, newPod, oldPod *corev1.Pod, dryRun bool, err error) {
	isIgnore = true
	// Ignore all calls to resources other than pods.
	gvr := req.AdmissionRequest.Resource
	if (schema.GroupResource{Group: gvr.Group, Resource: gvr.Resource} != corev1.Resource("pods")) {
		return
	}
	defer func() {
		if newPod != nil && newPod.Namespace == "" {
			newPod.Namespace = req.Namespace
		}
		if oldPod != nil && oldPod.Namespace == "" {
			oldPod.Namespace = req.Namespace
		}
	}()

	newPod = &corev1.Pod{}
	switch req.AdmissionRequest.Operation {

	// filter out invalid Update operation, we only validate update Pod.MetaData, Pod.Spec
	case admissionv1beta1.Update:
		//decode new pod
		err = p.Decoder.Decode(req, newPod)
		if err != nil {
			return
		}
		oldPod = &corev1.Pod{}
		if err = p.Decoder.Decode(
			admission.Request{AdmissionRequest: admissionv1beta1.AdmissionRequest{Object: req.AdmissionRequest.OldObject}},
			oldPod); err != nil {
			return
		}

		options := &metav1.UpdateOptions{}
		//decode eviction
		err = p.Decoder.DecodeRaw(req.Options, options)
		if err != nil {
			return
		}
		// if dry run
		dryRun = dryrun.IsDryRun(options.DryRun)

	// filter out invalid Delete operation, only validate delete pods resources
	case admissionv1beta1.Delete:
		if req.AdmissionRequest.SubResource != "" {
			klog.V(6).Infof("pod(%s.%s) AdmissionRequest operation(DELETE) subResource(%s), then admit", req.Namespace, req.Name, req.SubResource)
			return
		}
		deletion := &metav1.DeleteOptions{}
		//decode eviction
		err = p.Decoder.DecodeRaw(req.Options, deletion)
		if err != nil {
			return
		}
		// if dry run
		dryRun = dryrun.IsDryRun(deletion.DryRun)
		key := types.NamespacedName{
			Namespace: req.AdmissionRequest.Namespace,
			Name:      req.AdmissionRequest.Name,
		}
		if err = p.Client.Get(ctx, key, newPod); err != nil {
			return
		}

	// filter out invalid Create operation, only validate create pod eviction subresource
	case admissionv1beta1.Create:
		// ignore create operation other than subresource eviction
		if req.AdmissionRequest.SubResource != "eviction" {
			klog.V(6).Infof("pod(%s.%s) AdmissionRequest operation(CREATE) Resource(%s) subResource(%s), then admit", req.Namespace, req.Name, req.Resource, req.SubResource)
			return
		}
		eviction := &policy.Eviction{}
		//decode eviction
		err = p.Decoder.Decode(req, eviction)
		if err != nil {
			return
		}
		// if dry run
		dryRun = dryrun.IsDryRun(eviction.DeleteOptions.DryRun)
		key := types.NamespacedName{
			Namespace: req.AdmissionRequest.Namespace,
			Name:      req.AdmissionRequest.Name,
		}
		if err = p.Client.Get(ctx, key, newPod); err != nil {
			return
		}

	default:
		// process pobUnavailableBudget to validate the request
	}
	isIgnore = false
	return
}

func (p *PodCreateHandler) getPubForPod(pod *corev1.Pod) (*policyv1alpha1.PodUnavailableBudget, error) {
	var err error
	if len(pod.Labels) == 0 {
		return nil, nil
	}

	pubList := &policyv1alpha1.PodUnavailableBudgetList{}
	if err = p.Client.List(context.TODO(), pubList, &client.ListOptions{Namespace: pod.Namespace}); err != nil {
		return nil, err
	}

	var matchedPubs []policyv1alpha1.PodUnavailableBudget
	for _, pub := range pubList.Items {
		// if targetReference isn't nil, priority to take effect
		var selector *metav1.LabelSelector
		if pub.Spec.TargetReference != nil {
			for _, finder := range p.controllerFinder.Finders() {
				ref := podunavailablebudget.ControllerReference{
					APIVersion: pub.Spec.TargetReference.APIVersion,
					Kind:       pub.Spec.TargetReference.Kind,
					Name:       pub.Spec.TargetReference.Name,
				}
				scaleNSelector, err := finder(ref, pub.Namespace)
				if err != nil {
					klog.Errorf("podUnavailableBudget(%s.%s) handle TargetReference failed: %s", pub.Namespace, pub.Name, err.Error())
					continue
				}
				if scaleNSelector != nil {
					selector = scaleNSelector.Selector
					break
				}
			}
			if selector == nil {
				continue
			}
		} else {
			selector = pub.Spec.Selector
		}

		labelSelector, err := metav1.LabelSelectorAsSelector(selector)
		if err != nil {
			continue
		}

		// If a PUB with a nil or empty selector creeps in, it should match nothing, not everything.
		if labelSelector.Empty() || !labelSelector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		matchedPubs = append(matchedPubs, pub)
	}

	if len(matchedPubs) == 0 {
		klog.V(6).Infof("could not find PodUnavailableBudget for pod %s in namespace %s with labels: %v", pod.Name, pod.Namespace, pod.Labels)
		return nil, nil
	}
	if len(matchedPubs) > 1 {
		klog.Warningf("Pod %q/%q matches multiple PodUnavailableBudgets.  Choose %q arbitrarily.", pod.Namespace, pod.Name, matchedPubs[0].Name)
		return nil, nil
	}

	return &matchedPubs[0], nil
}

func (p *PodCreateHandler) checkAndDecrement(podName string, pub *policyv1alpha1.PodUnavailableBudget, isUpdated bool) error {
	if pub.Status.UnavailableAllowed < 0 {
		return errors.NewForbidden(policyv1alpha1.Resource("podunavailablebudget"), pub.Name, fmt.Errorf("pub unavailable allowed is negative"))
	}
	if len(pub.Status.DisruptedPods)+len(pub.Status.UnavailablePods) > MaxUnavailablePodSize {
		return errors.NewForbidden(policyv1alpha1.Resource("podunavailablebudget"), pub.Name, fmt.Errorf("DisruptedPods and UnavailablePods map too big - too many unavailable not confirmed by PUB controller"))
	}
	if pub.Status.UnavailableAllowed == 0 {
		err := errors.NewTooManyRequests("Cannot operate pod as it would violate the pod's unavailable budget.", 0)
		err.ErrStatus.Details.Causes = append(err.ErrStatus.Details.Causes, metav1.StatusCause{Type: "UnavailableBudget",
			Message: fmt.Sprintf("The unavailable budget %s needs %d available pods and has %d currently", pub.Name, pub.Status.DesiredAvailable, pub.Status.CurrentAvailable)})
		return err
	}

	pub.Status.UnavailableAllowed--

	if pub.Status.DisruptedPods == nil {
		pub.Status.DisruptedPods = make(map[string]metav1.Time)
	}
	if pub.Status.UnavailablePods == nil {
		pub.Status.UnavailablePods = make(map[string]metav1.Time)
	}

	if isUpdated {
		pub.Status.UnavailablePods[podName] = metav1.Time{Time: time.Now()}
		klog.V(3).Infof("pod(%s) is recorded in pub(%s.%s) UnavailablePods", podName, pub.Namespace, pub.Name)
	} else {
		pub.Status.DisruptedPods[podName] = metav1.Time{Time: time.Now()}
		klog.V(3).Infof("pod(%s) is recorded in pub(%s.%s) DisruptedPods", podName, pub.Namespace, pub.Name)
	}
	return nil
}

func isPodRecordedInPub(podName string, pub *policyv1alpha1.PodUnavailableBudget) bool {
	if _, ok := pub.Status.UnavailablePods[podName]; ok {
		return true
	}
	if _, ok := pub.Status.DisruptedPods[podName]; ok {
		return true
	}
	return false
}
