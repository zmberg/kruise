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
	"encoding/json"
	"reflect"

	"github.com/openkruise/kruise/apis/apps/pub"
	policyv1alpha1 "github.com/openkruise/kruise/apis/policy/v1alpha1"
	"github.com/openkruise/kruise/pkg/control/sidecarcontrol"
	"github.com/openkruise/kruise/pkg/util"
	"github.com/openkruise/kruise/pkg/util/inplaceupdate"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog"
)

type commonControl struct {
	*policyv1alpha1.PodUnavailableBudget
}

func (c *commonControl) IsPodConsistentAndReady(pod *corev1.Pod) bool {
	// 1. pod.Status.Phase != v1.PodRunning
	// 2. pod.condition PodReady != true
	if !util.IsRunningAndReady(pod) {
		klog.V(6).Infof("pod(%s.%s) is not running or ready", pod.Namespace, pod.Name)
		return false
	}

	// check consistent
	// if all container image is digest format
	// by comparing status.containers[x].ImageID with spec.container[x].Image can determine whether pod is consistent
	allDigestImage := true
	for _, container := range pod.Spec.Containers {
		//whether image is digest format,
		//for example: docker.io/busybox@sha256:a9286defaba7b3a519d585ba0e37d0b2cbee74ebfe590960b0b1d6a5e97d1e1d
		if !util.IsImageDigest(container.Image) {
			allDigestImage = false
			continue
		}

		if !util.IsPodContainerDigestEqual(sets.NewString(container.Name), pod) {
			klog.V(6).Infof("pod(%s.%s) container(%s) image is inconsistent", pod.Namespace, pod.Name, container.Name)
			return false
		}
	}
	// If all spec.container[x].image is digest format, only check digest imageId
	if allDigestImage {
		return true
	}

	// check whether injected sidecar container is consistent
	sidecarContainers := getSidecarContainersInPod(pod)
	if sidecarContainers.Len() > 0 && !isSidecarContainerUpdateCompleted(pod, sidecarContainers) {
		return false
	}

	// whether other containers is consistent
	if err := inplaceupdate.CheckInPlaceUpdateCompleted(pod); err != nil {
		klog.V(6).Infof("check pod(%s.%s) InPlaceUpdate failed: %s", pod.Namespace, pod.Name, err.Error())
		return false
	}

	return true
}

func (c *commonControl) IsPodUnavailableChanged(oldPod, newPod *corev1.Pod) bool {
	if !reflect.DeepEqual(oldPod.Labels, newPod.Labels) {
		// cloneSet In-place update pod,
		// first will change pod labels[lifecycle.apps.kruise.io/state] from Normal to PreparingUpdate
		oldState := oldPod.Labels[pub.LifecycleStateKey]
		newState := newPod.Labels[pub.LifecycleStateKey]
		if pub.LifecycleStateType(oldState) == pub.LifecycleStateNormal && pub.LifecycleStateType(newState) == pub.LifecycleStatePreparingUpdate {
			klog.V(3).Infof("pod(%s.%s) labels[%s] from(%s) -> to(%s), and maybe cause unavailability", newPod.Namespace, newPod.Name, pub.LifecycleStateKey, oldState, newState)
			return true
		}
	}

	// If pod.spec changed, pod will be in unavailable condition
	if !reflect.DeepEqual(oldPod.Spec, newPod.Spec) {
		klog.V(3).Infof("pod(%s.%s) specification changed, and maybe cause unavailability", newPod.Namespace, newPod.Name)
		return true
	}
	// pod other changes will not cause unavailability situation, then return false
	return false
}

func getSidecarContainersInPod(pod *corev1.Pod) sets.String {
	sidecars := sets.NewString()
	for _, container := range pod.Spec.Containers {
		val := util.GetContainerEnvValue(&container, sidecarcontrol.SidecarEnvKey)
		if val == "true" {
			sidecars.Insert(container.Name)
		}
	}

	return sidecars
}

// todo Subsequent sidecarControl public function
// isContainerInplaceUpdateCompleted checks whether imageID in container status has been changed since in-place update.
// If the imageID in containerStatuses has not been changed, we assume that kubelet has not updated containers in Pod.
func isSidecarContainerUpdateCompleted(pod *corev1.Pod, containers sets.String) bool {
	//format: sidecarset.name -> appsv1alpha1.InPlaceUpdateState
	sidecarUpdateStates := make(map[string]*pub.InPlaceUpdateState)
	// when the pod annotation not found, indicates the pod only injected sidecar container, and never inplace update
	// then always think it update complete
	if stateStr, ok := pod.Annotations[sidecarcontrol.SidecarsetInplaceUpdateStateKey]; !ok {
		return true
		// this won't happen in practice, unless someone manually edit pod annotations
	} else if err := json.Unmarshal([]byte(stateStr), &sidecarUpdateStates); err != nil {
		klog.Warningf("parse pod(%s.%s) annotations[%s] value(%s) failed: %s",
			pod.Namespace, pod.Name, sidecarcontrol.SidecarsetInplaceUpdateStateKey, stateStr, err.Error())
		return false
	}

	// last container status, container.Name -> containerStatus[x].ImageID
	lastContainerStatus := make(map[string]string)
	for _, updateState := range sidecarUpdateStates {
		for cName, lastStatus := range updateState.LastContainerStatuses {
			lastContainerStatus[cName] = lastStatus.ImageID
		}
	}

	containerImages := make(map[string]string, len(pod.Spec.Containers))
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		containerImages[c.Name] = c.Image
	}

	for _, cs := range pod.Status.ContainerStatuses {
		// only check containers set
		if !containers.Has(cs.Name) {
			continue
		}
		if imageID, ok := lastContainerStatus[cs.Name]; ok {
			// we assume that users should not update workload template with new image
			// which actually has the same imageID as the old image
			if imageID == cs.ImageID && containerImages[cs.Name] != cs.Image {
				klog.V(6).Infof("pod(%s.%s) container %s imageID not changed", pod.Namespace, pod.Name, cs.Name)
				return false
			}
		}
		// If sidecar container status.ImageID changed, or this oldStatus ImageID don't exist
		// indicate the sidecar container update is complete
	}

	return true
}
