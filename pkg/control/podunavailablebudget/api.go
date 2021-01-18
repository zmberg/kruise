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
	policyv1alpha1 "github.com/openkruise/kruise/apis/policy/v1alpha1"

	corev1 "k8s.io/api/core/v1"
)

type PubControl interface {
	// Common
	// IsPodConsistentAndReady indicates pod.spec and pod.status consistent,
	// Pod.Status=Running, Pod.condition[ready]=true, pod.containerStatus[x].ready=true
	IsPodConsistentAndReady(pod *corev1.Pod) bool

	// webhook
	// determine if this change to pod might cause unavailability
	IsPodUnavailableChanged(oldPod, newPod *corev1.Pod) bool
}

func NewPubControl(pub *policyv1alpha1.PodUnavailableBudget) PubControl {
	return &commonControl{PodUnavailableBudget: pub}
}
