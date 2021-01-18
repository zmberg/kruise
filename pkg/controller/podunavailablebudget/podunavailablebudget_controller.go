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
	"flag"
	"fmt"
	"reflect"
	"time"

	policyv1alpha1 "github.com/openkruise/kruise/apis/policy/v1alpha1"
	"github.com/openkruise/kruise/pkg/control/podunavailablebudget"
	"github.com/openkruise/kruise/pkg/util/gate"
	"github.com/openkruise/kruise/pkg/util/ratelimiter"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

func init() {
	flag.IntVar(&concurrentReconciles, "podunavailablebudget-workers", concurrentReconciles, "Max concurrent workers for PodUnavailableBudget controller.")
}

var (
	concurrentReconciles = 3
)

const (
	DeletionTimeout       = 2 * 60 * time.Second
	UpdatedDelayCheckTime = 5 * time.Second
)

var ConflictRetry = wait.Backoff{
	Steps:    4,
	Duration: 500 * time.Millisecond,
	Factor:   1.0,
	Jitter:   0.1,
}

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new PodUnavailableBudget Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	if !gate.ResourceEnabled(&policyv1alpha1.PodUnavailableBudget{}) {
		return nil
	}
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcilePodUnavailableBudget{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		recorder:         mgr.GetEventRecorderFor("podunavailablebudget-controller"),
		controllerFinder: podunavailablebudget.NewControllerFinder(mgr.GetClient()),
	}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("podunavailablebudget-controller", mgr, controller.Options{
		Reconciler: r, MaxConcurrentReconciles: concurrentReconciles,
		RateLimiter: ratelimiter.DefaultControllerRateLimiter()})
	if err != nil {
		return err
	}

	// Watch for changes to PodUnavailableBudget
	err = c.Watch(&source.Kind{Type: &policyv1alpha1.PodUnavailableBudget{}}, &handler.EnqueueRequestForObject{}, predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			old := e.ObjectOld.(*policyv1alpha1.PodUnavailableBudget)
			new := e.ObjectNew.(*policyv1alpha1.PodUnavailableBudget)
			if !reflect.DeepEqual(old.Spec, new.Spec) {
				klog.V(3).Infof("Observed updated Spec for PodUnavailableBudget: %s/%s", new.Namespace, new.Name)
				return true
			}
			return false
		},
	})
	if err != nil {
		return err
	}

	// Watch for changes to Pod
	if err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &enqueueRequestForPod{client: mgr.GetClient(), controllerFinder: podunavailablebudget.NewControllerFinder(mgr.GetClient())}); err != nil {
		return err
	}

	return nil
}

var _ reconcile.Reconciler = &ReconcilePodUnavailableBudget{}

// ReconcilePodUnavailableBudget reconciles a PodUnavailableBudget object
type ReconcilePodUnavailableBudget struct {
	client.Client
	Scheme           *runtime.Scheme
	recorder         record.EventRecorder
	controllerFinder *podunavailablebudget.ControllerFinder
}

// +kubebuilder:rbac:groups=policy.kruise.io,resources=podunavailablebudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy.kruise.io,resources=podunavailablebudgets/status,verbs=get;update;patch

// todo pub watch predicate, only status changed, don't reconcile.
// pkg/controller/cloneset/cloneset_controller.go Watch for changes to CloneSet
func (r *ReconcilePodUnavailableBudget) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	// Fetch the PodUnavailableBudget instance
	pub := &policyv1alpha1.PodUnavailableBudget{}
	err := r.Get(context.TODO(), req.NamespacedName, pub)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.V(3).Infof("pub(%s.%s) is Deletion in this time", req.Namespace, req.Name)
			// Object not found, return.  Created objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	klog.V(3).Infof("begin to process podUnavailableBudget(%s.%s)", pub.Namespace, pub.Name)
	err = r.syncPodUnavailableBudget(pub)
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *ReconcilePodUnavailableBudget) syncPodUnavailableBudget(pub *policyv1alpha1.PodUnavailableBudget) error {
	pods, err := r.getPodsForPub(pub)
	if err != nil {
		r.recorder.Eventf(pub, corev1.EventTypeWarning, "NoPods", "Failed to get pods: %v", err)
		return err
	}
	if len(pods) == 0 {
		r.recorder.Eventf(pub, corev1.EventTypeNormal, "NoPods", "No matching pods found")
	}

	klog.V(3).Infof("pub(%s.%s) controller len(%d) pods", pub.Namespace, pub.Name, len(pods))
	expectedCount, desiredAvailable, err := r.getExpectedPodCount(pub, pods)
	if err != nil {
		r.recorder.Eventf(pub, corev1.EventTypeWarning, "CalculateExpectedPodCountFailed", "Failed to calculate the number of expected pods: %v", err)
		return err
	}

	control := podunavailablebudget.NewPubControl(pub)
	currentTime := time.Now()
	pubClone := pub.DeepCopy()
	refresh := false
	err = retry.RetryOnConflict(ConflictRetry, func() error {
		if refresh {
			key := types.NamespacedName{
				Name:      pub.Name,
				Namespace: pub.Namespace,
			}
			if err := r.Get(context.TODO(), key, pubClone); err != nil {
				klog.Errorf("getting updated pobUnavailableBudget(%s.%s) failed: %s", pubClone.Namespace, pubClone.Name, err.Error())
				return err
			}
		}
		disruptedPods, unavailablePods := r.buildDisruptedAndUnavailablePods(pods, pubClone, currentTime, control)
		currentAvailable := countAvailablePods(pods, disruptedPods, unavailablePods, control)

		updateErr := r.updatePubStatus(pubClone, currentAvailable, desiredAvailable, expectedCount, disruptedPods, unavailablePods)
		if updateErr == nil {
			return nil
		}

		refresh = true
		klog.Errorf("update pub(%s.%s) status failed:%s", pubClone.Namespace, pubClone.Name, updateErr.Error())
		return updateErr
	})
	if err != nil {
		klog.Errorf("update pub(%s.%s) status failed: %s", pub.Namespace, pub.Name, err.Error())
	}
	return err
}

func countAvailablePods(pods []*corev1.Pod, disruptedPods, unavailablePods map[string]metav1.Time, control podunavailablebudget.PubControl) (currentAvailable int32) {
	recordPods := sets.String{}
	for pName := range disruptedPods {
		recordPods.Insert(pName)
	}
	for pName := range unavailablePods {
		recordPods.Insert(pName)
	}

	for _, pod := range pods {
		// Pod is being deleted.
		if pod.DeletionTimestamp != nil {
			continue
		}
		// ignore disrupted or unavailable pods
		if recordPods.Has(pod.Name) {
			continue
		}
		// pod consistent and ready
		if control.IsPodConsistentAndReady(pod) {
			currentAvailable++
		}
	}

	return
}

// This function returns pods using the PodUnavailableBudget object.
func (r *ReconcilePodUnavailableBudget) getPodsForPub(pub *policyv1alpha1.PodUnavailableBudget) ([]*corev1.Pod, error) {
	// if targetReference isn't nil, priority to take effect
	var selector *metav1.LabelSelector
	if pub.Spec.TargetReference != nil {
		ref := podunavailablebudget.ControllerReference{
			APIVersion: pub.Spec.TargetReference.APIVersion,
			Kind:       pub.Spec.TargetReference.Kind,
			Name:       pub.Spec.TargetReference.Name,
		}
		for _, finder := range r.controllerFinder.Finders() {
			scaleNSelector, err := finder(ref, pub.Namespace)
			if err != nil {
				klog.Errorf("podUnavailableBudget(%s.%s) handle TargetReference failed: %s", pub.Namespace, pub.Name, err.Error())
				return nil, err
			}
			if scaleNSelector != nil {
				selector = scaleNSelector.Selector
				break
			}
		}

		// if target reference workload not found, or reference selector is nil
		if selector == nil {
			r.recorder.Eventf(pub, corev1.EventTypeWarning, "TargetReference", "Target Reference Selector Not Found")
			return nil, nil
		}
	} else if pub.Spec.Selector == nil {
		r.recorder.Eventf(pub, corev1.EventTypeWarning, "NoSelector", "Selector cannot be empty")
		return nil, nil
	} else {
		selector = pub.Spec.Selector
	}

	labelSelector, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		r.recorder.Eventf(pub, corev1.EventTypeWarning, "Selector", fmt.Sprintf("Label selector failed: %s", err.Error()))
		return nil, nil
	}
	podList := &corev1.PodList{}
	if err := r.List(context.TODO(), podList, &client.ListOptions{Namespace: pub.Namespace, LabelSelector: labelSelector}); err != nil {
		return nil, err
	}

	matchedPods := make([]*corev1.Pod, 0, len(podList.Items))
	for _, pod := range podList.Items {
		matchedPods = append(matchedPods, pod.DeepCopy())
	}
	return matchedPods, nil
}

func (r *ReconcilePodUnavailableBudget) getExpectedPodCount(pub *policyv1alpha1.PodUnavailableBudget, pods []*corev1.Pod) (expectedCount, desiredAvailable int32, err error) {
	if pub.Spec.MaxUnavailable != nil {
		expectedCount, err = r.getExpectedScale(pub, pods)
		if err != nil {
			return
		}
		var maxUnavailable int
		maxUnavailable, err = intstr.GetValueFromIntOrPercent(pub.Spec.MaxUnavailable, int(expectedCount), true)
		if err != nil {
			return
		}

		desiredAvailable = expectedCount - int32(maxUnavailable)
		if desiredAvailable < 0 {
			desiredAvailable = 0
		}
	} else if pub.Spec.MinAvailable != nil {
		if pub.Spec.MinAvailable.Type == intstr.Int {
			desiredAvailable = pub.Spec.MinAvailable.IntVal
			expectedCount = int32(len(pods))
		} else if pub.Spec.MinAvailable.Type == intstr.String {
			expectedCount, err = r.getExpectedScale(pub, pods)
			if err != nil {
				return
			}

			var minAvailable int
			minAvailable, err = intstr.GetValueFromIntOrPercent(pub.Spec.MinAvailable, int(expectedCount), true)
			if err != nil {
				return
			}
			desiredAvailable = int32(minAvailable)
		}
	}
	return
}

func (r *ReconcilePodUnavailableBudget) getExpectedScale(pub *policyv1alpha1.PodUnavailableBudget, pods []*corev1.Pod) (expectedCount int32, err error) {
	// A mapping from controllers to their scale.
	controllerScale := map[types.UID]int32{}

	// 1. Find the controller for each pod.  If any pod has 0 controllers,
	// that's an error. With ControllerRef, a pod can only have 1 controller.
	for _, pod := range pods {
		controllerRef := metav1.GetControllerOf(pod)
		if controllerRef == nil {
			klog.Warningf("found no controller ref for pod(%s.%s)", pod.Namespace, pod.Name)
			continue
		}

		// If we already know the scale of the controller there is no need to do anything.
		if _, found := controllerScale[controllerRef.UID]; found {
			continue
		}

		// Check all the supported controllers to find the desired scale.
		foundController := false
		for _, finder := range r.controllerFinder.Finders() {
			ref := podunavailablebudget.ControllerReference{
				APIVersion: controllerRef.APIVersion,
				Kind:       controllerRef.Kind,
				Name:       controllerRef.Name,
				UID:        controllerRef.UID,
			}
			var scaleNSelector *podunavailablebudget.ScaleAndSelector
			scaleNSelector, err = finder(ref, pub.Namespace)
			if err != nil {
				return
			}
			if scaleNSelector != nil {
				controllerScale[scaleNSelector.UID] = scaleNSelector.Scale
				foundController = true
				break
			}
		}
		if !foundController {
			klog.Warningf("found no controllers for pod(%s.%s)", pod.Namespace, pod.Name)
			continue
		}
	}

	// 2. Add up all the controllers.
	expectedCount = 0
	for _, count := range controllerScale {
		expectedCount += count
	}

	return
}

func (r *ReconcilePodUnavailableBudget) buildDisruptedAndUnavailablePods(pods []*corev1.Pod,
	pub *policyv1alpha1.PodUnavailableBudget, currentTime time.Time, control podunavailablebudget.PubControl) (

	// disruptedPods, unavailablePods
	map[string]metav1.Time, map[string]metav1.Time) {

	disruptedPods := pub.Status.DisruptedPods
	unavailablePods := pub.Status.UnavailablePods

	resultDisruptedPods := make(map[string]metav1.Time)
	resultUnavailablePods := make(map[string]metav1.Time)

	if disruptedPods == nil && unavailablePods == nil {
		return resultDisruptedPods, resultUnavailablePods
	}
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			// Already being deleted.
			continue
		}

		//handle disruption pods which will be eviction or deletion
		disruptionTime, found := disruptedPods[pod.Name]
		if found {
			expectedDeletion := disruptionTime.Time.Add(DeletionTimeout)
			if expectedDeletion.Before(currentTime) {
				r.recorder.Eventf(pod, corev1.EventTypeWarning, "NotDeleted", "Pod was expected by PUB %s/%s to be deleted but it wasn't",
					pub.Namespace, pub.Namespace)
			} else {
				resultDisruptedPods[pod.Name] = disruptionTime
			}
		}

		// handle unavailable pods which have been in-updated specification
		unavailableTime, found := unavailablePods[pod.Name]
		if found {
			// in case of informer cache latency, after 5 seconds, check whether pod is consistent and ready
			exceptCheckTime := unavailableTime.Time.Add(UpdatedDelayCheckTime)
			if exceptCheckTime.Before(currentTime) && control.IsPodConsistentAndReady(pod) {
				// after except check time, pod is consistent and ready, so we assume it is available now
				continue
			}
			resultUnavailablePods[pod.Name] = unavailableTime
		}
	}
	return resultDisruptedPods, resultUnavailablePods
}

func (r *ReconcilePodUnavailableBudget) updatePubStatus(pub *policyv1alpha1.PodUnavailableBudget, currentAvailable, desiredAvailable, expectedCount int32,
	disruptedPods, unavailablePods map[string]metav1.Time) error {

	unavailableAllowed := currentAvailable - desiredAvailable
	if expectedCount <= 0 || unavailableAllowed <= 0 {
		unavailableAllowed = 0
	}

	if pub.Status.CurrentAvailable == currentAvailable &&
		pub.Status.DesiredAvailable == desiredAvailable &&
		pub.Status.TotalReplicas == expectedCount &&
		pub.Status.UnavailableAllowed == unavailableAllowed &&
		pub.Status.ObservedGeneration == pub.Generation &&
		apiequality.Semantic.DeepEqual(pub.Status.DisruptedPods, disruptedPods) &&
		apiequality.Semantic.DeepEqual(pub.Status.UnavailablePods, unavailablePods) {
		return nil
	}

	pub.Status = policyv1alpha1.PodUnavailableBudgetStatus{
		CurrentAvailable:   currentAvailable,
		DesiredAvailable:   desiredAvailable,
		TotalReplicas:      expectedCount,
		UnavailableAllowed: unavailableAllowed,
		DisruptedPods:      disruptedPods,
		UnavailablePods:    unavailablePods,
		ObservedGeneration: pub.Generation,
	}
	klog.V(3).Infof("pub(%s.%s) update status(disruptedPods:%d, unavailablePods:%d, expectedCount:%d, desiredAvailable:%d, currentAvailable:%d, unavailableAllowed:%d)",
		pub.Namespace, pub.Name, len(disruptedPods), len(unavailablePods), expectedCount, desiredAvailable, currentAvailable, unavailableAllowed)
	return r.Client.Status().Update(context.TODO(), pub)
}
