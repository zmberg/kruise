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
	"net/http"
	"reflect"

	policyv1alpha1 "github.com/openkruise/kruise/apis/policy/v1alpha1"
	"github.com/openkruise/kruise/pkg/util"

	metavalidation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	appsvalidation "k8s.io/kubernetes/pkg/apis/apps/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/runtime/inject"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// PodUnavailableBudgetCreateUpdateHandler handles PodUnavailableBudget
type PodUnavailableBudgetCreateUpdateHandler struct {
	// To use the client, you need to do the following:
	// - uncomment it
	// - import sigs.k8s.io/controller-runtime/pkg/client
	// - uncomment the InjectClient method at the bottom of this file.
	Client client.Client

	// Decoder decodes objects
	Decoder *admission.Decoder
}

var _ admission.Handler = &PodUnavailableBudgetCreateUpdateHandler{}

// Handle handles admission requests.
func (h *PodUnavailableBudgetCreateUpdateHandler) Handle(ctx context.Context, req admission.Request) admission.Response {
	obj := &policyv1alpha1.PodUnavailableBudget{}

	err := h.Decoder.Decode(req, obj)
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	allErrs := h.validatingSidecarSetFn(obj)
	if len(allErrs) != 0 {
		return admission.Errored(http.StatusInternalServerError, allErrs.ToAggregate())
	}
	return admission.ValidationResponse(true, "")
}

func (h *PodUnavailableBudgetCreateUpdateHandler) validatingSidecarSetFn(obj *policyv1alpha1.PodUnavailableBudget) field.ErrorList {
	//validate Pub.Spec
	allErrs := validatePodUnavailableBudgetSpec(obj, field.NewPath("spec"))

	//validate whether pub is in conflict with others
	pubList := &policyv1alpha1.PodUnavailableBudgetList{}
	if err := h.Client.List(context.TODO(), pubList, &client.ListOptions{Namespace: obj.Namespace}); err != nil {
		allErrs = append(allErrs, field.InternalError(field.NewPath(""), fmt.Errorf("query other podUnavailableBudget failed, err: %v", err)))
	} else {
		allErrs = append(allErrs, validatePubConflict(obj, pubList.Items, field.NewPath("spec"))...)
	}
	return allErrs
}

func validatePodUnavailableBudgetSpec(obj *policyv1alpha1.PodUnavailableBudget, fldPath *field.Path) field.ErrorList {
	spec := &obj.Spec
	allErrs := field.ErrorList{}

	if spec.Selector == nil && spec.TargetReference == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("selector, targetRef"), "no selector or targetRef defined in PodUnavailableBudget"))
	} else if spec.TargetReference != nil {
		if spec.TargetReference.APIVersion == "" || spec.TargetReference.Name == "" || spec.TargetReference.Kind == "" {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("TargetReference"), spec.TargetReference, "empty TargetReference is not valid for PodUnavailableBudget."))
		}
	} else {
		allErrs = append(allErrs, metavalidation.ValidateLabelSelector(spec.Selector, fldPath.Child("selector"))...)
		if len(spec.Selector.MatchLabels)+len(spec.Selector.MatchExpressions) == 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("selector"), spec.Selector, "empty selector is not valid for PodUnavailableBudget."))
		}
	}

	if spec.MaxUnavailable == nil && spec.MinAvailable == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("maxUnavailable, minAvailable"), "no maxUnavailable or minAvailable defined in PodUnavailableBudget"))
	} else if spec.MaxUnavailable != nil {
		allErrs = append(allErrs, appsvalidation.ValidatePositiveIntOrPercent(*spec.MaxUnavailable, fldPath.Child("maxUnavailable"))...)
		allErrs = append(allErrs, appsvalidation.IsNotMoreThan100Percent(*spec.MaxUnavailable, fldPath.Child("maxUnavailable"))...)
	} else {
		allErrs = append(allErrs, appsvalidation.ValidatePositiveIntOrPercent(*spec.MinAvailable, fldPath.Child("minAvailable"))...)
		allErrs = append(allErrs, appsvalidation.IsNotMoreThan100Percent(*spec.MinAvailable, fldPath.Child("minAvailable"))...)
	}
	return allErrs
}

func validatePubConflict(pub *policyv1alpha1.PodUnavailableBudget, others []policyv1alpha1.PodUnavailableBudget, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	for _, other := range others {
		if pub.Name == other.Name {
			continue
		}
		// pod cannot be controlled by multiple pubs
		if pub.Spec.TargetReference != nil && other.Spec.TargetReference != nil {
			if reflect.DeepEqual(pub.Spec.TargetReference, other.Spec.TargetReference) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("targetReference"), pub.Spec.TargetReference, fmt.Sprintf(
					"pub.spec.targetReference is in conflict with other PodUnavailableBudget %s", other.Name)))
				return allErrs
			}
		} else if pub.Spec.Selector != nil && other.Spec.Selector != nil {
			if util.IsSelectorOverlapping(pub.Spec.Selector, other.Spec.Selector) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("selector"), pub.Spec.Selector, fmt.Sprintf(
					"pub.spec.selector is in conflict with other PodUnavailableBudget %s", other.Name)))
				return allErrs
			}
		}
	}
	return allErrs
}

var _ inject.Client = &PodUnavailableBudgetCreateUpdateHandler{}

// InjectClient injects the client into the PodUnavailableBudgetCreateUpdateHandler
func (h *PodUnavailableBudgetCreateUpdateHandler) InjectClient(c client.Client) error {
	h.Client = c
	return nil
}

var _ admission.DecoderInjector = &PodUnavailableBudgetCreateUpdateHandler{}

// InjectDecoder injects the decoder into the PodUnavailableBudgetCreateUpdateHandler
func (h *PodUnavailableBudgetCreateUpdateHandler) InjectDecoder(d *admission.Decoder) error {
	h.Decoder = d
	return nil
}
