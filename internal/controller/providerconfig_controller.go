/*
Copyright 2026.

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

package controller

import (
	"context"
	"errors"
	"reflect"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	obsv1alpha1 "go.wilaris.de/obs-operator/api/v1alpha1"
	"go.wilaris.de/obs-operator/internal/provider"
)

// ProviderConfigReconciler reconciles a ProviderConfig object
type ProviderConfigReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	ProviderResolver *provider.ProviderResolver
}

// +kubebuilder:rbac:groups=obs.wilaris.de,resources=providerconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=obs.wilaris.de,resources=providerconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=obs.wilaris.de,resources=providerconfigs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get

func (r *ProviderConfigReconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	providerConfig := &obsv1alpha1.ProviderConfig{}
	if err := r.Get(ctx, req.NamespacedName, providerConfig); err != nil {
		if apierrors.IsNotFound(err) {
			if r.ProviderResolver != nil {
				r.ProviderResolver.InvalidateProvider(req.Namespace, req.Name)
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	resolver := r.ProviderResolver
	if resolver == nil {
		resolver = provider.NewProviderResolver(r.Client, provider.NewCache())
	}

	resolved, err := resolver.ResolveProviderConfigObject(ctx, providerConfig)
	if err != nil {
		log.Info(
			"ProviderConfig is not ready",
			"name",
			providerConfig.Name,
			"namespace",
			providerConfig.Namespace,
			"error",
			err.Error(),
		)
	} else {
		log.Info(
			"Resolved ProviderConfig",
			"name",
			providerConfig.Name,
			"namespace",
			providerConfig.Namespace,
			"region",
			resolved.Region,
			"endpoint",
			resolved.Endpoint,
			"fromCache",
			resolved.FromCache,
		)
	}

	condition := providerConfigReadyCondition(providerConfig, err)
	if err := r.updateProviderConfigStatus(ctx, providerConfig, condition); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ProviderConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&obsv1alpha1.ProviderConfig{}).
		Named("providerconfig").
		Complete(r)
}

func providerConfigReadyCondition(
	providerConfig *obsv1alpha1.ProviderConfig,
	err error,
) metav1.Condition {
	condition := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "ClientConfigured",
		Message:            "OBS client configuration is valid",
		ObservedGeneration: providerConfig.Generation,
	}

	if err == nil {
		return condition
	}

	condition.Status = metav1.ConditionFalse
	condition.Message = err.Error()
	switch {
	case apierrors.IsNotFound(err):
		condition.Reason = "CredentialsSecretNotFound"
	case errors.Is(err, provider.ErrMissingCredentials):
		condition.Reason = "CredentialsSecretInvalid"
	case errors.Is(err, provider.ErrInvalidEndpoint):
		condition.Reason = "InvalidEndpoint"
	default:
		condition.Reason = "ClientBuildFailed"
	}
	return condition
}

func (r *ProviderConfigReconciler) updateProviderConfigStatus(
	ctx context.Context,
	providerConfig *obsv1alpha1.ProviderConfig,
	condition metav1.Condition,
) error {
	original := providerConfig.DeepCopy()
	nextStatus := providerConfig.Status
	nextStatus.ObservedGeneration = providerConfig.Generation
	meta.SetStatusCondition(&nextStatus.Conditions, condition)

	statusChanged := original.Status.ObservedGeneration != nextStatus.ObservedGeneration ||
		original.Status.LastValidationTime == nil ||
		!reflect.DeepEqual(original.Status.Conditions, nextStatus.Conditions)
	if !statusChanged {
		return nil
	}

	now := metav1.Now()
	nextStatus.LastValidationTime = &now
	providerConfig.Status = nextStatus
	return r.Status().Patch(ctx, providerConfig, client.MergeFrom(original))
}
