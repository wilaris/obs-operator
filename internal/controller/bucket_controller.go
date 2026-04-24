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
	"fmt"
	"maps"
	"reflect"
	"sort"
	"strings"

	"github.com/opentelekomcloud/gophertelekomcloud/openstack/obs"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	obsv1alpha1 "go.wilaris.de/obs-operator/api/v1alpha1"
	"go.wilaris.de/obs-operator/internal/provider"
)

const (
	bucketFinalizer                          = "buckets.obs.wilaris.de/finalizer"
	bucketProviderConfigRefIndex             = ".spec.providerConfigRef.name"
	bucketReadyCondition                     = "Ready"
	bucketEncryptionAlgorithm                = "kms"
	conditionReasonCredentialsSecretNotFound = "CredentialsSecretNotFound"
	obsBucketAlreadyExistsCode               = "BucketAlreadyExists"
	obsBucketAlreadyOwnedByYou               = "BucketAlreadyOwnedByYou"
	obsNoSuchTagSetCode                      = "NoSuchTagSet"
	obsNoSuchEncryptionConfigCode            = "NoSuchEncryptionConfiguration"
	obsFsNotSupportCode                      = "FsNotSupport"
)

var errBucketAlreadyExists = errors.New("bucket already exists")

// BucketReconciler reconciles a Bucket object
type BucketReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	ProviderResolver *provider.ProviderResolver
}

type bucketObservation struct {
	exists   bool
	metadata *obs.GetBucketMetadataOutput
}

// +kubebuilder:rbac:groups=obs.wilaris.de,resources=buckets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=obs.wilaris.de,resources=buckets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=obs.wilaris.de,resources=buckets/finalizers,verbs=update
// +kubebuilder:rbac:groups=obs.wilaris.de,resources=providerconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get

func (r *BucketReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	bucket := &obsv1alpha1.Bucket{}
	if err := r.Get(ctx, req.NamespacedName, bucket); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	hasFinalizer := controllerutil.ContainsFinalizer(bucket, bucketFinalizer)
	if !bucket.DeletionTimestamp.IsZero() && !hasFinalizer {
		// Without our finalizer, this Bucket is not tracking a remote bucket.
		return ctrl.Result{}, nil
	}

	resolved, obsClient, err := r.resolveBucketClient(ctx, bucket)
	if err != nil {
		condition := bucketReadyStatusCondition(bucket, err)
		log.Info(
			"Bucket provider not ready",
			"reason",
			condition.Reason,
			"error",
			err.Error(),
		)
		return r.patchStatus(ctx, bucket, nil, err)
	}

	observation, err := r.observe(ctx, obsClient, bucket)
	if err != nil {
		return r.patchStatus(ctx, bucket, resolved, err)
	}

	if !bucket.DeletionTimestamp.IsZero() {
		if observation.exists {
			// Only delete the remote bucket after observing that it still exists.
			if err := r.delete(ctx, obsClient, bucket); err != nil {
				if isBucketNotEmpty(err) && !bucket.Spec.ForceDestroy {
					condition := bucketReadyStatusCondition(bucket, err)
					log.Info(
						"OBS bucket is not empty",
						"reason",
						condition.Reason,
						"forceDestroy",
						bucket.Spec.ForceDestroy,
						"error",
						err.Error(),
					)
					return ctrl.Result{}, r.updateBucketStatus(ctx, bucket, resolved, condition)
				}
				return r.patchStatus(ctx, bucket, resolved, err)
			}
		}

		if err := r.patchFinalizer(ctx, bucket, func() {
			controllerutil.RemoveFinalizer(bucket, bucketFinalizer)
		}); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("Removed bucket finalizer")
		return ctrl.Result{}, nil
	}

	if !hasFinalizer {
		if observation.exists {
			// A remote bucket that exists before our finalizer is set is unmanaged.
			err := fmt.Errorf(
				"%w: bucket %s already exists and is not managed by this Bucket",
				errBucketAlreadyExists,
				bucket.Name,
			)
			condition := bucketReadyStatusCondition(bucket, err)
			log.Info(
				"OBS bucket already exists and is unmanaged",
				"reason",
				condition.Reason,
				"error",
				err.Error(),
			)
			return r.patchStatus(ctx, bucket, resolved, err)
		}

		// No finalizer and no remote bucket means this controller can claim ownership.
		err := r.patchFinalizer(
			ctx,
			bucket,
			func() { controllerutil.AddFinalizer(bucket, bucketFinalizer) },
		)
		if err != nil {
			return ctrl.Result{}, err
		}
		log.Info("Added bucket finalizer")

		if err := r.create(ctx, obsClient, bucket, resolved.Region); err != nil {
			if errors.Is(err, errBucketAlreadyExists) {
				removeErr := r.patchFinalizer(ctx, bucket, func() {
					controllerutil.RemoveFinalizer(bucket, bucketFinalizer)
				})
				if removeErr == nil {
					log.Info("Removed bucket finalizer after create conflict")
				}
				if removeErr != nil {
					err = errors.Join(
						err,
						fmt.Errorf("remove finalizer after bucket create conflict: %w", removeErr),
					)
				}
			}
			return r.patchStatus(ctx, bucket, resolved, err)
		}
	} else if !observation.exists {
		// Finalizer present means we own the bucket, so recreate it if it drifted away.
		if err := r.create(ctx, obsClient, bucket, resolved.Region); err != nil {
			return r.patchStatus(ctx, bucket, resolved, err)
		}
	}

	if err := r.update(ctx, obsClient, bucket); err != nil {
		return r.patchStatus(ctx, bucket, resolved, err, observation.metadata)
	}

	observation, err = r.observe(ctx, obsClient, bucket)
	if err != nil {
		return r.patchStatus(ctx, bucket, resolved, err)
	}

	return r.patchStatus(ctx, bucket, resolved, nil, observation.metadata)
}

func (r *BucketReconciler) resolveBucketClient(
	ctx context.Context,
	bucket *obsv1alpha1.Bucket,
) (*provider.ResolvedClient, *obs.ObsClient, error) {
	resolver := r.ProviderResolver
	if resolver == nil {
		resolver = provider.NewProviderResolver(r.Client, provider.NewCache())
	}

	resolved, err := resolver.ResolveBucket(ctx, bucket)
	if err != nil {
		return nil, nil, err
	}

	return resolved, resolved.OBS, nil
}

func (r *BucketReconciler) observe(
	_ context.Context,
	obsClient *obs.ObsClient,
	bucket *obsv1alpha1.Bucket,
) (*bucketObservation, error) {
	metadata, err := obsClient.GetBucketMetadata(
		&obs.GetBucketMetadataInput{Bucket: bucket.Name},
	)
	if err != nil {
		if isBucketNotFound(err) {
			return &bucketObservation{}, nil
		}
		return nil, fmt.Errorf("observe bucket %s: %w", bucket.Name, err)
	}

	return &bucketObservation{
		exists:   true,
		metadata: metadata,
	}, nil
}

func (r *BucketReconciler) create(
	ctx context.Context,
	obsClient *obs.ObsClient,
	bucket *obsv1alpha1.Bucket,
	region string,
) error {
	input := &obs.CreateBucketInput{
		Bucket:            bucket.Name,
		ACL:               bucketACL(bucket),
		IsFSFileInterface: bucket.Spec.ParallelFS,
		StorageClass:      bucketStorageClass(bucket),
	}
	input.Location = region

	if _, err := obsClient.CreateBucket(input); err != nil {
		if isCreateBucketConflict(err) {
			return fmt.Errorf("%w: create bucket %s: %v", errBucketAlreadyExists, bucket.Name, err)
		}
		return fmt.Errorf("create bucket %s: %w", bucket.Name, err)
	}
	logf.FromContext(ctx).Info(
		"Created OBS bucket",
		"region",
		region,
		"storageClass",
		input.StorageClass,
		"acl",
		input.ACL,
		"parallelFS",
		input.IsFSFileInterface,
	)
	return nil
}

func (r *BucketReconciler) update(
	ctx context.Context,
	obsClient *obs.ObsClient,
	bucket *obsv1alpha1.Bucket,
) error {
	steps := []func(context.Context, *obs.ObsClient, *obsv1alpha1.Bucket) error{
		reconcileBucketStorageClass,
		reconcileBucketACL,
		reconcileBucketVersioning,
		reconcileBucketTags,
		reconcileBucketLogging,
		reconcileBucketEncryption,
	}

	for _, step := range steps {
		if err := step(ctx, obsClient, bucket); err != nil {
			return err
		}
	}

	return nil
}

func (r *BucketReconciler) delete(
	ctx context.Context,
	obsClient *obs.ObsClient,
	bucket *obsv1alpha1.Bucket,
) error {
	_, err := obsClient.DeleteBucket(bucket.Name)
	if err != nil {
		if isBucketNotFound(err) {
			return nil
		}
		if isBucketNotEmpty(err) && bucket.Spec.ForceDestroy {
			destroyErr := deleteAllBucketObjects(ctx, obsClient, bucket.Name)
			if destroyErr != nil {
				return destroyErr
			}
			_, err = obsClient.DeleteBucket(bucket.Name)
		}
	}

	if err != nil {
		return fmt.Errorf("delete bucket %s: %w", bucket.Name, err)
	}
	logf.FromContext(ctx).Info(
		"Deleted OBS bucket",
		"forceDestroy",
		bucket.Spec.ForceDestroy,
	)
	return nil
}

func reconcileBucketStorageClass(
	ctx context.Context,
	obsClient *obs.ObsClient,
	bucket *obsv1alpha1.Bucket,
) error {
	desired := normalizeStorageClass(string(bucketStorageClass(bucket)))
	current, err := obsClient.GetBucketStoragePolicy(bucket.Name)
	if err != nil {
		return fmt.Errorf("read bucket storage class: %w", err)
	}

	if normalizeStorageClass(current.StorageClass) == desired {
		return nil
	}

	_, err = obsClient.SetBucketStoragePolicy(
		&obs.SetBucketStoragePolicyInput{
			Bucket: bucket.Name,
			BucketStoragePolicy: obs.BucketStoragePolicy{
				StorageClass: obs.StorageClassType(desired),
			},
		},
	)
	if err != nil {
		return fmt.Errorf("set bucket storage class: %w", err)
	}
	logf.FromContext(ctx).Info(
		"Updated bucket storage class",
		"from",
		current.StorageClass,
		"to",
		desired,
	)
	return nil
}

func reconcileBucketACL(
	ctx context.Context,
	obsClient *obs.ObsClient,
	bucket *obsv1alpha1.Bucket,
) error {
	desired := bucketACL(bucket)
	current, err := obsClient.GetBucketAcl(bucket.Name)
	if err != nil {
		return fmt.Errorf("read bucket acl: %w", err)
	}

	if currentACL, ok := flattenObsBucketCannedACL(current); ok && currentACL == desired {
		return nil
	}

	_, err = obsClient.SetBucketAcl(
		&obs.SetBucketAclInput{
			Bucket: bucket.Name,
			ACL:    desired,
		},
	)
	if err != nil {
		return fmt.Errorf("set bucket acl: %w", err)
	}
	keysAndValues := []any{"to", desired}
	if currentACL, ok := flattenObsBucketCannedACL(current); ok {
		keysAndValues = append(keysAndValues, "from", currentACL)
	}
	logf.FromContext(ctx).Info("Updated bucket ACL", keysAndValues...)
	return nil
}

func reconcileBucketVersioning(
	ctx context.Context,
	obsClient *obs.ObsClient,
	bucket *obsv1alpha1.Bucket,
) error {
	desired := obs.VersioningStatusSuspended
	if bucket.Spec.Versioning {
		desired = obs.VersioningStatusEnabled
	}

	current, err := obsClient.GetBucketVersioning(bucket.Name)
	if err != nil {
		return fmt.Errorf("read bucket versioning: %w", err)
	}
	if current.Status == desired {
		return nil
	}

	_, err = obsClient.SetBucketVersioning(
		&obs.SetBucketVersioningInput{
			Bucket: bucket.Name,
			BucketVersioningConfiguration: obs.BucketVersioningConfiguration{
				Status: desired,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("set bucket versioning: %w", err)
	}
	logf.FromContext(ctx).Info(
		"Updated bucket versioning",
		"from",
		current.Status,
		"to",
		desired,
	)
	return nil
}

func reconcileBucketTags(
	ctx context.Context,
	obsClient *obs.ObsClient,
	bucket *obsv1alpha1.Bucket,
) error {
	desired := bucket.Spec.Tags
	if desired == nil {
		desired = map[string]string{}
	}

	current, err := bucketTags(obsClient, bucket.Name)
	if err != nil {
		return err
	}

	if maps.Equal(current, desired) {
		return nil
	}
	if len(desired) == 0 {
		_, err = obsClient.DeleteBucketTagging(bucket.Name)
		if err != nil && !isBucketCode(err, obsNoSuchTagSetCode) && !isBucketNotFound(err) {
			return fmt.Errorf("delete bucket tags: %w", err)
		}
		logf.FromContext(ctx).Info("Cleared bucket tags")
		return nil
	}

	_, err = obsClient.SetBucketTagging(
		&obs.SetBucketTaggingInput{
			Bucket:        bucket.Name,
			BucketTagging: obs.BucketTagging{Tags: tagsFromMap(desired)},
		},
	)
	if err != nil {
		return fmt.Errorf("set bucket tags: %w", err)
	}
	logf.FromContext(ctx).Info(
		"Updated bucket tags",
		"tagCount",
		len(desired),
	)
	return nil
}

func reconcileBucketLogging(
	ctx context.Context,
	obsClient *obs.ObsClient,
	bucket *obsv1alpha1.Bucket,
) error {
	current, err := obsClient.GetBucketLoggingConfiguration(bucket.Name)
	if err != nil {
		return fmt.Errorf("read bucket logging: %w", err)
	}

	desired := loggingInput(bucket)
	if loggingMatches(current, desired.BucketLoggingStatus) {
		return nil
	}

	_, err = obsClient.SetBucketLoggingConfiguration(desired)
	if err != nil {
		return fmt.Errorf("set bucket logging: %w", err)
	}
	logf.FromContext(ctx).Info(
		"Updated bucket logging",
		"enabled",
		desired.TargetBucket != "",
		"targetBucket",
		desired.TargetBucket,
		"targetPrefix",
		desired.TargetPrefix,
	)
	return nil
}

func reconcileBucketEncryption(
	ctx context.Context,
	obsClient *obs.ObsClient,
	bucket *obsv1alpha1.Bucket,
) error {
	current, err := obsClient.GetBucketEncryption(bucket.Name)
	if err != nil {
		if !isBucketCode(err, obsNoSuchEncryptionConfigCode) &&
			!isBucketCode(err, obsFsNotSupportCode) &&
			!isBucketNotFound(err) {
			return fmt.Errorf("read bucket encryption: %w", err)
		}
		current = nil
	}

	if bucket.Spec.ServerSideEncryption == nil {
		if current == nil {
			return nil
		}
		_, err = obsClient.DeleteBucketEncryption(bucket.Name)
		if err != nil && !isBucketCode(err, obsNoSuchEncryptionConfigCode) &&
			!isBucketNotFound(err) {
			return fmt.Errorf("delete bucket encryption: %w", err)
		}
		logf.FromContext(ctx).Info("Cleared bucket encryption")
		return nil
	}

	desired := encryptionConfig(bucket.Spec.ServerSideEncryption)
	if encryptionMatches(current, desired) {
		return nil
	}

	_, err = obsClient.SetBucketEncryption(
		&obs.SetBucketEncryptionInput{
			Bucket:                        bucket.Name,
			BucketEncryptionConfiguration: desired,
		},
	)
	if err != nil {
		return fmt.Errorf("set bucket encryption: %w", err)
	}
	logf.FromContext(ctx).Info(
		"Updated bucket encryption",
		"kmsKeyIDSet",
		desired.KMSMasterKeyID != "",
		"kmsProjectIDSet",
		desired.ProjectID != "",
	)
	return nil
}

func bucketTags(obsClient *obs.ObsClient, bucketName string) (map[string]string, error) {
	output, err := obsClient.GetBucketTagging(bucketName)
	if err != nil {
		if isBucketCode(err, obsNoSuchTagSetCode) || isBucketNotFound(err) {
			return map[string]string{}, nil
		}
		return nil, fmt.Errorf("read bucket tags: %w", err)
	}

	tags := make(map[string]string, len(output.Tags))
	for _, tag := range output.Tags {
		tags[tag.Key] = tag.Value
	}
	return tags, nil
}

func tagsFromMap(src map[string]string) []obs.Tag {
	keys := make([]string, 0, len(src))
	for key := range src {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	tags := make([]obs.Tag, 0, len(keys))
	for _, key := range keys {
		tags = append(tags, obs.Tag{
			Key:   key,
			Value: src[key],
		})
	}
	return tags
}

func loggingInput(bucket *obsv1alpha1.Bucket) *obs.SetBucketLoggingConfigurationInput {
	input := &obs.SetBucketLoggingConfigurationInput{
		Bucket: bucket.Name,
	}

	if bucket.Spec.Logging == nil {
		return input
	}

	input.TargetBucket = bucket.Spec.Logging.TargetBucket
	input.TargetPrefix = bucket.Spec.Logging.TargetPrefix
	input.Agency = bucket.Spec.Logging.Agency
	return input
}

func loggingMatches(
	current *obs.GetBucketLoggingConfigurationOutput,
	desired obs.BucketLoggingStatus,
) bool {
	if current == nil {
		return desired.TargetBucket == "" && desired.TargetPrefix == "" && desired.Agency == ""
	}
	return current.TargetBucket == desired.TargetBucket &&
		current.TargetPrefix == desired.TargetPrefix &&
		current.Agency == desired.Agency
}

func encryptionConfig(
	spec *obsv1alpha1.BucketServerSideEncryptionSpec,
) obs.BucketEncryptionConfiguration {
	return obs.BucketEncryptionConfiguration{
		SSEAlgorithm:   bucketEncryptionAlgorithm,
		KMSMasterKeyID: spec.KMSKeyID,
		ProjectID:      spec.KMSProjectID,
	}
}

func encryptionMatches(
	current *obs.GetBucketEncryptionOutput,
	desired obs.BucketEncryptionConfiguration,
) bool {
	if current == nil {
		return false
	}
	return current.SSEAlgorithm == desired.SSEAlgorithm &&
		current.KMSMasterKeyID == desired.KMSMasterKeyID &&
		current.ProjectID == desired.ProjectID
}

func deleteAllBucketObjects(
	ctx context.Context,
	obsClient *obs.ObsClient,
	bucketName string,
) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		output, err := obsClient.ListVersions(&obs.ListVersionsInput{Bucket: bucketName})
		if err != nil {
			return fmt.Errorf("list bucket object versions: %w", err)
		}

		objects := make(
			[]obs.ObjectToDelete,
			0,
			len(output.Versions)+len(output.DeleteMarkers),
		)
		for _, version := range output.Versions {
			objects = append(objects, obs.ObjectToDelete{
				Key:       version.Key,
				VersionId: version.VersionId,
			})
		}
		for _, marker := range output.DeleteMarkers {
			objects = append(objects, obs.ObjectToDelete{
				Key:       marker.Key,
				VersionId: marker.VersionId,
			})
		}

		if len(objects) == 0 {
			return nil
		}

		deleteOutput, err := obsClient.DeleteObjects(
			&obs.DeleteObjectsInput{
				Bucket:  bucketName,
				Objects: objects,
			},
		)
		if err != nil {
			return fmt.Errorf("delete bucket object versions: %w", err)
		}
		if len(deleteOutput.Errors) > 0 {
			return fmt.Errorf(
				"delete bucket object versions: OBS returned %d object deletion errors",
				len(deleteOutput.Errors),
			)
		}
		logf.FromContext(ctx).Info(
			"Deleted OBS object versions",
			"objectCount",
			len(objects),
		)

		if len(objects) < 1000 && !output.IsTruncated {
			return nil
		}
	}
}

func (r *BucketReconciler) patchStatus(
	ctx context.Context,
	bucket *obsv1alpha1.Bucket,
	resolved *provider.ResolvedClient,
	reconcileErr error,
	metadata ...*obs.GetBucketMetadataOutput,
) (ctrl.Result, error) {
	condition := bucketReadyStatusCondition(bucket, reconcileErr)
	statusErr := r.updateBucketStatus(
		ctx,
		bucket,
		resolved,
		condition,
		metadata...,
	)
	if statusErr != nil {
		return ctrl.Result{}, statusErr
	}
	if isUserCorrectableBucketError(reconcileErr) {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, reconcileErr
}

func isUserCorrectableBucketError(err error) bool {
	if err == nil {
		return false
	}

	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		return allBucketErrorsUserCorrectable(joined.Unwrap())
	}

	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		child := wrapped.Unwrap()
		if _, childIsJoined := child.(interface{ Unwrap() []error }); childIsJoined {
			return isUserCorrectableBucketError(child)
		}
		if isUserCorrectableBucketErrorType(err) {
			return true
		}
		return isUserCorrectableBucketError(child)
	}

	return isUserCorrectableBucketErrorType(err)
}

func allBucketErrorsUserCorrectable(children []error) bool {
	if len(children) == 0 {
		return false
	}
	for _, child := range children {
		if !isUserCorrectableBucketError(child) {
			return false
		}
	}
	return true
}

func isUserCorrectableBucketErrorType(err error) bool {
	return errors.Is(err, provider.ErrProviderConfigNotFound) ||
		errors.Is(err, provider.ErrCredentialsSecretNotFound) ||
		errors.Is(err, provider.ErrMissingCredentials) ||
		errors.Is(err, provider.ErrInvalidEndpoint) ||
		errors.Is(err, errBucketAlreadyExists)
}

func bucketReadyStatusCondition(
	bucket *obsv1alpha1.Bucket,
	err error,
) metav1.Condition {
	condition := metav1.Condition{
		Type:               bucketReadyCondition,
		Status:             metav1.ConditionTrue,
		Reason:             "BucketReady",
		Message:            "OBS bucket is reconciled",
		ObservedGeneration: bucket.Generation,
	}

	if err == nil {
		return condition
	}

	condition.Status = metav1.ConditionFalse
	condition.Message = err.Error()
	switch {
	case errors.Is(err, provider.ErrProviderConfigNotFound):
		condition.Reason = "ProviderConfigNotFound"
	case errors.Is(err, provider.ErrCredentialsSecretNotFound):
		condition.Reason = conditionReasonCredentialsSecretNotFound
	case apierrors.IsNotFound(err):
		condition.Reason = "ProviderConfigNotFound"
	case errors.Is(err, provider.ErrMissingCredentials):
		condition.Reason = "CredentialsSecretInvalid"
	case errors.Is(err, provider.ErrInvalidEndpoint):
		condition.Reason = "InvalidEndpoint"
	case errors.Is(err, errBucketAlreadyExists):
		condition.Reason = "BucketAlreadyExists"
	case isBucketNotEmpty(err):
		condition.Reason = "BucketNotEmpty"
	case isBucketNotFound(err):
		condition.Reason = "BucketNotFound"
	default:
		condition.Reason = "ReconcileFailed"
	}
	return condition
}

func (r *BucketReconciler) updateBucketStatus(
	ctx context.Context,
	bucket *obsv1alpha1.Bucket,
	resolved *provider.ResolvedClient,
	condition metav1.Condition,
	metadata ...*obs.GetBucketMetadataOutput,
) error {
	original := bucket.DeepCopy()
	nextStatus := bucket.Status
	nextStatus.ObservedGeneration = bucket.Generation
	if resolved != nil {
		nextStatus.Region = resolved.Region
		nextStatus.BucketDomainName = bucketDomainName(bucket.Name, resolved.Region)
	}
	if len(metadata) > 0 && metadata[0] != nil {
		nextStatus.BucketVersion = metadata[0].Version
	}

	meta.SetStatusCondition(&nextStatus.Conditions, condition)

	statusChanged := original.Status.ObservedGeneration != nextStatus.ObservedGeneration ||
		original.Status.Region != nextStatus.Region ||
		original.Status.BucketDomainName != nextStatus.BucketDomainName ||
		original.Status.BucketVersion != nextStatus.BucketVersion ||
		original.Status.LastSyncTime == nil ||
		!reflect.DeepEqual(original.Status.Conditions, nextStatus.Conditions)
	if !statusChanged {
		return nil
	}

	now := metav1.Now()
	nextStatus.LastSyncTime = &now
	bucket.Status = nextStatus
	return r.Status().Patch(ctx, bucket, client.MergeFrom(original))
}

func (r *BucketReconciler) patchFinalizer(
	ctx context.Context,
	bucket *obsv1alpha1.Bucket,
	mutate func(),
) error {
	original := bucket.DeepCopy()
	mutate()
	return r.Patch(ctx, bucket, client.MergeFrom(original))
}

func bucketProviderConfigRefIndexer(rawObj client.Object) []string {
	bucket, ok := rawObj.(*obsv1alpha1.Bucket)
	if !ok || bucket.Spec.ProviderConfigRef.Name == "" {
		return nil
	}
	return []string{bucket.Spec.ProviderConfigRef.Name}
}

func (r *BucketReconciler) bucketsForProviderConfig(
	ctx context.Context,
	obj client.Object,
) []reconcile.Request {
	buckets := &obsv1alpha1.BucketList{}
	err := r.List(
		ctx,
		buckets,
		client.InNamespace(obj.GetNamespace()),
		client.MatchingFields{bucketProviderConfigRefIndex: obj.GetName()},
	)
	if err != nil {
		logf.FromContext(ctx).Error(
			err,
			"Failed to list Buckets for ProviderConfig",
			"name",
			obj.GetName(),
			"namespace",
			obj.GetNamespace(),
		)
		return nil
	}

	requests := make([]reconcile.Request, 0, len(buckets.Items))
	for _, bucket := range buckets.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: client.ObjectKey{
				Namespace: bucket.Namespace,
				Name:      bucket.Name,
			},
		})
	}
	return requests
}

type obsBucketGrantSignature struct {
	granteeType obs.GranteeType
	granteeID   string
	granteeURI  string
	permission  obs.PermissionType
	delivered   bool
}

func flattenObsBucketCannedACL(output *obs.GetBucketAclOutput) (obs.AclType, bool) {
	if output == nil || output.Owner.ID == "" {
		return "", false
	}

	ownerGrant := obsBucketGrantSignature{
		granteeType: obs.GranteeUser,
		granteeID:   output.Owner.ID,
		permission:  obs.PermissionFullControl,
	}
	publicReadGrant := obsBucketGrantSignature{
		granteeType: obs.GranteeGroup,
		granteeURI:  string(obs.GroupAllUsers),
		permission:  obs.PermissionRead,
	}
	publicWriteGrant := obsBucketGrantSignature{
		granteeType: obs.GranteeGroup,
		granteeURI:  string(obs.GroupAllUsers),
		permission:  obs.PermissionWrite,
	}
	logDeliveryWriteGrant := obsBucketGrantSignature{
		granteeType: obs.GranteeGroup,
		granteeURI:  string(obs.GroupLogDelivery),
		permission:  obs.PermissionWrite,
	}
	logDeliveryReadACPGrant := obsBucketGrantSignature{
		granteeType: obs.GranteeGroup,
		granteeURI:  string(obs.GroupLogDelivery),
		permission:  obs.PermissionReadAcp,
	}

	switch {
	case obsBucketACLMatches(output.Grants, ownerGrant):
		return obs.AclPrivate, true
	case obsBucketACLMatches(output.Grants, ownerGrant, publicReadGrant):
		return obs.AclPublicRead, true
	case obsBucketACLMatches(output.Grants, ownerGrant, publicReadGrant, publicWriteGrant):
		return obs.AclPublicReadWrite, true
	case obsBucketACLMatches(output.Grants, ownerGrant, logDeliveryWriteGrant, logDeliveryReadACPGrant):
		return obs.AclLogDeliveryWrite, true
	default:
		return "", false
	}
}

func obsBucketACLMatches(actual []obs.Grant, expected ...obsBucketGrantSignature) bool {
	if len(actual) != len(expected) {
		return false
	}

	actualSet := make(map[obsBucketGrantSignature]struct{}, len(actual))
	for _, grant := range actual {
		signature := obsBucketGrantSignature{
			granteeType: grant.Grantee.Type,
			granteeID:   grant.Grantee.ID,
			granteeURI:  normalizeObsBucketGrantURI(grant.Grantee.Type, grant.Grantee.URI),
			permission:  grant.Permission,
			delivered:   grant.Delivered,
		}
		actualSet[signature] = struct{}{}
	}

	for _, grant := range expected {
		if _, ok := actualSet[grant]; !ok {
			return false
		}
	}

	return true
}

func normalizeObsBucketGrantURI(granteeType obs.GranteeType, granteeURI obs.GroupUriType) string {
	if granteeType == obs.GranteeUser {
		return ""
	}

	uri := string(granteeURI)
	if uri == "" {
		return ""
	}

	parts := strings.Split(uri, "/")
	return parts[len(parts)-1]
}

func normalizeStorageClass(class string) string {
	switch class {
	case "STANDARD_IA":
		return string(obs.StorageClassWarm)
	case "GLACIER":
		return string(obs.StorageClassCold)
	default:
		return class
	}
}

func bucketStorageClass(bucket *obsv1alpha1.Bucket) obs.StorageClassType {
	if bucket.Spec.StorageClass == "" {
		return obs.StorageClassStandard
	}
	return obs.StorageClassType(bucket.Spec.StorageClass)
}

func bucketACL(bucket *obsv1alpha1.Bucket) obs.AclType {
	if bucket.Spec.ACL == "" {
		return obs.AclPrivate
	}
	return obs.AclType(bucket.Spec.ACL)
}

func bucketDomainName(bucketName, region string) string {
	return fmt.Sprintf("%s.obs.%s.otc.t-systems.com", bucketName, region)
}

func isBucketNotFound(err error) bool {
	var obsErr obs.ObsError
	if !errors.As(err, &obsErr) {
		return false
	}
	return obsErr.StatusCode == 404 || obsErr.Code == "NoSuchBucket"
}

func isBucketNotEmpty(err error) bool {
	return isBucketCode(err, "BucketNotEmpty")
}

func isCreateBucketConflict(err error) bool {
	var obsErr obs.ObsError
	if !errors.As(err, &obsErr) {
		return false
	}
	return obsErr.StatusCode == 409 ||
		obsErr.Code == obsBucketAlreadyExistsCode ||
		obsErr.Code == obsBucketAlreadyOwnedByYou
}

func isBucketCode(err error, code string) bool {
	var obsErr obs.ObsError
	if !errors.As(err, &obsErr) {
		return false
	}
	return obsErr.Code == code
}

// SetupWithManager sets up the controller with the Manager.
func (r *BucketReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&obsv1alpha1.Bucket{},
		bucketProviderConfigRefIndex,
		bucketProviderConfigRefIndexer,
	); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&obsv1alpha1.Bucket{}).
		Watches(
			&obsv1alpha1.ProviderConfig{},
			handler.EnqueueRequestsFromMapFunc(r.bucketsForProviderConfig),
		).
		Named("bucket").
		Complete(r)
}
