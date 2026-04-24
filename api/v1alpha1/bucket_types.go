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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BucketStorageClass defines the OBS storage class used for objects in a bucket.
type BucketStorageClass string

const (
	// BucketStorageClassStandard stores objects in the OBS standard storage class.
	BucketStorageClassStandard BucketStorageClass = "STANDARD"
	// BucketStorageClassWarm stores objects in the OBS warm storage class.
	BucketStorageClassWarm BucketStorageClass = "WARM"
	// BucketStorageClassCold stores objects in the OBS cold storage class.
	BucketStorageClassCold BucketStorageClass = "COLD"
)

// BucketACL defines the ACL applied to an OBS bucket.
type BucketACL string

const (
	// BucketACLPrivate grants access only to the bucket owner.
	BucketACLPrivate BucketACL = "private"
	// BucketACLPublicRead grants public read access.
	BucketACLPublicRead BucketACL = "public-read"
	// BucketACLPublicReadWrite grants public read and write access.
	BucketACLPublicReadWrite BucketACL = "public-read-write"
	// BucketACLLogDeliveryWrite grants OBS log delivery write access.
	BucketACLLogDeliveryWrite BucketACL = "log-delivery-write"
)

// BucketLoggingSpec defines OBS access logging for a bucket.
type BucketLoggingSpec struct {
	// TargetBucket is the OBS bucket that receives access logs.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	TargetBucket string `json:"targetBucket"`

	// TargetPrefix is the prefix used for access log objects.
	// +optional
	// +kubebuilder:default:="logs/"
	// +kubebuilder:validation:MinLength=1
	TargetPrefix string `json:"targetPrefix,omitempty"`

	// Agency is the optional agency used by OBS for log delivery.
	// +optional
	// +kubebuilder:validation:MinLength=1
	Agency string `json:"agency,omitempty"`
}

// BucketServerSideEncryptionSpec defines default server-side encryption for a bucket.
type BucketServerSideEncryptionSpec struct {
	// KMSKeyID is the KMS key used for bucket default encryption.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	KMSKeyID string `json:"kmsKeyID"`

	// KMSProjectID is the optional KMS project ID used for bucket default encryption.
	// +optional
	// +kubebuilder:validation:MinLength=1
	KMSProjectID string `json:"kmsProjectID,omitempty"`
}

// BucketSpec defines the desired state of Bucket.
type BucketSpec struct {
	// ProviderConfigRef references the same-namespace ProviderConfig used to create an OBS client.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="has(self.name) && self.name != ''",message="providerConfigRef.name is required"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="providerConfigRef is immutable"
	ProviderConfigRef corev1.LocalObjectReference `json:"providerConfigRef"`

	// StorageClass is the OBS storage class applied to the bucket.
	// +optional
	// +kubebuilder:default:=STANDARD
	// +kubebuilder:validation:Enum=STANDARD;WARM;COLD
	StorageClass BucketStorageClass `json:"storageClass,omitempty"`

	// ACL is the canned OBS bucket ACL.
	// +optional
	// +kubebuilder:default:=private
	// +kubebuilder:validation:Enum=private;public-read;public-read-write;log-delivery-write
	ACL BucketACL `json:"acl,omitempty"`

	// Versioning enables OBS bucket versioning when true.
	// +optional
	// +kubebuilder:default:=false
	Versioning bool `json:"versioning,omitempty"`

	// ParallelFS creates the bucket with the OBS parallel file system interface.
	// This setting is immutable after bucket creation.
	// +optional
	// +kubebuilder:default:=false
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="parallelFS is immutable"
	ParallelFS bool `json:"parallelFS,omitempty"`

	// Tags are OBS bucket tags.
	// +optional
	Tags map[string]string `json:"tags,omitempty"`

	// ForceDestroy deletes all object versions and delete markers before deleting the bucket.
	// +optional
	// +kubebuilder:default:=false
	ForceDestroy bool `json:"forceDestroy,omitempty"`

	// Logging configures OBS bucket access logging.
	// +optional
	Logging *BucketLoggingSpec `json:"logging,omitempty"`

	// ServerSideEncryption configures OBS bucket default server-side encryption.
	// OBS uses the kms algorithm for this setting.
	// +optional
	ServerSideEncryption *BucketServerSideEncryptionSpec `json:"serverSideEncryption,omitempty"`
}

// BucketStatus defines the observed state of Bucket.
type BucketStatus struct {
	// ObservedGeneration is the latest generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Region is the ProviderConfig region used for the last successful observation.
	// +optional
	Region string `json:"region,omitempty"`

	// BucketDomainName is the OBS DNS name for the bucket.
	// +optional
	BucketDomainName string `json:"bucketDomainName,omitempty"`

	// BucketVersion is the OBS bucket metadata version.
	// +optional
	BucketVersion string `json:"bucketVersion,omitempty"`

	// LastSyncTime is the last time the bucket was observed or reconciled.
	// +optional
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty"`

	// Conditions represent the current state of the Bucket resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=bkt
// +kubebuilder:printcolumn:name="ProviderConfig",type=string,JSONPath=".spec.providerConfigRef.name"
// +kubebuilder:printcolumn:name="StorageClass",type=string,JSONPath=".spec.storageClass"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
// +kubebuilder:validation:XValidation:rule="self.metadata.name.matches('^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$')",message="metadata.name must be an OBS bucket name: 3-63 lowercase letters, digits, or hyphens, starting and ending with a letter or digit"

// Bucket is the Schema for the buckets API
type Bucket struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Bucket
	// +required
	Spec BucketSpec `json:"spec"`

	// status defines the observed state of Bucket
	// +optional
	Status BucketStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// BucketList contains a list of Bucket
type BucketList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Bucket `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Bucket{}, &BucketList{})
}
