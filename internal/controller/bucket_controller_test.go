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
	"net/http"
	"net/http/httptest"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/opentelekomcloud/gophertelekomcloud/openstack/obs"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	obsv1alpha1 "go.wilaris.de/obs-operator/api/v1alpha1"
	"go.wilaris.de/obs-operator/internal/provider"
)

var _ = Describe("Bucket Controller", func() {
	const namespace = "default"

	var (
		ctx    context.Context
		scheme *runtime.Scheme
	)

	BeforeEach(func() {
		ctx = context.Background()
		scheme = runtime.NewScheme()
		Expect(corev1.AddToScheme(scheme)).To(Succeed())
		Expect(obsv1alpha1.AddToScheme(scheme)).To(Succeed())
	})

	It("sets Ready false when the provider cannot be resolved", func() {
		bucket := testBucket("missing-provider", obsv1alpha1.BucketSpec{
			ProviderConfigRef: corev1.LocalObjectReference{Name: "missing"},
		})
		k8sClient := testClient(scheme, bucket)
		reconciler := testBucketReconciler(k8sClient)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: bucket.Name, Namespace: bucket.Namespace},
		})
		Expect(err).To(HaveOccurred())

		current := &obsv1alpha1.Bucket{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bucket), current)).To(Succeed())
		Expect(current.Finalizers).NotTo(ContainElement(bucketFinalizer))
		condition := meta.FindStatusCondition(current.Status.Conditions, bucketReadyCondition)
		Expect(condition).NotTo(BeNil())
		Expect(condition.Status).To(Equal(metav1.ConditionFalse))
		Expect(condition.Reason).To(Equal("ProviderConfigNotFound"))
	})

	It("does not adopt a pre-existing OBS bucket", func() {
		var observeRequests atomic.Int32
		var mutationRequests atomic.Int32
		var unexpectedRequests atomic.Int32
		server := httptest.NewTLSServer(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead && r.URL.Path == "/existing-bucket" &&
					r.URL.RawQuery == "" {
					observeRequests.Add(1)
					w.WriteHeader(http.StatusOK)
					return
				}
				if r.Method == http.MethodPut ||
					r.Method == http.MethodPost ||
					r.Method == http.MethodDelete {
					mutationRequests.Add(1)
					w.WriteHeader(http.StatusTeapot)
					return
				}

				unexpectedRequests.Add(1)
				w.WriteHeader(http.StatusTeapot)
			}),
		)
		DeferCleanup(server.Close)

		providerConfig := &obsv1alpha1.ProviderConfig{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "provider",
				Namespace: namespace,
			},
			Spec: obsv1alpha1.ProviderConfigSpec{
				Region:   "eu-de",
				Endpoint: server.URL,
				CredentialsSecretRef: corev1.LocalObjectReference{
					Name: "obs-credentials",
				},
			},
		}
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "obs-credentials",
				Namespace: namespace,
			},
			Data: map[string][]byte{
				provider.AccessKeyIDSecretKey:     []byte("access-key"),
				provider.SecretAccessKeySecretKey: []byte("secret-key"),
			},
		}
		bucket := testBucket("existing-bucket", obsv1alpha1.BucketSpec{
			ProviderConfigRef: corev1.LocalObjectReference{Name: "provider"},
		})
		k8sClient := testClient(scheme, providerConfig, secret, bucket)
		resolver := provider.NewProviderResolver(
			k8sClient,
			provider.NewCache(),
			provider.WithOBSClientFactory(func(
				credentials provider.Credentials,
				endpoint string,
				region string,
			) (*obs.ObsClient, error) {
				return obs.New(
					credentials.AccessKeyID,
					credentials.SecretAccessKey,
					endpoint,
					obs.WithPathStyle(true),
					obs.WithRegion(region),
					obs.WithMaxRetryCount(0),
					obs.WithSslVerify(false),
				)
			}),
		)
		reconciler := &BucketReconciler{Client: k8sClient, ProviderResolver: resolver}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: bucket.Name, Namespace: bucket.Namespace},
		})
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, errBucketAlreadyExists)).To(BeTrue())

		current := &obsv1alpha1.Bucket{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bucket), current)).To(Succeed())
		Expect(current.Finalizers).NotTo(ContainElement(bucketFinalizer))
		condition := meta.FindStatusCondition(current.Status.Conditions, bucketReadyCondition)
		Expect(condition).NotTo(BeNil())
		Expect(condition.Status).To(Equal(metav1.ConditionFalse))
		Expect(condition.Reason).To(Equal("BucketAlreadyExists"))
		Expect(observeRequests.Load()).To(Equal(int32(1)))
		Expect(mutationRequests.Load()).To(Equal(int32(0)))
		Expect(unexpectedRequests.Load()).To(Equal(int32(0)))
	})

	It("maps ProviderConfig events to same-namespace Buckets", func() {
		matching := testBucket("matching", obsv1alpha1.BucketSpec{
			ProviderConfigRef: corev1.LocalObjectReference{Name: "provider"},
		})
		otherProvider := testBucket("other-provider", obsv1alpha1.BucketSpec{
			ProviderConfigRef: corev1.LocalObjectReference{Name: "other"},
		})
		otherNamespace := testBucket("other-namespace", obsv1alpha1.BucketSpec{
			ProviderConfigRef: corev1.LocalObjectReference{Name: "provider"},
		})
		otherNamespace.Namespace = "other"

		k8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithIndex(
				&obsv1alpha1.Bucket{},
				bucketProviderConfigRefIndex,
				bucketProviderConfigRefIndexer,
			).
			WithObjects(matching, otherProvider, otherNamespace).
			Build()
		reconciler := &BucketReconciler{Client: k8sClient}

		requests := reconciler.bucketsForProviderConfig(ctx, &obsv1alpha1.ProviderConfig{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "provider",
				Namespace: namespace,
			},
		})

		Expect(requests).To(ConsistOf(reconcile.Request{
			NamespacedName: types.NamespacedName{Name: "matching", Namespace: namespace},
		}))
	})

	It("builds bucket OBS helper values", func() {
		bucket := testBucket("helper-bucket", obsv1alpha1.BucketSpec{
			StorageClass: obsv1alpha1.BucketStorageClassWarm,
			ACL:          obsv1alpha1.BucketACLLogDeliveryWrite,
			Logging: &obsv1alpha1.BucketLoggingSpec{
				TargetBucket: "logs-bucket",
				TargetPrefix: "logs/",
				Agency:       "obs-log-agency",
			},
			ServerSideEncryption: &obsv1alpha1.BucketServerSideEncryptionSpec{
				KMSKeyID:     "kms-key",
				KMSProjectID: "project-id",
			},
		})

		Expect(bucketStorageClass(bucket)).To(Equal(obs.StorageClassWarm))
		Expect(bucketACL(bucket)).To(Equal(obs.AclLogDeliveryWrite))
		Expect(bucketDomainName(bucket.Name, "eu-de")).To(Equal(
			"helper-bucket.obs.eu-de.otc.t-systems.com",
		))

		logging := loggingInput(bucket)
		Expect(logging.Bucket).To(Equal(bucket.Name))
		Expect(logging.TargetBucket).To(Equal("logs-bucket"))
		Expect(logging.TargetPrefix).To(Equal("logs/"))
		Expect(logging.Agency).To(Equal("obs-log-agency"))

		encryption := encryptionConfig(bucket.Spec.ServerSideEncryption)
		Expect(encryption.SSEAlgorithm).To(Equal(bucketEncryptionAlgorithm))
		Expect(encryption.KMSMasterKeyID).To(Equal("kms-key"))
		Expect(encryption.ProjectID).To(Equal("project-id"))

		acl, ok := flattenObsBucketCannedACL(cannedACLOutput("owner", obs.AclLogDeliveryWrite))
		Expect(ok).To(BeTrue())
		Expect(acl).To(Equal(obs.AclLogDeliveryWrite))
	})

	It("classifies OBS bucket errors", func() {
		Expect(isBucketNotFound(obsError("NoSuchBucket", 404))).To(BeTrue())
		Expect(isBucketNotFound(obsError("OtherCode", 404))).To(BeTrue())
		Expect(isBucketNotEmpty(obsError("BucketNotEmpty", 409))).To(BeTrue())
		Expect(isCreateBucketConflict(obsError(obsBucketAlreadyExistsCode, 409))).To(BeTrue())
		Expect(isCreateBucketConflict(obsError(obsBucketAlreadyOwnedByYou, 409))).To(BeTrue())
		Expect(isBucketCode(obsError(obsNoSuchTagSetCode, 404), obsNoSuchTagSetCode)).To(BeTrue())
		Expect(isBucketNotFound(obsError("BucketNotEmpty", 409))).To(BeFalse())
	})
})

func testBucket(name string, spec obsv1alpha1.BucketSpec) *obsv1alpha1.Bucket {
	return &obsv1alpha1.Bucket{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: spec,
	}
}

func testClient(scheme *runtime.Scheme, objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&obsv1alpha1.Bucket{}, &obsv1alpha1.ProviderConfig{}).
		WithObjects(objects...).
		Build()
}

func testBucketReconciler(k8sClient client.Client) *BucketReconciler {
	return &BucketReconciler{
		Client:           k8sClient,
		ProviderResolver: provider.NewProviderResolver(k8sClient, provider.NewCache()),
	}
}

func cannedACLOutput(ownerID string, acl obs.AclType) *obs.GetBucketAclOutput {
	grants := []obs.Grant{
		{
			Grantee: obs.Grantee{
				Type: obs.GranteeUser,
				ID:   ownerID,
			},
			Permission: obs.PermissionFullControl,
		},
	}

	switch acl {
	case obs.AclPublicRead:
		grants = append(grants, obs.Grant{
			Grantee: obs.Grantee{
				Type: obs.GranteeGroup,
				URI:  obs.GroupAllUsers,
			},
			Permission: obs.PermissionRead,
		})
	case obs.AclPublicReadWrite:
		grants = append(grants,
			obs.Grant{
				Grantee: obs.Grantee{
					Type: obs.GranteeGroup,
					URI:  obs.GroupAllUsers,
				},
				Permission: obs.PermissionRead,
			},
			obs.Grant{
				Grantee: obs.Grantee{
					Type: obs.GranteeGroup,
					URI:  obs.GroupAllUsers,
				},
				Permission: obs.PermissionWrite,
			},
		)
	case obs.AclLogDeliveryWrite:
		grants = append(grants,
			obs.Grant{
				Grantee: obs.Grantee{
					Type: obs.GranteeGroup,
					URI:  obs.GroupLogDelivery,
				},
				Permission: obs.PermissionWrite,
			},
			obs.Grant{
				Grantee: obs.Grantee{
					Type: obs.GranteeGroup,
					URI:  obs.GroupLogDelivery,
				},
				Permission: obs.PermissionReadAcp,
			},
		)
	}

	return &obs.GetBucketAclOutput{
		AccessControlPolicy: obs.AccessControlPolicy{
			Owner:  obs.Owner{ID: ownerID},
			Grants: grants,
		},
	}
}

func obsError(code string, statusCode int) obs.ObsError {
	return obs.ObsError{
		BaseModel: obs.BaseModel{StatusCode: statusCode},
		Code:      code,
		Message:   code,
	}
}
