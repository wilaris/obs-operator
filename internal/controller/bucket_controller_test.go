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
	"net/http"
	"net/http/httptest"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/opentelekomcloud/gophertelekomcloud/openstack/obs"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
		Expect(err).NotTo(HaveOccurred())

		current := &obsv1alpha1.Bucket{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bucket), current)).To(Succeed())
		Expect(current.Finalizers).NotTo(ContainElement(bucketFinalizer))
		condition := meta.FindStatusCondition(current.Status.Conditions, bucketReadyCondition)
		Expect(condition).NotTo(BeNil())
		Expect(condition.Status).To(Equal(metav1.ConditionFalse))
		Expect(condition.Reason).To(Equal("ProviderConfigNotFound"))
	})

	It("returns nil after status for user-correctable provider resolution failures", func() {
		cases := []struct {
			name    string
			objects []client.Object
			reason  string
		}{
			{
				name: "missing-credentials-secret",
				objects: []client.Object{
					&obsv1alpha1.ProviderConfig{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "provider",
							Namespace: namespace,
						},
						Spec: obsv1alpha1.ProviderConfigSpec{
							Region: "eu-de",
							CredentialsSecretRef: corev1.LocalObjectReference{
								Name: "missing-credentials",
							},
						},
					},
				},
				reason: "CredentialsSecretNotFound",
			},
			{
				name: "missing-credential-keys",
				objects: []client.Object{
					&obsv1alpha1.ProviderConfig{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "provider",
							Namespace: namespace,
						},
						Spec: obsv1alpha1.ProviderConfigSpec{
							Region: "eu-de",
							CredentialsSecretRef: corev1.LocalObjectReference{
								Name: "obs-credentials",
							},
						},
					},
					&corev1.Secret{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "obs-credentials",
							Namespace: namespace,
						},
						Data: map[string][]byte{
							provider.AccessKeyIDSecretKey: []byte("access-key"),
						},
					},
				},
				reason: "CredentialsSecretInvalid",
			},
			{
				name: "invalid-endpoint",
				objects: []client.Object{
					&obsv1alpha1.ProviderConfig{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "provider",
							Namespace: namespace,
						},
						Spec: obsv1alpha1.ProviderConfigSpec{
							Region:   "eu-de",
							Endpoint: "http://obs.test",
							CredentialsSecretRef: corev1.LocalObjectReference{
								Name: "obs-credentials",
							},
						},
					},
					&corev1.Secret{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "obs-credentials",
							Namespace: namespace,
						},
						Data: map[string][]byte{
							provider.AccessKeyIDSecretKey:     []byte("access-key"),
							provider.SecretAccessKeySecretKey: []byte("secret-key"),
						},
					},
				},
				reason: "InvalidEndpoint",
			},
		}

		for _, tc := range cases {
			By(tc.name)
			bucket := testBucket(tc.name, obsv1alpha1.BucketSpec{
				ProviderConfigRef: corev1.LocalObjectReference{Name: "provider"},
			})
			objects := append([]client.Object{bucket}, tc.objects...)
			k8sClient := testClient(scheme, objects...)
			reconciler := testBucketReconciler(k8sClient)

			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      bucket.Name,
					Namespace: bucket.Namespace,
				},
			})
			Expect(err).NotTo(HaveOccurred())

			current := &obsv1alpha1.Bucket{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bucket), current)).To(Succeed())
			Expect(current.Finalizers).NotTo(ContainElement(bucketFinalizer))
			condition := meta.FindStatusCondition(current.Status.Conditions, bucketReadyCondition)
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(tc.reason))
		}
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
		Expect(err).NotTo(HaveOccurred())

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

	It("keeps returning errors for transient OBS failures", func() {
		var observeRequests atomic.Int32
		server := httptest.NewTLSServer(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead && r.URL.Path == "/transient-bucket" &&
					r.URL.RawQuery == "" {
					observeRequests.Add(1)
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(
						`<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code><Message>temporary failure</Message></Error>`,
					))
					return
				}

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
		bucket := testBucket("transient-bucket", obsv1alpha1.BucketSpec{
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
		Expect(observeRequests.Load()).To(Equal(int32(1)))

		current := &obsv1alpha1.Bucket{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bucket), current)).To(Succeed())
		condition := meta.FindStatusCondition(current.Status.Conditions, bucketReadyCondition)
		Expect(condition).NotTo(BeNil())
		Expect(condition.Status).To(Equal(metav1.ConditionFalse))
		Expect(condition.Reason).To(Equal("ReconcileFailed"))
	})

	It("returns nil after status for non-retryable OBS request failures", func() {
		var observeRequests atomic.Int32
		var createRequests atomic.Int32
		server := httptest.NewTLSServer(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead && r.URL.Path == "/invalid-epid-bucket" &&
					r.URL.RawQuery == "" {
					observeRequests.Add(1)
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(http.StatusNotFound)
					_, _ = w.Write([]byte(
						`<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchBucket</Code><Message>bucket not found</Message></Error>`,
					))
					return
				}

				if r.Method == http.MethodPut && r.URL.Path == "/invalid-epid-bucket" &&
					r.URL.RawQuery == "" {
					createRequests.Add(1)
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte(
						`<?xml version="1.0" encoding="UTF-8"?><Error><Code>InvalidArgument</Code><Message>The enterprise project id is invalid</Message></Error>`,
					))
					return
				}

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
		bucket := testBucket("invalid-epid-bucket", obsv1alpha1.BucketSpec{
			ProviderConfigRef:   corev1.LocalObjectReference{Name: "provider"},
			EnterpriseProjectID: "invalid-epid",
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
		Expect(err).NotTo(HaveOccurred())

		current := &obsv1alpha1.Bucket{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bucket), current)).To(Succeed())
		Expect(current.Finalizers).To(ContainElement(bucketFinalizer))
		condition := meta.FindStatusCondition(current.Status.Conditions, bucketReadyCondition)
		Expect(condition).NotTo(BeNil())
		Expect(condition.Status).To(Equal(metav1.ConditionFalse))
		Expect(condition.Reason).To(Equal("RequestRejected"))
		Expect(condition.Message).To(Equal("The enterprise project id is invalid"))
		Expect(condition.Message).NotTo(ContainSubstring(bucket.Name))
		Expect(condition.Message).NotTo(ContainSubstring("Status=400"))
		Expect(condition.Message).NotTo(ContainSubstring("InvalidArgument"))
		Expect(condition.Message).NotTo(ContainSubstring("RequestId="))
		Expect(observeRequests.Load()).To(Equal(int32(1)))
		Expect(createRequests.Load()).To(Equal(int32(1)))
	})

	It("passes enterprise project ID when creating buckets", func() {
		var createRequests atomic.Int32
		epidHeaders := make(chan string, 1)
		server := httptest.NewServer(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut && r.URL.Path == "/enterprise-bucket" &&
					r.URL.RawQuery == "" {
					createRequests.Add(1)
					epidHeaders <- r.Header.Get("X-Amz-Epid")
					w.WriteHeader(http.StatusOK)
					return
				}

				w.WriteHeader(http.StatusTeapot)
			}),
		)
		DeferCleanup(server.Close)

		obsClient, err := obs.New(
			"access-key",
			"secret-key",
			server.URL,
			obs.WithPathStyle(true),
			obs.WithRegion("eu-de"),
			obs.WithMaxRetryCount(0),
		)
		Expect(err).NotTo(HaveOccurred())

		bucket := testBucket("enterprise-bucket", obsv1alpha1.BucketSpec{
			ProviderConfigRef:   corev1.LocalObjectReference{Name: "provider"},
			EnterpriseProjectID: "eps-123",
		})

		reconciler := &BucketReconciler{}
		Expect(reconciler.create(ctx, obsClient, bucket, "eu-de")).To(Succeed())
		Expect(createRequests.Load()).To(Equal(int32(1)))
		Expect(epidHeaders).To(Receive(Equal("eps-123")))
	})

	It("does not pass enterprise project ID when omitted", func() {
		var createRequests atomic.Int32
		epidHeaders := make(chan string, 1)
		server := httptest.NewServer(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut && r.URL.Path == "/standard-bucket" &&
					r.URL.RawQuery == "" {
					createRequests.Add(1)
					epidHeaders <- r.Header.Get("X-Amz-Epid")
					w.WriteHeader(http.StatusOK)
					return
				}

				w.WriteHeader(http.StatusTeapot)
			}),
		)
		DeferCleanup(server.Close)

		obsClient, err := obs.New(
			"access-key",
			"secret-key",
			server.URL,
			obs.WithPathStyle(true),
			obs.WithRegion("eu-de"),
			obs.WithMaxRetryCount(0),
		)
		Expect(err).NotTo(HaveOccurred())

		bucket := testBucket("standard-bucket", obsv1alpha1.BucketSpec{
			ProviderConfigRef: corev1.LocalObjectReference{Name: "provider"},
		})

		reconciler := &BucketReconciler{}
		Expect(reconciler.create(ctx, obsClient, bucket, "eu-de")).To(Succeed())
		Expect(createRequests.Load()).To(Equal(int32(1)))
		Expect(epidHeaders).To(Receive(BeEmpty()))
	})

	It("blocks deletion of non-empty buckets without force destroy", func() {
		var observeRequests atomic.Int32
		var bucketDeleteRequests atomic.Int32
		var versionListRequests atomic.Int32
		var objectDeleteRequests atomic.Int32
		var unexpectedRequests atomic.Int32
		server := httptest.NewServer(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/non-empty-bucket" {
					unexpectedRequests.Add(1)
					w.WriteHeader(http.StatusTeapot)
					return
				}

				switch {
				case r.Method == http.MethodHead && r.URL.RawQuery == "":
					observeRequests.Add(1)
					w.WriteHeader(http.StatusOK)
				case r.Method == http.MethodDelete && r.URL.RawQuery == "":
					bucketDeleteRequests.Add(1)
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(http.StatusConflict)
					_, _ = w.Write([]byte(
						`<?xml version="1.0" encoding="UTF-8"?><Error><Code>BucketNotEmpty</Code><Message>The bucket you tried to delete is not empty</Message></Error>`,
					))
				case r.Method == http.MethodGet && r.URL.Query().Has("versions"):
					versionListRequests.Add(1)
					w.WriteHeader(http.StatusTeapot)
				case r.Method == http.MethodPost && r.URL.Query().Has("delete"):
					objectDeleteRequests.Add(1)
					w.WriteHeader(http.StatusTeapot)
				default:
					unexpectedRequests.Add(1)
					w.WriteHeader(http.StatusTeapot)
				}
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
				Endpoint: "https://obs.test",
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
		bucket := testBucket("non-empty-bucket", obsv1alpha1.BucketSpec{
			ProviderConfigRef: corev1.LocalObjectReference{Name: "provider"},
		})
		now := metav1.Now()
		bucket.DeletionTimestamp = &now
		bucket.Finalizers = []string{bucketFinalizer}

		k8sClient := testClient(scheme, providerConfig, secret, bucket)
		resolver := provider.NewProviderResolver(
			k8sClient,
			provider.NewCache(),
			provider.WithOBSClientFactory(func(
				credentials provider.Credentials,
				_ string,
				region string,
			) (*obs.ObsClient, error) {
				return obs.New(
					credentials.AccessKeyID,
					credentials.SecretAccessKey,
					server.URL,
					obs.WithPathStyle(true),
					obs.WithRegion(region),
					obs.WithMaxRetryCount(0),
				)
			}),
		)
		reconciler := &BucketReconciler{Client: k8sClient, ProviderResolver: resolver}

		result, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      bucket.Name,
				Namespace: bucket.Namespace,
			},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(reconcile.Result{}))

		current := &obsv1alpha1.Bucket{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bucket), current)).To(Succeed())
		Expect(current.Finalizers).To(ContainElement(bucketFinalizer))
		condition := meta.FindStatusCondition(current.Status.Conditions, bucketReadyCondition)
		Expect(condition).NotTo(BeNil())
		Expect(condition.Status).To(Equal(metav1.ConditionFalse))
		Expect(condition.Reason).To(Equal("BucketNotEmpty"))
		Expect(condition.Message).To(Equal("The bucket you tried to delete is not empty"))
		Expect(observeRequests.Load()).To(Equal(int32(1)))
		Expect(bucketDeleteRequests.Load()).To(Equal(int32(1)))
		Expect(versionListRequests.Load()).To(Equal(int32(0)))
		Expect(objectDeleteRequests.Load()).To(Equal(int32(0)))
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
		Expect(isNonRetryableOBSRequestError(obsError("InvalidArgument", 400))).To(BeTrue())
		Expect(isNonRetryableOBSRequestError(obsError("AccessDenied", 403))).To(BeTrue())
		Expect(isNonRetryableOBSRequestError(obsError("NoSuchBucket", 404))).To(BeFalse())
		Expect(isNonRetryableOBSRequestError(obsError("RequestTimeout", 408))).To(BeFalse())
		Expect(isNonRetryableOBSRequestError(obsError("Conflict", 409))).To(BeFalse())
		Expect(isNonRetryableOBSRequestError(obsError("TooManyRequests", 429))).To(BeFalse())
		Expect(isNonRetryableOBSRequestError(obsError("InternalError", 500))).To(BeFalse())
		Expect(isBucketCode(obsError(obsNoSuchTagSetCode, 404), obsNoSuchTagSetCode)).To(BeTrue())
		Expect(isBucketNotFound(obsError("BucketNotEmpty", 409))).To(BeFalse())
	})

	It("formats bucket condition messages", func() {
		Expect(bucketReadyConditionMessage(obs.ObsError{
			BaseModel: obs.BaseModel{StatusCode: http.StatusBadRequest},
			Code:      "InvalidArgument",
			Message:   "The enterprise project id is invalid",
		})).To(Equal("The enterprise project id is invalid"))
		Expect(bucketReadyConditionMessage(errors.New("plain failure"))).To(Equal("plain failure"))
	})

	It("classifies user-correctable bucket reconcile errors", func() {
		transientErr := errors.New("transient failure")
		notFoundErr := apierrors.NewNotFound(schema.GroupResource{
			Group:    "obs.wilaris.de",
			Resource: "providerconfigs",
		}, "missing")
		wrappedMixedErr := fmt.Errorf(
			"outer: %w",
			errors.Join(errBucketAlreadyExists, transientErr),
		)

		Expect(isUserCorrectableBucketError(provider.ErrProviderConfigNotFound)).To(BeTrue())
		Expect(isUserCorrectableBucketError(provider.ErrCredentialsSecretNotFound)).To(BeTrue())
		Expect(isUserCorrectableBucketError(provider.ErrMissingCredentials)).To(BeTrue())
		Expect(isUserCorrectableBucketError(provider.ErrInvalidEndpoint)).To(BeTrue())
		Expect(isUserCorrectableBucketError(errBucketAlreadyExists)).To(BeTrue())
		Expect(isUserCorrectableBucketError(obsError("InvalidArgument", 400))).To(BeTrue())
		Expect(isUserCorrectableBucketError(obsError("AccessDenied", 403))).To(BeTrue())
		Expect(isUserCorrectableBucketError(fmt.Errorf("wrapped: %w", errBucketAlreadyExists))).
			To(BeTrue())
		Expect(isUserCorrectableBucketError(notFoundErr)).To(BeFalse())
		Expect(isUserCorrectableBucketError(obsError("InternalError", 500))).To(BeFalse())
		Expect(isUserCorrectableBucketError(obsError("TooManyRequests", 429))).To(BeFalse())
		Expect(isUserCorrectableBucketError(transientErr)).To(BeFalse())
		Expect(isUserCorrectableBucketError(errors.Join(errBucketAlreadyExists, transientErr))).
			To(BeFalse())
		Expect(isUserCorrectableBucketError(wrappedMixedErr)).To(BeFalse())
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
