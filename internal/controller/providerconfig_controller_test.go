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
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/opentelekomcloud/gophertelekomcloud/openstack/obs"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
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

var _ = Describe("ProviderConfig Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		providerconfig := &obsv1alpha1.ProviderConfig{}

		BeforeEach(func() {
			By("creating the credentials Secret")
			secret := &corev1.Secret{}
			secretName := types.NamespacedName{
				Name:      "otc-credentials",
				Namespace: "default",
			}
			err := k8sClient.Get(ctx, secretName, secret)
			if err != nil && errors.IsNotFound(err) {
				secret = &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "otc-credentials",
						Namespace: "default",
					},
					Data: map[string][]byte{
						provider.AccessKeyIDSecretKey:     []byte("access-key-id"),
						provider.SecretAccessKeySecretKey: []byte("secret-access-key"),
					},
				}
				Expect(k8sClient.Create(ctx, secret)).To(Succeed())
			}

			By("creating the custom resource for the Kind ProviderConfig")
			err = k8sClient.Get(ctx, typeNamespacedName, providerconfig)
			if err != nil && errors.IsNotFound(err) {
				resource := &obsv1alpha1.ProviderConfig{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: obsv1alpha1.ProviderConfigSpec{
						Region: "eu-de",
						CredentialsSecretRef: corev1.LocalObjectReference{
							Name: "otc-credentials",
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &obsv1alpha1.ProviderConfig{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance ProviderConfig")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())

			By("Cleanup the credentials Secret")
			secret := &corev1.Secret{}
			err = k8sClient.Get(
				ctx,
				types.NamespacedName{Name: "otc-credentials", Namespace: "default"},
				secret,
			)
			if err == nil {
				Expect(k8sClient.Delete(ctx, secret)).To(Succeed())
			}
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			cache := provider.NewCache()
			resolver, server := testProviderConfigResolver(k8sClient, cache, http.StatusOK)
			DeferCleanup(server.Close)

			controllerReconciler := &ProviderConfigReconciler{
				Client:           k8sClient,
				Scheme:           k8sClient.Scheme(),
				ProviderResolver: resolver,
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying the ProviderConfig Ready condition")
			Eventually(func(g Gomega) {
				resource := &obsv1alpha1.ProviderConfig{}
				g.Expect(k8sClient.Get(ctx, typeNamespacedName, resource)).To(Succeed())
				condition := meta.FindStatusCondition(resource.Status.Conditions, "Ready")
				g.Expect(condition).NotTo(BeNil())
				g.Expect(condition.Status).To(Equal(metav1.ConditionTrue))
				g.Expect(condition.Reason).To(Equal("ClientValidated"))
				g.Expect(resource.Status.ObservedGeneration).To(Equal(resource.Generation))
				g.Expect(resource.Status.LastValidationTime).NotTo(BeNil())
			}).Should(Succeed())
			Expect(cache.Len()).To(Equal(1))
		})

		It("should set Ready false when the credentials Secret is missing", func() {
			cache := provider.NewCache()
			resolver, server := testProviderConfigResolver(k8sClient, cache, http.StatusOK)
			DeferCleanup(server.Close)

			By("Reconciling the ProviderConfig to warm the cache")
			controllerReconciler := &ProviderConfigReconciler{
				Client:           k8sClient,
				Scheme:           k8sClient.Scheme(),
				ProviderResolver: resolver,
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(cache.Len()).To(Equal(1))

			By("Deleting the credentials Secret")
			secret := &corev1.Secret{}
			Expect(
				k8sClient.Get(
					ctx,
					types.NamespacedName{Name: "otc-credentials", Namespace: "default"},
					secret,
				),
			).To(Succeed())
			Expect(k8sClient.Delete(ctx, secret)).To(Succeed())

			By("Reconciling the ProviderConfig")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(cache.Len()).To(Equal(0))

			By("Verifying the ProviderConfig Ready condition")
			Eventually(func(g Gomega) {
				resource := &obsv1alpha1.ProviderConfig{}
				g.Expect(k8sClient.Get(ctx, typeNamespacedName, resource)).To(Succeed())
				condition := meta.FindStatusCondition(resource.Status.Conditions, "Ready")
				g.Expect(condition).NotTo(BeNil())
				g.Expect(condition.Status).To(Equal(metav1.ConditionFalse))
				g.Expect(condition.Reason).To(Equal("CredentialsSecretNotFound"))
				g.Expect(resource.Status.ObservedGeneration).To(Equal(resource.Generation))
				g.Expect(resource.Status.LastValidationTime).NotTo(BeNil())
			}).Should(Succeed())
		})

		It("should set Ready false when OBS rejects the credentials", func() {
			cache := provider.NewCache()
			resolver, server := testProviderConfigResolver(
				k8sClient,
				cache,
				http.StatusForbidden,
			)
			DeferCleanup(server.Close)

			controllerReconciler := &ProviderConfigReconciler{
				Client:           k8sClient,
				Scheme:           k8sClient.Scheme(),
				ProviderResolver: resolver,
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(cache.Len()).To(Equal(0))

			Eventually(func(g Gomega) {
				resource := &obsv1alpha1.ProviderConfig{}
				g.Expect(k8sClient.Get(ctx, typeNamespacedName, resource)).To(Succeed())
				condition := meta.FindStatusCondition(resource.Status.Conditions, "Ready")
				g.Expect(condition).NotTo(BeNil())
				g.Expect(condition.Status).To(Equal(metav1.ConditionFalse))
				g.Expect(condition.Reason).To(Equal("ClientValidationFailed"))
				g.Expect(condition.Message).To(ContainSubstring("provider validation failed"))
				g.Expect(resource.Status.ObservedGeneration).To(Equal(resource.Generation))
				g.Expect(resource.Status.LastValidationTime).NotTo(BeNil())
			}).Should(Succeed())
		})
	})

	Context("When mapping credentials Secret events", func() {
		It("should reject plaintext OBS endpoint overrides", func() {
			endpoint, err := provider.ResolveEndpoint("eu-de", "obs.eu-de.otc.t-systems.com")
			Expect(err).NotTo(HaveOccurred())
			Expect(endpoint).To(Equal("https://obs.eu-de.otc.t-systems.com"))

			_, err = provider.ResolveEndpoint("eu-de", "http://obs.eu-de.otc.t-systems.com")
			Expect(err).To(MatchError(ContainSubstring("endpoint scheme must be https")))
		})

		It("should enqueue same-namespace ProviderConfigs that reference the Secret", func() {
			ctx := context.Background()
			scheme := runtime.NewScheme()
			Expect(corev1.AddToScheme(scheme)).To(Succeed())
			Expect(obsv1alpha1.AddToScheme(scheme)).To(Succeed())

			matchingProviderConfig := &obsv1alpha1.ProviderConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "matching",
					Namespace: "default",
				},
				Spec: obsv1alpha1.ProviderConfigSpec{
					Region: "eu-de",
					CredentialsSecretRef: corev1.LocalObjectReference{
						Name: "otc-credentials",
					},
				},
			}
			otherSecretProviderConfig := &obsv1alpha1.ProviderConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "other-secret",
					Namespace: "default",
				},
				Spec: obsv1alpha1.ProviderConfigSpec{
					Region: "eu-de",
					CredentialsSecretRef: corev1.LocalObjectReference{
						Name: "other-credentials",
					},
				},
			}
			otherNamespaceProviderConfig := &obsv1alpha1.ProviderConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "other-namespace",
					Namespace: "other",
				},
				Spec: obsv1alpha1.ProviderConfigSpec{
					Region: "eu-de",
					CredentialsSecretRef: corev1.LocalObjectReference{
						Name: "otc-credentials",
					},
				},
			}

			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithIndex(
					&obsv1alpha1.ProviderConfig{},
					providerConfigCredentialsSecretIndex,
					providerConfigCredentialsSecretRefIndexer,
				).
				WithObjects(
					matchingProviderConfig,
					otherSecretProviderConfig,
					otherNamespaceProviderConfig,
				).
				Build()
			controllerReconciler := &ProviderConfigReconciler{Client: k8sClient}

			requests := controllerReconciler.providerConfigsForSecret(
				ctx,
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "otc-credentials",
						Namespace: "default",
					},
				},
			)
			Expect(requests).To(ConsistOf(reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      "matching",
					Namespace: "default",
				},
			}))

			requests = controllerReconciler.providerConfigsForSecret(
				ctx,
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "unknown",
						Namespace: "default",
					},
				},
			)
			Expect(requests).To(BeEmpty())
		})
	})
})

func testProviderConfigResolver(
	k8sClient client.Client,
	cache *provider.Cache,
	validationStatus int,
) (*provider.ProviderResolver, *httptest.Server) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/" {
			w.WriteHeader(http.StatusTeapot)
			return
		}

		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(validationStatus)
		if validationStatus >= http.StatusBadRequest {
			_, _ = w.Write(
				[]byte(`<Error><Code>AccessDenied</Code><Message>access denied</Message></Error>`),
			)
			return
		}

		_, _ = w.Write([]byte(
			`<?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult><Owner><ID>owner</ID></Owner><Buckets></Buckets></ListAllMyBucketsResult>`,
		))
	}))

	resolver := provider.NewProviderResolver(
		k8sClient,
		cache,
		provider.WithOBSClientFactory(func(
			credentials provider.Credentials,
			_ string,
			region string,
		) (*obs.ObsClient, error) {
			return obs.New(
				credentials.AccessKeyID,
				credentials.SecretAccessKey,
				server.URL,
				obs.WithRegion(region),
				obs.WithMaxRetryCount(0),
				obs.WithSslVerify(false),
			)
		}),
	)

	return resolver, server
}
