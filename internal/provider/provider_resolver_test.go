package provider

import (
	"context"
	"errors"
	"testing"

	"github.com/opentelekomcloud/gophertelekomcloud/openstack/obs"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	obsv1alpha1 "go.wilaris.de/obs-operator/api/v1alpha1"
)

func TestResolveEndpoint(t *testing.T) {
	tests := map[string]struct {
		region      string
		endpoint    string
		want        string
		wantErrIs   error
		description string
	}{
		"default endpoint": {
			region: "eu-de",
			want:   "https://obs.eu-de.otc.t-systems.com",
		},
		"explicit endpoint without scheme": {
			endpoint: "obs.eu-de.otc.t-systems.com",
			want:     "https://obs.eu-de.otc.t-systems.com",
		},
		"explicit endpoint trims trailing slash": {
			endpoint: "https://obs.eu-de.otc.t-systems.com/",
			want:     "https://obs.eu-de.otc.t-systems.com",
		},
		"reject endpoint path": {
			endpoint:  "https://obs.eu-de.otc.t-systems.com/api",
			wantErrIs: ErrInvalidEndpoint,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := ResolveEndpoint(tt.region, tt.endpoint)
			if tt.wantErrIs != nil {
				if !errors.Is(err, tt.wantErrIs) {
					t.Fatalf("expected error %v, got %v", tt.wantErrIs, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("expected endpoint %q, got %q", tt.want, got)
			}
		})
	}
}

func TestCredentialsFromSecret(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "credentials",
		},
		Data: map[string][]byte{
			AccessKeyIDSecretKey:     []byte(" access "),
			SecretAccessKeySecretKey: []byte(" secret "),
			SecurityTokenSecretKey:   []byte(" token "),
		},
	}

	credentials, err := CredentialsFromSecret(secret)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if credentials.AccessKeyID != "access" {
		t.Fatalf("expected trimmed access key, got %q", credentials.AccessKeyID)
	}
	if credentials.SecretAccessKey != "secret" {
		t.Fatalf("expected trimmed secret key, got %q", credentials.SecretAccessKey)
	}
	if credentials.SecurityToken != "token" {
		t.Fatalf("expected trimmed security token, got %q", credentials.SecurityToken)
	}

	delete(secret.Data, SecretAccessKeySecretKey)
	if _, err := CredentialsFromSecret(secret); !errors.Is(err, ErrMissingCredentials) {
		t.Fatalf("expected missing credentials error, got %v", err)
	}
}

func TestProviderResolverPreservesNotFoundErrorIdentity(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := obsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add OBS scheme: %v", err)
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&obsv1alpha1.ProviderConfig{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "provider",
			},
			Spec: obsv1alpha1.ProviderConfigSpec{
				Region: "eu-de",
				CredentialsSecretRef: corev1.LocalObjectReference{
					Name: "missing-credentials",
				},
			},
		}).
		Build()
	resolver := NewProviderResolver(k8sClient, NewCache())

	_, err := resolver.ResolveProviderConfig(
		ctx,
		types.NamespacedName{Namespace: "default", Name: "missing-provider"},
	)
	if !errors.Is(err, ErrProviderConfigNotFound) {
		t.Fatalf("expected ProviderConfig sentinel, got %v", err)
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected Kubernetes NotFound identity, got %v", err)
	}

	_, err = resolver.ResolveProviderConfig(
		ctx,
		types.NamespacedName{Namespace: "default", Name: "provider"},
	)
	if !errors.Is(err, ErrCredentialsSecretNotFound) {
		t.Fatalf("expected credentials Secret sentinel, got %v", err)
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected Kubernetes NotFound identity, got %v", err)
	}
}

func TestProviderResolverCachesUntilInputsChange(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := obsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add OBS scheme: %v", err)
	}

	providerConfig := &obsv1alpha1.ProviderConfig{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "default",
			Name:            "provider",
			UID:             "provider-uid",
			ResourceVersion: "1",
		},
		Spec: obsv1alpha1.ProviderConfigSpec{
			Region: "eu-de",
			CredentialsSecretRef: corev1.LocalObjectReference{
				Name: "credentials",
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "default",
			Name:            "credentials",
			UID:             "secret-uid",
			ResourceVersion: "1",
		},
		Data: map[string][]byte{
			AccessKeyIDSecretKey:     []byte("access"),
			SecretAccessKeySecretKey: []byte("secret"),
		},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(providerConfig, secret).
		Build()

	builds := 0
	closes := 0
	cache := NewCache()
	cache.closeClient = func(*obs.ObsClient) {
		closes++
	}
	resolver := NewProviderResolver(
		k8sClient,
		cache,
		WithOBSClientFactory(func(Credentials, string, string) (*obs.ObsClient, error) {
			builds++
			return &obs.ObsClient{}, nil
		}),
	)

	first, err := resolver.ResolveProviderConfig(
		ctx,
		types.NamespacedName{Namespace: "default", Name: "provider"},
	)
	if err != nil {
		t.Fatalf("resolve provider: %v", err)
	}
	if first.FromCache {
		t.Fatal("first resolution should not be served from cache")
	}
	if first.ProviderRevision == "" {
		t.Fatal("first resolution should return a provider revision")
	}
	if builds != 1 {
		t.Fatalf("expected one client build, got %d", builds)
	}

	second, err := resolver.ResolveProviderConfig(
		ctx,
		types.NamespacedName{Namespace: "default", Name: "provider"},
	)
	if err != nil {
		t.Fatalf("resolve provider from cache: %v", err)
	}
	if !second.FromCache {
		t.Fatal("second resolution should be served from cache")
	}
	if builds != 1 {
		t.Fatalf("expected cached client to avoid rebuild, got %d builds", builds)
	}
	if cache.Len() != 1 {
		t.Fatalf("expected one cached client, got %d entries", cache.Len())
	}

	providerConfig.Labels = map[string]string{"metadata-only": "true"}
	if err := k8sClient.Update(ctx, providerConfig); err != nil {
		t.Fatalf("update provider metadata: %v", err)
	}

	afterProviderMetadata, err := resolver.ResolveProviderConfig(
		ctx,
		types.NamespacedName{Namespace: "default", Name: "provider"},
	)
	if err != nil {
		t.Fatalf("resolve provider after metadata change: %v", err)
	}
	if !afterProviderMetadata.FromCache {
		t.Fatal("metadata-only ProviderConfig change should use cached client")
	}
	if afterProviderMetadata.ProviderRevision != first.ProviderRevision {
		t.Fatal("metadata-only ProviderConfig change should not change provider revision")
	}
	if builds != 1 {
		t.Fatalf(
			"expected metadata-only ProviderConfig change to avoid rebuild, got %d builds",
			builds,
		)
	}

	secret.Labels = map[string]string{"metadata-only": "true"}
	if err := k8sClient.Update(ctx, secret); err != nil {
		t.Fatalf("update secret metadata: %v", err)
	}

	afterSecretMetadata, err := resolver.ResolveProviderConfig(
		ctx,
		types.NamespacedName{Namespace: "default", Name: "provider"},
	)
	if err != nil {
		t.Fatalf("resolve provider after secret metadata change: %v", err)
	}
	if !afterSecretMetadata.FromCache {
		t.Fatal("metadata-only Secret change should use cached client")
	}
	if afterSecretMetadata.ProviderRevision == first.ProviderRevision {
		t.Fatal("metadata-only Secret resource version change should change provider revision")
	}
	if builds != 1 {
		t.Fatalf("expected metadata-only Secret change to avoid rebuild, got %d builds", builds)
	}

	latestSecret := &corev1.Secret{}
	if err := k8sClient.Get(
		ctx,
		types.NamespacedName{Namespace: "default", Name: "credentials"},
		latestSecret,
	); err != nil {
		t.Fatalf("get latest secret: %v", err)
	}
	latestSecret.Data[SecurityTokenSecretKey] = []byte("rotated")
	if err := k8sClient.Update(ctx, latestSecret); err != nil {
		t.Fatalf("update secret: %v", err)
	}

	third, err := resolver.ResolveProviderConfig(
		ctx,
		types.NamespacedName{Namespace: "default", Name: "provider"},
	)
	if err != nil {
		t.Fatalf("resolve provider after secret change: %v", err)
	}
	if third.FromCache {
		t.Fatal("resolution after secret change should not use stale cached client")
	}
	if builds != 2 {
		t.Fatalf("expected client rebuild after secret change, got %d builds", builds)
	}
	if cache.Len() != 1 {
		t.Fatalf("expected replacement to keep one cached client, got %d entries", cache.Len())
	}
	if closes != 1 {
		t.Fatalf("expected old client to close after replacement, got %d closes", closes)
	}

	resolver.InvalidateProvider("default", "provider")
	if cache.Len() != 0 {
		t.Fatalf("expected cache to be empty after invalidation, got %d entries", cache.Len())
	}
	if closes != 2 {
		t.Fatalf("expected cached client to close after invalidation, got %d closes", closes)
	}
}
