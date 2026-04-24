package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/opentelekomcloud/gophertelekomcloud/openstack/obs"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	obsv1alpha1 "go.wilaris.de/obs-operator/api/v1alpha1"
)

const (
	// AccessKeyIDSecretKey is the Secret data key containing the OBS access key ID.
	AccessKeyIDSecretKey = "accessKeyID"
	// SecretAccessKeySecretKey is the Secret data key containing the OBS secret access key.
	SecretAccessKeySecretKey = "secretAccessKey"
	// SecurityTokenSecretKey is the optional Secret data key containing a temporary security token.
	SecurityTokenSecretKey = "securityToken"
)

var (
	// ErrMissingCredentials indicates that the credentials Secret is missing required keys.
	ErrMissingCredentials = errors.New("missing provider credentials")
	// ErrInvalidEndpoint indicates that the configured OBS endpoint is invalid.
	ErrInvalidEndpoint = errors.New("invalid OBS endpoint")
)

// OBSClientFactory creates an OBS client from resolved provider configuration.
type OBSClientFactory func(credentials Credentials, endpoint string, region string) (*obs.ObsClient, error)

// ProviderResolverOption configures a ProviderResolver.
type ProviderResolverOption func(*ProviderResolver)

// WithOBSClientFactory overrides OBS client construction.
func WithOBSClientFactory(factory OBSClientFactory) ProviderResolverOption {
	return func(r *ProviderResolver) {
		r.newOBSClient = factory
	}
}

// Credentials contains OBS AK/SK credentials resolved from a Secret.
type Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SecurityToken   string
}

// ResolvedClient contains a cached or newly built OBS client with provider metadata.
type ResolvedClient struct {
	OBS               *obs.ObsClient
	ProviderConfig    types.NamespacedName
	CredentialsSecret types.NamespacedName
	Region            string
	Endpoint          string
	FromCache         bool
}

type cacheKey struct {
	ProviderConfig    types.NamespacedName
	ProviderUID       string
	CredentialsSecret types.NamespacedName
	SecretUID         string
	CredentialsHash   string
	Region            string
	Endpoint          string
}

// ProviderResolver resolves ProviderConfig resources into OBS clients.
type ProviderResolver struct {
	client       client.Client
	cache        *Cache
	newOBSClient OBSClientFactory
}

// NewProviderResolver creates a resolver backed by a shared client cache.
func NewProviderResolver(
	k8sClient client.Client,
	cache *Cache,
	opts ...ProviderResolverOption,
) *ProviderResolver {
	resolver := &ProviderResolver{
		client:       k8sClient,
		cache:        cache,
		newOBSClient: newOBSClient,
	}

	for _, opt := range opts {
		opt(resolver)
	}

	return resolver
}

// ResolveProviderConfig fetches a ProviderConfig and resolves it into an OBS client.
func (r *ProviderResolver) ResolveProviderConfig(
	ctx context.Context,
	key types.NamespacedName,
) (*ResolvedClient, error) {
	providerConfig := &obsv1alpha1.ProviderConfig{}
	if err := r.client.Get(ctx, key, providerConfig); err != nil {
		return nil, err
	}

	return r.ResolveProviderConfigObject(ctx, providerConfig)
}

// ResolveProviderConfigObject resolves a loaded ProviderConfig into an OBS client.
func (r *ProviderResolver) ResolveProviderConfigObject(
	ctx context.Context,
	providerConfig *obsv1alpha1.ProviderConfig,
) (*ResolvedClient, error) {
	secretKey := types.NamespacedName{
		Namespace: providerConfig.Namespace,
		Name:      providerConfig.Spec.CredentialsSecretRef.Name,
	}
	secret := &corev1.Secret{}
	if err := r.client.Get(ctx, secretKey, secret); err != nil {
		return nil, err
	}

	credentials, err := CredentialsFromSecret(secret)
	if err != nil {
		return nil, err
	}

	region := strings.TrimSpace(providerConfig.Spec.Region)
	endpoint, err := ResolveEndpoint(region, providerConfig.Spec.Endpoint)
	if err != nil {
		return nil, err
	}

	key := cacheKey{
		ProviderConfig: types.NamespacedName{
			Namespace: providerConfig.Namespace,
			Name:      providerConfig.Name,
		},
		ProviderUID:       string(providerConfig.UID),
		CredentialsSecret: secretKey,
		SecretUID:         string(secret.UID),
		CredentialsHash:   credentials.Hash(),
		Region:            region,
		Endpoint:          endpoint,
	}

	if resolved, ok := r.cache.get(key); ok {
		cached := *resolved
		cached.FromCache = true
		return &cached, nil
	}

	obsClient, err := r.newOBSClient(credentials, endpoint, region)
	if err != nil {
		return nil, fmt.Errorf("build OBS client: %w", err)
	}

	return r.cache.set(key, obsClient), nil
}

// ResolveBucket resolves the ProviderConfig referenced by a Bucket.
func (r *ProviderResolver) ResolveBucket(
	ctx context.Context,
	bucket *obsv1alpha1.Bucket,
) (*ResolvedClient, error) {
	return r.ResolveProviderConfig(ctx, types.NamespacedName{
		Namespace: bucket.Namespace,
		Name:      bucket.Spec.ProviderConfigRef.Name,
	})
}

// InvalidateProvider removes cached clients for a ProviderConfig.
func (r *ProviderResolver) InvalidateProvider(namespace, name string) {
	if r == nil || r.cache == nil {
		return
	}

	r.cache.InvalidateProvider(namespace, name)
}

// CredentialsFromSecret parses OBS credentials from a Kubernetes Secret.
func CredentialsFromSecret(secret *corev1.Secret) (Credentials, error) {
	credentials := Credentials{
		AccessKeyID:     strings.TrimSpace(string(secret.Data[AccessKeyIDSecretKey])),
		SecretAccessKey: strings.TrimSpace(string(secret.Data[SecretAccessKeySecretKey])),
		SecurityToken:   strings.TrimSpace(string(secret.Data[SecurityTokenSecretKey])),
	}

	if credentials.AccessKeyID == "" {
		return Credentials{}, fmt.Errorf(
			"%w: secret %s/%s data key %q is required",
			ErrMissingCredentials,
			secret.Namespace,
			secret.Name,
			AccessKeyIDSecretKey,
		)
	}
	if credentials.SecretAccessKey == "" {
		return Credentials{}, fmt.Errorf(
			"%w: secret %s/%s data key %q is required",
			ErrMissingCredentials,
			secret.Namespace,
			secret.Name,
			SecretAccessKeySecretKey,
		)
	}

	return credentials, nil
}

// Hash returns a non-secret cache discriminator for the credential values.
func (c Credentials) Hash() string {
	hash := sha256.New()
	hash.Write([]byte(c.AccessKeyID))
	hash.Write([]byte{0})
	hash.Write([]byte(c.SecretAccessKey))
	hash.Write([]byte{0})
	hash.Write([]byte(c.SecurityToken))
	return hex.EncodeToString(hash.Sum(nil))
}

// ResolveEndpoint returns the endpoint used for OBS client construction.
func ResolveEndpoint(region, endpoint string) (string, error) {
	region = strings.TrimSpace(region)
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		endpoint = fmt.Sprintf("https://obs.%s.otc.t-systems.com", region)
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}

	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidEndpoint, err)
	}
	if parsed.Scheme != "https" {
		return "", fmt.Errorf("%w: endpoint scheme must be https", ErrInvalidEndpoint)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("%w: endpoint host is required", ErrInvalidEndpoint)
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return "", fmt.Errorf("%w: endpoint path must be empty", ErrInvalidEndpoint)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf(
			"%w: endpoint query and fragment are not supported",
			ErrInvalidEndpoint,
		)
	}

	parsed.Path = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimRight(parsed.String(), "/"), nil
}

func newOBSClient(credentials Credentials, endpoint string, region string) (*obs.ObsClient, error) {
	return obs.New(
		credentials.AccessKeyID,
		credentials.SecretAccessKey,
		endpoint,
		obs.WithSecurityToken(credentials.SecurityToken),
		obs.WithSignature(obs.SignatureObs),
		obs.WithRegion(region),
	)
}
