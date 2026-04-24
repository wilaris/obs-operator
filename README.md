# obs-operator

`obs-operator` is a small Kubernetes operator for declarative OBS bucket
management.

It exposes Object Storage Service (OBS) buckets on T Cloud Public through a
simple Kubernetes API. Teams describe buckets as custom resources, the operator
reconciles the requested OBS state and readiness is reported back through normal
Kubernetes status conditions.

The project intentionally keeps the model small: one resource describes how to
connect to OBS and one resource describes a bucket.

## Why this exists

We needed a practical way to provide customers with self-service resources such
as S3/OBS buckets while keeping responsibility boundaries clear: the platform
owns provider credentials and infrastructure integration, services consume
Kubernetes APIs and customer workloads request only the resources they need.

Direct cloud console access or broad OBS permissions would have pushed too much
provider-specific responsibility into the customer layer. At the same time,
manually provisioning every bucket in the infrastructure layer would have made
simple consumption workflows unnecessarily slow.

This operator is the approach we came up with: a small Kubernetes-native
interface that lets buckets be requested and reconciled where the consuming
workloads already live, without turning the project into a broad cloud control
plane.

That same pattern may be useful if you want to:

- keep bucket requests declarative and reviewable
- work with GitOps and existing Kubernetes workflows
- avoid direct provider credential access for users or tenants
- make lifecycle behavior predictable and visible in Kubernetes
- keep the operator easy to understand and operate

`obs-operator` focuses on the common bucket lifecycle needed for those workflows
and deliberately avoids becoming a general-purpose cloud control plane.

## Resource model

The operator defines two namespaced custom resources in
`obs.wilaris.de/v1alpha1`.

| Resource | Purpose |
| --- | --- |
| `ProviderConfig` | Describes the OBS region, optional endpoint and credentials Secret used by the controller. |
| `Bucket` | Describes one OBS bucket that should be created, reconciled, observed and deleted. |

`ProviderConfig`, its credentials Secret and all `Bucket` resources that use it
must live in the same namespace.

### ProviderConfig

`ProviderConfig` is the platform-side part of the contract. It tells the
operator which T Cloud Public OBS region to use and which Kubernetes Secret
contains the credentials for that namespace.

```yaml
apiVersion: obs.wilaris.de/v1alpha1
kind: ProviderConfig
metadata:
  name: otc-eu-de
  namespace: obs-demo
spec:
  region: eu-de
  credentialsSecretRef:
    name: obs-credentials
  # Optional. If omitted, the controller derives the OBS endpoint from region.
  # endpoint: https://obs.eu-de.otc.t-systems.com
```

The referenced Secret contains `accessKeyID` and `secretAccessKey` and can
also include `securityToken` for temporary credentials. The operator validates
the configuration by creating an OBS client and listing buckets, then reports
the result through a Kubernetes `Ready` condition.

### Bucket

`Bucket` is the resource consumers normally care about. Create one, point it at
a `ProviderConfig` and the operator creates the OBS bucket and reconciles its
supported settings whenever the Kubernetes resource is reconciled. The
Kubernetes object name is the OBS bucket name.

```yaml
apiVersion: obs.wilaris.de/v1alpha1
kind: Bucket
metadata:
  name: backups-2026
  namespace: obs-demo
spec:
  providerConfigRef:
    name: otc-eu-de
  storageClass: STANDARD
  acl: private
  versioning: true
  forceDestroy: false
  tags:
    app: backups
    owner: platform
```

The spec intentionally covers the bucket settings that tend to matter for
day-to-day consumption:

- storage class: `STANDARD`, `WARM` or `COLD`
- canned ACL: `private`, `public-read`, `public-read-write` or `log-delivery-write`
- versioning
- bucket tags
- access logging
- default server-side encryption using OBS KMS
- optional Parallel File System creation with `parallelFS`
- deletion behavior with `forceDestroy`

## Design choices

The operator is deliberately conservative about ownership. It creates and
manages buckets that it owns through Kubernetes resources, but it does not try to
adopt arbitrary existing buckets.

Some fields are fixed after creation because changing them would blur ownership
or conflict with OBS behavior.

Deleting a `Bucket` resource deletes the owned OBS bucket as well. Empty buckets
are removed directly; non-empty buckets require `forceDestroy: true` when the
operator should remove object versions and delete markers before deletion.

If an owned bucket is missing when the controller reconciles a still-existing
`Bucket` resource, the operator creates it again. The controller currently
reconciles from Kubernetes events for `Bucket` and `ProviderConfig` resources;
it does not poll OBS on a fixed interval. Readiness and failures are always
reported through `.status.conditions`.
