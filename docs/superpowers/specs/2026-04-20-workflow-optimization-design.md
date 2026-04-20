# Workflow Optimization: Eliminate Redundant Wash Binary Compilation

## Problem

Push to main triggers wash compilation **3 times** across 2 workflows:

1. `wash.yml` → `check` job → `cargo build` (debug)
2. `wash.yml` → `canary` job → Docker build → `cargo build --release`
3. `build-wash-image.yml` → Docker build → `cargo build --release`

Tag push adds 5 more compilations in the `release` job.

## Solution

Compile wash binary **once** in the `check` job, then share the artifact everywhere.

## Design

### 1. wash.yml `check` job: build release + upload artifact

Change `cargo build` to `cargo build --release`, upload `target/release/wash` as artifact `wash-x86_64-unknown-linux-musl`.

### 2. docker-build-push.yml: accept pre-built binary

Add optional input `binary-artifact`. When provided:
- Download the artifact before Docker build
- Pass binary via `--build-context binary=<path>`
- Dockerfile uses a "skip build" stage when binary context is present

### 3. Dockerfile: add pre-built binary mode

```dockerfile
# New stage: use pre-built binary
FROM scratch AS prebuilt
ARG BINARY_CONTEXT=
# When BINARY_CONTEXT is set, copy from it; otherwise build from source

FROM ${BINARY_CONTEXT:+prebuilt} AS final-binary
# ... or the existing builder stage when no BINARY_CONTEXT
```

Simplified approach: two Dockerfile modes controlled by build-arg.

### 4. Delete build-wash-image.yml

Its functionality (push main → build Docker image with tag `custom-v1-alpha`) merges into `wash.yml` canary job.

### 5. build-http-api-distributed.yml → job within wash.yml

Convert from `workflow_run` trigger to a `needs: check` job in wash.yml. Eliminates cross-workflow artifact permission issues.

## Workflow After Changes

```
wash.yml (push main):
  check job:
    cargo build --release → upload artifact
  lint job: (unchanged)
  canary job:
    needs: check → download artifact → Docker build (skip compile) → push
  build-http-api job:
    needs: check → download artifact → wash build --skip-fetch → push wasm

wash.yml (tag push):
  check job: (same)
  lint job: (same)
  release job: cross-platform builds (unchanged, needed for native binaries)
  docker-release job:
    needs: check → download artifact → Docker build (skip compile) → push
  upload-release-assets: (unchanged)
```

## Files Changed

| File | Action |
|------|--------|
| `wash.yml` | check uploads artifact; canary/docker-release use artifact; add build-http-api job |
| `docker-build-push.yml` | Add `binary-artifact` input; download and pass to Docker |
| `Dockerfile` | Add pre-built binary skip mode |
| `build-wash-image.yml` | Delete |
| `build-http-api-distributed.yml` | Delete |
