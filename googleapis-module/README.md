# Tiny Systems Google APIs Module

Google Cloud and Workspace integration components covering OAuth, Calendar, and Firestore.

## Components

### OAuth

| Component | Description |
|-----------|-------------|
| OAuth URL Get | Generate Google OAuth 2.0 authorization URLs |
| OAuth Code Exchange | Exchange authorization codes for access tokens |

### Google API

| Component | Description |
|-----------|-------------|
| Google API Call | Universal Google API client for any REST endpoint |

### Calendar

| Component | Description |
|-----------|-------------|
| Calendar List | List available Google Calendars |
| Calendar Events Get | Fetch events from a calendar |
| Calendar Event Respond | Accept, decline, or tentatively accept an event |
| Calendar Watch | Subscribe to calendar change notifications |
| Calendar Watch Stop | Stop an active calendar watch subscription |

### Firestore

| Component | Description |
|-----------|-------------|
| Firestore Get Docs | Query documents from a Firestore collection |
| Firestore Create Doc | Create a new Firestore document |
| Firestore Update Doc | Replace a Firestore document |
| Firestore Update Doc Field | Update individual fields in a Firestore document |
| Firestore Delete Doc | Delete a Firestore document |
| Firestore Listen Collection | Real-time listener for Firestore collection changes |

## Installation

```shell
helm repo add tinysystems https://tiny-systems.github.io/module/
helm install googleapis-module tinysystems/tinysystems-operator \
  --set controllerManager.manager.image.repository=ghcr.io/tiny-systems/googleapis-module
```

## Run locally

```shell
go run cmd/main.go run --name=googleapis-module --namespace=tinysystems --version=1.0.0
```

## Part of Tiny Systems

This module is part of the [Tiny Systems](https://github.com/tiny-systems) platform -- a visual flow-based automation engine running on Kubernetes.

## License

This module's source code is MIT-licensed. It depends on the [Tiny Systems Module SDK](https://github.com/tiny-systems/module) (BSL 1.1). See [LICENSE](LICENSE) for details.
