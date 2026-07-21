# Tiny Systems module index

A static, **Helm-style** module repo — `index.yaml` lists installable modules.
There is no server: it's just files, served over GitHub Pages, mirrorable
anywhere. The platform is one consumer of this, not its owner.

## Use it

```sh
tiny repo add tinysystems https://tiny-systems.github.io/modules/index.yaml
tiny repo update
tiny install http-module
```

## Add / update a module

Drop a `<module>/module.yaml` (and an optional sibling `values.yaml` overlay for
rbac/ingress/storage), then regenerate:

```sh
tiny repo index . -o index.yaml
```

Each module is `{ image, values overlay }` for the shared operator chart —
image on GHCR, source on GitHub. See the design at
`tiny-systems/tiny:docs/design/module-distribution-v2.md`.
