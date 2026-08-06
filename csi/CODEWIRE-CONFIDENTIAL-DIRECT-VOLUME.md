# Codewire confidential direct-volume mode

This fork adds an opt-in Longhorn V1 CSI node path for Codewire confidential
workspaces. It is inactive unless the provisioned volume carries the exact
StorageClass parameter:

```yaml
parameters:
  codewireKataDirectVolume: "true"
```

All unmarked volumes keep the upstream Longhorn CSI path.

## Contract

The marked volume must be a filesystem-mode, `ReadWriteOncePod`, ext4 volume
with one Longhorn V1 replica, the block-device frontend, and no Longhorn host
encryption or migration. The PVC must explicitly use `volumeMode: Filesystem`
and carry a canonical environment UUID in:

```text
codewire.sh/confidential-storage-key-id
```

The external provisioner runs with `--extra-create-metadata=true`, allowing the
node plugin to locate that PVC without copying its annotation into Longhorn
volume parameters.

`NodeStageVolume` validates the attached raw endpoint and persists only
non-secret cleanup metadata. It does not inspect device contents, format,
resize, encrypt, or mount the endpoint on the host. `NodePublishVolume` invokes
the host's exact `/opt/kata/bin/kata-runtime` through the existing namespace
helper and registers this mount information:

```text
volume-type=block
fstype=ext4
io.codewire.storage.encryption=luks2
io.codewire.storage.source=auto
io.codewire.storage.key-uri=kbs:///default/codewire-workspace-luks/<environment-uuid>
io.codewire.storage.filesystem=ext4
io.codewire.storage.grow=true
```

No key bytes are present in CSI requests, lifecycle state, mount metadata,
command errors, or logs. Kata and the attested guest own LUKS2 detection,
initialization/reopen, ext4 mounting, and filesystem growth.

Unpublish and unstage remove Kata registration and lifecycle state
idempotently. Volume statistics are requested from the guest through Kata.
Online `NodeExpandVolume` returns the stable restart-required
`FailedPrecondition` error; normal detached controller expansion remains
unchanged and the guest grows ext4 after pod recreation.

## Runtime prerequisite

The CSI plugin must run on a node whose host root contains the matching
`/opt/kata/bin/kata-runtime`. The plugin uses `nsmounter --host-root` so the
runtime observes the host's direct-volume registry and shim sockets. If this
prerequisite or any direct-volume validation fails, the operation fails closed;
it never falls back to the standard host-mount path.
