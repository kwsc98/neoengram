# NeoEngram fuser patch

This directory is based on the crates.io `fuser` 0.16.0 package.

- Upstream repository: `https://github.com/cberner/fuser`
- Upstream release commit: `d39b15200d2509db6bf712346d2cceade3a3f2fd`
- crates.io checksum: `0bb29a3ae32279fe3e79a958fe01899f5fb23eadccee919cf88e145b54ed9367`

## Why this patch exists

macFUSE 5.3.3 supports its FSKit backend through the message-oriented `MFMount` and `MFChannel`
API. It does not expose a FUSE device file descriptor for that backend. Upstream fuser 0.16 uses
`fuse_mount_compat25()` on macOS and then performs `read(2)` and `writev(2)` on the returned file
descriptor. macFUSE 5.3.3 intentionally rejects that compatibility entry point, so the upstream
transport cannot mount an FSKit filesystem.

The NeoEngram patch keeps fuser's request parser, `Filesystem` trait, dispatcher, replies, and all
non-macOS transports. On macOS it:

- creates an `MFChannel` and runs `MFMount` on a dedicated thread;
- receives complete messages with `MFChannelCopyNextMessage`, flattens their body buffers into
  fuser's existing aligned request buffer, and releases each `MFMessage` after the copy;
- sends existing vectored replies with `MFChannelSendMessage`;
- closes each channel once and releases it only after all channel users and the mount worker are
  finished;
- removes libfuse-only control options before passing mount options to `MFMount`;
- starts the fuser request loop before waiting for mount completion, because FSKit mount completion
  can depend on userspace answering `FUSE_INIT`;
- waits at most 30 seconds for `MFMount` in `spawn_mount2()`. Failure closes the channel and joins
  the request-loop thread before returning the mount error. A framework worker that does not react
  to channel close is detached with its own retained channel reference instead of defeating the
  timeout with an unbounded join;
- forwards fuser's `quiet` custom mount option to `MFMount` so unattended retries do not open a
  user-facing approval dialog.

An FSKit channel has no meaningful file descriptor. Consequently, the `AsFd` implementations for
`Channel` and `Session` are unavailable on macOS in this patched build. `Session::from_fd` remains
available for callers that explicitly manage a device-backed channel.

## Scope and replacement

This is a narrowly scoped macOS transport compatibility patch. Linux and other Unix mount paths are
unchanged. The workspace selects it through `[patch.crates-io]` in the root `Cargo.toml`.

Remove this patch when an upstream fuser release supports macFUSE's non-file-descriptor `MFChannel`
transport and provides equivalent mount-completion and cleanup behavior. Revalidate message body
ownership, concurrent reply sending, failed-mount wakeups, and unmount behavior before switching.
