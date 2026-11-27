# DriftFolderSync

Real-time, two-way synchronisation between folders on Linux and Google Drive,
with several Google accounts side by side. Written in Rust and packaged as a
Flatpak.

> **DriftFolderSync is an independent, unofficial project.** It is not affiliated
> with, endorsed by or sponsored by Google LLC. "Google Drive", "Google Docs"
> and related names are trademarks of Google LLC and are used here only to
> describe what the software works with. DriftFolderSync uses only Google's
> public, documented Drive API v3. It ships **no** Google credentials: each
> user signs in through their **own** OAuth client (see
> [OAuth set-up](#oauth-set-up)).

## Contents

- [Features](#features)
- [Install](#install)
- [OAuth set-up](#oauth-set-up)
- [Quick start](#quick-start)
- [Migrating from an rclone cron job](#migrating-from-an-rclone-cron-job)
- [Sync frequency and batching](#sync-frequency-and-batching)
- [How it stays light on the system](#how-it-stays-light-on-the-system)
- [Safety](#safety)
- [Configuration reference](#configuration-reference)
- [Command reference](#command-reference)
- [Legal notes](#legal-notes)
- [Building and testing](#building-and-testing)

## Features

Compared with Drive for desktop (Windows/macOS):

| Feature | DriftFolderSync |
|---|---|
| Mirror files: local copies, kept in sync both ways | ✅ two-way, plus upload-only ("backup") and download-only modes |
| Several accounts at once | ✅ any number, each with its own folders |
| Sign-in through the browser | ✅ OAuth loopback + PKCE; tokens in the desktop keyring |
| Sync any local folder to any Drive folder ("sync other folders") | ✅ |
| Shared drives | ✅ `shared_drive = "Name"` |
| Google Docs/Sheets/Slides | ✅ `.gdoc`/`.gsheet`/… link files open in the browser; or Office export, or ignore |
| Real-time change detection | ✅ inotify locally; Drive changes feed remotely |
| Batching (sync every 1/5/30 min) | ✅ per folder, from the tray or the CLI |
| API rate limiting and back-off | ✅ token bucket per account; exponential back-off with jitter on 403/429/5xx |
| Pause / resume, sync now | ✅ per folder or all |
| Bandwidth limits | ✅ upload and download, KiB/s |
| Proxy | ✅ HTTP/HTTPS/SOCKS5, or the standard `*_proxy` variables |
| Notifications | ✅ conflicts, sign-in problems, full storage, held-back deletions |
| Tray icon with status, activity and storage use | ✅ StatusNotifierItem (KDE, GNOME with AppIndicator extension, Xfce, …) |
| Start at login | ✅ Background portal (Flatpak) or XDG autostart |
| Deleted files are recoverable | ✅ cloud deletions go to the Drive bin; local ones to your Trash |
| Large deletion warning | ✅ more than `delete_guard` deletions wait for confirmation |
| Selective sync | ✅ `include_folders` / `exclude_folders` and glob `exclude` patterns |
| Conflict resolution | ✅ keep both (default), prefer local, prefer cloud, or prefer newest |
| No "test users only" or 7-day sign-in limits from a shared app | ✅ you use your own OAuth client, so no shared app needs Google verification ([details](#oauth-set-up)) |
| Resumable uploads of large files, MD5 verification | ✅ |
| Renames and moves without re-uploading | ✅ detected by inode locally and by file ID remotely |
| Symbolic links | ✅ optional, compatible with `rclone --links` (`.rclonelink`) |
| Streaming (files on demand) | ❌ needs a FUSE file system, which a Flatpak sandbox cannot mount. All synced files are stored locally, as in mirror mode. |
| Google Photos backup | ❌ not implemented |

## Install

### Flatpak (recommended)

```sh
./flatpak/build.sh            # builds and installs for the current user
./flatpak/build.sh --bundle   # also writes driftfoldersync.flatpak, a single-file installer
```

To install the bundle on another machine: `flatpak install --user driftfoldersync.flatpak`.

The Flatpak has access to your home directory, so you can sync folders
anywhere under it. To sync a folder elsewhere, grant access first:

```sh
flatpak override --user --filesystem=/data/projects io.github.OnepointConsultingLtd.DriftFolderSync
```

A shell alias makes the CLI more convenient:

```sh
alias driftfoldersync='flatpak run io.github.OnepointConsultingLtd.DriftFolderSync'
```

### From source

```sh
cargo build --release
install -Dm755 target/release/driftfoldersync ~/.local/bin/driftfoldersync
```

## OAuth set-up

DriftFolderSync never ships a client ID. You create a free OAuth client in your own
Google Cloud project once (about 5 minutes) and can use it for all your
accounts. This keeps you within Google's terms: the app acts as *you*, with
*your* credentials, and nobody else's data passes through anyone else's
project.

1. Go to <https://console.cloud.google.com/>, create a project (any name,
   e.g. "My Drive sync").
2. **APIs & Services → Library**: enable the **Google Drive API**.
3. **APIs & Services → OAuth consent screen** (Google Auth Platform → Branding/Audience):
   - **Workspace account, and you are an admin:** choose **Internal**. Refresh
     tokens then never expire, and no verification is involved.
   - **Otherwise:** choose **External**. Under *Audience*, add every Google
     account you will sync with as a **test user**. In *Testing* status,
     Google expires the sign-in after 7 days. To avoid that, press **Publish
     app** to move it to *In production*. For personal use (under 100 users)
     you do not need to submit it for verification. At sign-in you will see
     "Google hasn't verified this app": choose *Advanced → Go to … (unsafe)*.
     That is your own app.
4. **APIs & Services → Credentials → Create credentials → OAuth client ID →
   Application type: Desktop app**. Copy the *Client ID* and *Client secret*.
   For desktop apps Google does not treat the secret as confidential. It
   still belongs in your settings file only, not in a public repository.

One External project can serve accounts in different organisations (for
example a work account and a client's account), provided each organisation's
Workspace admin allows third-party apps. If one of them blocks unconfigured
apps, ask its admin to trust your client ID, or create an Internal project in
that organisation.

DriftFolderSync requests the `https://www.googleapis.com/auth/drive` scope, which it
needs to see and mirror existing files.

## Quick start

```sh
# 1. Add an account (opens your browser to sign in)
driftfoldersync account add work --client-id 'XXXX.apps.googleusercontent.com' \
    --client-secret 'GOCSPX-…' --email me@example.com

# 2. Add a folder: local ~/Work ⇄ "My Drive/Work"
driftfoldersync folder add work-docs --account work --local ~/Work --remote /Work

# 3. Start the service (tray icon appears) and make it start at login
driftfoldersync daemon &
driftfoldersync autostart on

driftfoldersync status
```

You can also add folders from the tray: **Add a folder to sync → Using
account '…'** opens a folder picker and mirrors the folder to
`My Drive/<folder name>`.

## Migrating from an rclone cron job

DriftFolderSync is designed to take over trees that rclone already keeps in step,
**without re-uploading or re-downloading them**:

- `initial_match = "size-mtime"` (the default) treats a file present on both
  sides with the same size and a modification time within 2 s as identical.
  rclone preserves modification times, so no data is transferred. Files whose
  times differ are compared by MD5 before anything happens.
- `symlinks = "rclonelink"` reads and writes the `name.rclonelink` files that
  `rclone --links` creates.
- rclone exports Google Docs as `.docx`/`.xlsx`/`.pptx`. Those local copies
  are **left alone**, not uploaded as duplicates. The document also appears
  as a `.gdoc` link (or switch to `google_docs = "export"` to keep receiving
  Office exports).

Steps:

1. **Stop the cron job first**, so that two tools are not syncing the same
   folders: `sudo rm /etc/cron.hourly/rclone-sync` (or `chmod -x` it).
2. Run one last `rclone sync` in the direction you trust, so that both sides
   match.
3. Add accounts and folders that mirror your rclone remotes, e.g. for
   `rclone sync ~/Docs remote:/documents`:
   ```toml
   [[sync]]
   name = "docs"
   account = "work"
   local = "/home/me/Docs"
   remote = "/documents"
   symlinks = "rclonelink"
   ```
4. Start the daemon. The first run lists the cloud folder (a few minutes for
   about 100k files, within the API rate limits), then records the matching
   state locally. Watch it with `driftfoldersync status` or the tray.

## Sync frequency and batching

Each folder has a `sync_interval`:

- `realtime` (default): local changes are uploaded once a file has been quiet
  for `settle_time` (3 s). Under constant churn, changes are flushed at least
  every 30 s. The cloud is checked every `remote_poll_interval` (30 s) with a
  single cheap changes-feed request.
- `1m`, `5m`, `30m`, `1h`, or any duration: changes from **both** sides are
  collected and applied together at most once per interval. A file saved 500
  times in 5 minutes is uploaded once. Nothing is sent to the API between
  ticks.

Change it from the tray (**Sync frequency**) or with
`driftfoldersync interval <folder> 5m`.

## How it stays light on the system

- **inotify with one watch per directory**, never one per file: about 21k
  watches for a tree of 120k files, well under the usual limit of 1,048,576.
- Only **end-of-write** events (`CLOSE_WRITE`) are requested, not every
  `write()`, and events are coalesced into a set of dirty paths.
- **No hashing unless needed**: changes are detected by size and mtime. An
  MD5 is computed only when the mtime changed but the size did not, or on
  the first comparison with the cloud.
- The daemon runs at **nice 10 with low I/O priority**, configurable as `nice`.
- On kernel event-queue overflow, or if the watch limit is reached, it falls
  back to a **stat-only rescan**. A safety-net rescan also runs every
  `full_rescan_interval` (6 h by default).
- Remote changes come from Drive's **changes feed**: one request per poll,
  however large the drive.

## Safety

- Three-way comparison against the last synced state, so a deletion is only
  propagated when the item was really deleted on one side and unchanged on
  the other. An edited file always wins over a deletion.
- Cloud deletions go to the **Drive bin** (recoverable for 30 days). Local
  deletions go to your desktop **Trash**.
- More than `delete_guard` (250) deletions in one cycle are **held back**
  until you confirm them: `driftfoldersync confirm-deletes <folder>`, or the tray.
- A folder is only deleted as a whole when nothing inside it changed.
  Otherwise it is kept and its contents are handled one by one.
- **Conflicts** (both sides edited) default to *keep both*: your local copy
  is renamed `name (conflicted copy <host> <date>).ext` and uploaded too, and
  you get a notification.
- Downloads go to a temporary file, are **MD5-verified**, then atomically
  renamed into place. A download is abandoned if the local file changed in
  the meantime.
- Disk space is checked before each download.

## Configuration reference

The settings file is `~/.config/driftfoldersync/config.toml` (Flatpak:
`~/.var/app/io.github.OnepointConsultingLtd.DriftFolderSync/config/driftfoldersync/config.toml`).
Run `driftfoldersync config-path` to print it. Every option is documented in
[`config.example.toml`](config.example.toml). After editing, run
`driftfoldersync reload` or choose **Reload settings** from the tray.

## Command reference

```
driftfoldersync daemon [--no-tray]           run the service
driftfoldersync status [--json]              state of every folder
driftfoldersync pause|resume|sync-now [folder]
driftfoldersync interval <folder> <realtime|1m|5m|30m|…>
driftfoldersync confirm-deletes <folder>
driftfoldersync account add|login|logout|list
driftfoldersync folder add|remove|list|reset
driftfoldersync open <file.gdoc>             open a document link in the browser
driftfoldersync autostart on|off
driftfoldersync reload | quit | config-path
```

Logs: `~/.local/share/driftfoldersync/driftfoldersync.log` (Flatpak:
`~/.var/app/io.github.OnepointConsultingLtd.DriftFolderSync/data/driftfoldersync/driftfoldersync.log`).
Set `DRIFTFOLDERSYNC_LOG=debug` for more detail. On a headless machine without a
keyring, `DRIFTFOLDERSYNC_TOKEN_STORE=file` stores tokens in a file readable only by
you.

## Legal notes

- Licensed under the **GNU GPL v3 or later**. See [LICENSE](LICENSE).
- The software uses only the public Google Drive API v3 and Google's
  documented OAuth 2.0 flow for installed applications. It does not scrape,
  use undocumented endpoints, or bypass quotas.
- No Google client ID, secret, logo or other brand asset is included or
  distributed. The name and icon are original. Google product names appear
  only to describe compatibility (nominative use), alongside the disclaimer
  above.
- Each user supplies their own OAuth client and is bound by the Google APIs
  Terms of Service and the Google API Services User Data Policy for their own
  use. Because the project distributes no OAuth client, it is not an
  "application" that Google would need to verify, and it never handles other
  people's data.
- API usage limits are respected by design: client-side rate limiting, honouring
  `Retry-After`, and exponential back-off.
- This is not legal advice. If you plan to *redistribute* builds that embed a
  shared OAuth client, you would take on Google's app verification and
  policy obligations yourself.

## Building and testing

```sh
cargo test        # unit tests + end-to-end tests against an in-process fake Drive server
cargo build --release
```

The end-to-end tests run the real sync engine against a fake Drive API. They
cover uploads and downloads, edits in both directions, echo suppression,
renames and moves without re-upload, deletions, conflicts, rclone migration
without transfers, batching, the deletion guard, resumable uploads, and
selective sync.
