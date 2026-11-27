//! Pure three-way reconciliation: given the local state, the remote state
//! and the last-synced base state of one path, decide what to do.
//! Kept free of I/O so it can be exhaustively unit-tested.

use crate::config::{DeleteMode, SyncMode};
use crate::db::Kind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Nothing,
    /// Both sides already agree; just record the base state.
    Record,
    /// Drop a stale base entry (gone on both sides).
    Forget,
    Upload,
    Download,
    MkdirRemote,
    MkdirLocal,
    DeleteRemote,
    DeleteLocal,
    Conflict,
}

#[derive(Debug, Clone, Copy)]
pub struct Side {
    pub kind: Kind,
    /// Changed relative to the base entry (always false if no base).
    pub changed: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct Ctx {
    pub mode: SyncMode,
    pub delete: DeleteMode,
}

/// `same_content` is only consulted when both sides exist and either there
/// is no base or both changed: `Some(true)` if they hold identical content.
pub fn decide(
    ctx: Ctx,
    l: Option<Side>,
    r: Option<Side>,
    has_base: bool,
    same_content: impl FnOnce() -> bool,
) -> Action {
    use Action::*;
    use SyncMode::*;
    let up = ctx.mode != DownloadOnly;
    let down = ctx.mode != UploadOnly;

    match (l, r, has_base) {
        (None, None, false) => Nothing,
        (None, None, true) => Forget,

        // New on one side only.
        (Some(l), None, false) => {
            if !up || matches!(l.kind, Kind::Link | Kind::Export) {
                Nothing
            } else if l.kind == Kind::Dir {
                MkdirRemote
            } else {
                Upload
            }
        }
        (None, Some(r), false) => {
            if !down {
                Nothing
            } else if r.kind == Kind::Dir {
                MkdirLocal
            } else {
                Download
            }
        }

        // Present on both sides, never synced (e.g. first run after rclone).
        (Some(l), Some(r), false) => match (l.kind, r.kind) {
            (Kind::Dir, Kind::Dir) => Record,
            (Kind::Dir, _) | (_, Kind::Dir) => Conflict,
            (_, Kind::Link) | (_, Kind::Export) => {
                if down {
                    Download
                } else {
                    Nothing
                }
            }
            _ => {
                if same_content() {
                    Record
                } else {
                    match ctx.mode {
                        TwoWay => Conflict,
                        UploadOnly => Upload,
                        DownloadOnly => Download,
                    }
                }
            }
        },

        // Present on both sides, previously synced.
        (Some(l), Some(r), true) => {
            if l.kind == Kind::Dir && r.kind == Kind::Dir {
                return Nothing;
            }
            if matches!(r.kind, Kind::Link | Kind::Export) {
                // Local edits to link/export files are never uploaded; an
                // edited export whose document also changed is kept as a
                // conflicted copy rather than overwritten.
                return match (r.changed && down, r.kind == Kind::Export && l.changed) {
                    (true, true) => Conflict,
                    (true, false) => Download,
                    _ => Nothing,
                };
            }
            match (l.changed, r.changed) {
                (false, false) => Nothing,
                (true, false) => {
                    if up {
                        Upload
                    } else {
                        Nothing
                    }
                }
                (false, true) => match ctx.mode {
                    UploadOnly => Upload,
                    _ => Download,
                },
                (true, true) => {
                    if l.kind == r.kind && same_content() {
                        Record
                    } else {
                        match ctx.mode {
                            TwoWay => Conflict,
                            UploadOnly => Upload,
                            DownloadOnly => Download,
                        }
                    }
                }
            }
        }

        // Deleted locally.
        (None, Some(r), true) => match ctx.mode {
            DownloadOnly => {
                if r.kind == Kind::Dir {
                    MkdirLocal
                } else {
                    Download
                }
            }
            _ if ctx.delete == DeleteMode::Never => {
                if ctx.mode == UploadOnly {
                    Forget
                } else if r.kind == Kind::Dir {
                    MkdirLocal
                } else {
                    Download
                }
            }
            TwoWay if r.changed && r.kind != Kind::Dir => Download,
            _ => DeleteRemote,
        },

        // Deleted remotely.
        (Some(l), None, true) => match ctx.mode {
            UploadOnly => {
                if l.kind == Kind::Dir {
                    MkdirRemote
                } else {
                    Upload
                }
            }
            _ if ctx.delete == DeleteMode::Never => {
                if ctx.mode == DownloadOnly {
                    Forget
                } else if matches!(l.kind, Kind::Link | Kind::Export) {
                    // The native document is gone; nothing to restore from a link.
                    DeleteLocal
                } else if l.kind == Kind::Dir {
                    MkdirRemote
                } else {
                    Upload
                }
            }
            TwoWay if l.changed && !matches!(l.kind, Kind::Dir | Kind::Link | Kind::Export) => Upload,
            _ => DeleteLocal,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Action::*;

    const TW: Ctx = Ctx { mode: SyncMode::TwoWay, delete: DeleteMode::Trash };
    const UP: Ctx = Ctx { mode: SyncMode::UploadOnly, delete: DeleteMode::Trash };
    const DN: Ctx = Ctx { mode: SyncMode::DownloadOnly, delete: DeleteMode::Trash };
    const NEVER: Ctx = Ctx { mode: SyncMode::TwoWay, delete: DeleteMode::Never };

    fn f(changed: bool) -> Option<Side> {
        Some(Side { kind: Kind::File, changed })
    }
    fn d() -> Option<Side> {
        Some(Side { kind: Kind::Dir, changed: false })
    }

    #[test]
    fn new_items() {
        assert_eq!(decide(TW, f(false), None, false, || false), Upload);
        assert_eq!(decide(TW, None, f(false), false, || false), Download);
        assert_eq!(decide(TW, d(), None, false, || false), MkdirRemote);
        assert_eq!(decide(TW, None, d(), false, || false), MkdirLocal);
        assert_eq!(decide(DN, f(false), None, false, || false), Nothing);
        assert_eq!(decide(UP, None, f(false), false, || false), Nothing);
    }

    #[test]
    fn initial_match() {
        assert_eq!(decide(TW, f(false), f(false), false, || true), Record);
        assert_eq!(decide(TW, f(false), f(false), false, || false), Conflict);
        assert_eq!(decide(UP, f(false), f(false), false, || false), Upload);
        assert_eq!(decide(TW, d(), d(), false, || false), Record);
        assert_eq!(decide(TW, d(), f(false), false, || false), Conflict);
    }

    #[test]
    fn edits() {
        assert_eq!(decide(TW, f(false), f(false), true, || false), Nothing);
        assert_eq!(decide(TW, f(true), f(false), true, || false), Upload);
        assert_eq!(decide(TW, f(false), f(true), true, || false), Download);
        assert_eq!(decide(TW, f(true), f(true), true, || false), Conflict);
        assert_eq!(decide(TW, f(true), f(true), true, || true), Record);
        assert_eq!(decide(DN, f(true), f(false), true, || false), Nothing);
        assert_eq!(decide(UP, f(false), f(true), true, || false), Upload);
    }

    #[test]
    fn deletions() {
        assert_eq!(decide(TW, None, f(false), true, || false), DeleteRemote);
        assert_eq!(decide(TW, None, f(true), true, || false), Download);
        assert_eq!(decide(TW, f(false), None, true, || false), DeleteLocal);
        assert_eq!(decide(TW, f(true), None, true, || false), Upload);
        assert_eq!(decide(TW, None, None, true, || false), Forget);
        assert_eq!(decide(DN, None, f(false), true, || false), Download);
        assert_eq!(decide(UP, f(false), None, true, || false), Upload);
        assert_eq!(decide(NEVER, None, f(false), true, || false), Download);
        assert_eq!(decide(NEVER, f(false), None, true, || false), Upload);
    }

    #[test]
    fn links_are_never_uploaded() {
        let link = |changed| Some(Side { kind: Kind::Link, changed });
        assert_eq!(decide(TW, link(true), link(false), true, || false), Nothing);
        assert_eq!(decide(TW, link(false), link(true), true, || false), Download);
        assert_eq!(decide(TW, link(false), None, false, || false), Nothing);
        assert_eq!(decide(TW, None, link(false), true, || false), DeleteRemote);
        assert_eq!(decide(NEVER, link(false), None, true, || false), DeleteLocal);
        let export = |changed| Some(Side { kind: Kind::Export, changed });
        assert_eq!(decide(TW, export(true), export(true), true, || false), Conflict);
        assert_eq!(decide(TW, export(false), export(true), true, || false), Download);
        assert_eq!(decide(TW, export(true), export(false), true, || false), Nothing);
    }
}
