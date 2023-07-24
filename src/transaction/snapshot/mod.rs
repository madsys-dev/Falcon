
// #[cfg(feature = "cc_cfg_occ")]
// pub mod clog_snapshot;
// #[cfg(feature = "cc_cfg_occ")]
// pub type SnapShot = clog_snapshot::ClogMvccSnapShot;
// #[cfg(feature = "cc_cfg_occ")]
// pub type SnapShotEntity = clog_snapshot::ClogMvccSnapShotEntity;

// #[cfg(feature = "cc_cfg_to")]
pub mod to_snapshot;
// #[cfg(feature = "cc_cfg_to")]
pub type SnapShot = to_snapshot::ToSnapShot;
// #[cfg(feature = "cc_cfg_to")]
pub type SnapShotEntity = to_snapshot::ToSnapShotEntity;

// #[cfg(feature = "cc_cfg_2pl")]
// pub mod to_snapshot;
// #[cfg(feature = "cc_cfg_2pl")]
// pub type SnapShot = to_snapshot::ToSnapShot;
// #[cfg(feature = "cc_cfg_2pl")]
// pub type SnapShotEntity = to_snapshot::ToSnapShotEntity;
