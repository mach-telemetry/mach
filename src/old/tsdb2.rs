use crate::{
    buffer::{ActiveSegment, ActiveSegmentWriter, FlushBuffer, SnapshotBuffer},
};

struct MemSeries {
    active_segment: ActiveSegment,
}
