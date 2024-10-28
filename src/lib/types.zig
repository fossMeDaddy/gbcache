pub const ValueMetadataSize = @sizeOf(ValueMetadata);
pub const ValueMetadata = struct {
    value_offset: u64,
    value_size: u64,
};
