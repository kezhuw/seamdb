#[derive(Copy, Eq, PartialOrd, Ord)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Timestamp {
    #[prost(uint64, tag = "1")]
    pub seconds: u64,
    #[prost(uint32, tag = "2")]
    pub nanoseconds: u32,
    #[prost(uint32, tag = "3")]
    pub logical: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub enum Value {
    #[prost(int64, tag = "1")]
    Int(i64),
    #[prost(double, tag = "2")]
    Float(f64),
    #[prost(bytes, tag = "3")]
    Bytes(::prost::alloc::vec::Vec<u8>),
    #[prost(string, tag = "4")]
    String(::prost::alloc::string::String),
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimestampedValue {
    #[prost(message, required, tag = "1")]
    pub value: Value,
    #[prost(message, required, tag = "2")]
    pub timestamp: Timestamp,
}
#[derive(::num_enum::TryFromPrimitive)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TxnStatus {
    Pending = 0,
    Committed = 1,
    Aborted = 2,
}
impl TxnStatus {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            TxnStatus::Pending => "Pending",
            TxnStatus::Committed => "Committed",
            TxnStatus::Aborted => "Aborted",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Pending" => Some(Self::Pending),
            "Committed" => Some(Self::Committed),
            "Aborted" => Some(Self::Aborted),
            _ => None,
        }
    }
}
