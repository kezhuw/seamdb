#[derive(Copy, Eq, Hash, PartialOrd, Ord)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Uuid {
    #[prost(fixed64, tag = "1")]
    pub msb: u64,
    #[prost(fixed64, tag = "2")]
    pub lsb: u64,
}
#[derive(Copy, Eq, PartialOrd, Ord)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MessageId {
    #[prost(uint64, tag = "1")]
    pub epoch: u64,
    /// Sequence for deduplication.
    #[prost(uint64, tag = "2")]
    pub sequence: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyRange {
    #[prost(bytes = "vec", tag = "1")]
    pub start: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub end: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeySpan {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    /// Empty if this is a single key span.
    #[prost(bytes = "vec", tag = "2")]
    pub end: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardSegment {
    #[prost(string, tag = "1")]
    pub file: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub log: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub range: ::core::option::Option<KeyRange>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardDepot {
    #[prost(message, repeated, tag = "1")]
    pub segments: ::prost::alloc::vec::Vec<ShardSegment>,
    #[prost(string, tag = "2")]
    pub file: ::prost::alloc::string::String,
    /// kafka://kafka-cluster-address/topic-name?start=1
    #[prost(string, tag = "3")]
    pub log: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "4")]
    pub range: ::core::option::Option<KeyRange>,
}
/// range/range1_end_key ==> ShardDescriptor
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardDescriptor {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(message, required, tag = "3")]
    pub range: KeyRange,
    #[prost(uint64, tag = "4")]
    pub tablet_id: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardDescription {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(message, required, tag = "3")]
    pub range: KeyRange,
    #[prost(message, repeated, tag = "4")]
    pub segments: ::prost::alloc::vec::Vec<ShardSegment>,
    /// Default to All.
    #[prost(enumeration = "!ShardMergeBounds", tag = "5")]
    pub merge_bounds: ShardMergeBounds,
}
/// system/tablets/deployments/id1 ==> deployment1
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TabletDeployment {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    /// Increment on leader change.
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
    /// Increment on not-leader change and reset on leader change.
    #[prost(uint64, tag = "3")]
    pub generation: u64,
    #[prost(string, repeated, tag = "4")]
    pub servers: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TabletDescriptor {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(uint64, tag = "2")]
    pub generation: u64,
    #[prost(string, tag = "4")]
    pub manifest_log: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TabletDescription {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(uint64, tag = "2")]
    pub generation: u64,
    /// sort by range
    #[prost(message, repeated, tag = "3")]
    pub shards: ::prost::alloc::vec::Vec<ShardDescription>,
    #[prost(string, tag = "4")]
    pub data_log: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TabletManifest {
    #[prost(message, required, tag = "1")]
    pub tablet: TabletDescription,
    #[prost(message, repeated, tag = "2")]
    pub splits: ::prost::alloc::vec::Vec<TabletDescriptor>,
    #[prost(message, required, tag = "5")]
    pub watermark: TabletWatermark,
    /// Empty logs and files for future usage.
    #[prost(string, repeated, tag = "30")]
    pub empty_logs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "31")]
    pub empty_files: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Dirty logs and files from previous runs. We are uncertain about their
    /// states, so we are going to delete them in future.
    #[prost(string, repeated, tag = "32")]
    pub dirty_logs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "33")]
    pub dirty_files: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "34")]
    pub obsoleted_logs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "35")]
    pub obsoleted_files: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
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
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyValue {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    /// Tombstone if empty.
    #[prost(message, optional, tag = "2")]
    pub value: ::core::option::Option<Value>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub enum Temporal {
    #[prost(message, tag = "1")]
    Timestamp(Timestamp),
    #[prost(message, tag = "2")]
    Transaction(Transaction),
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Write {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    /// Tombstone if empty.
    #[prost(message, optional, tag = "2")]
    pub value: ::core::option::Option<Value>,
    #[prost(uint32, tag = "3")]
    pub sequence: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Batch {
    #[prost(message, repeated, tag = "2")]
    pub writes: ::prost::alloc::vec::Vec<Write>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TabletServerAssignment {
    #[prost(uint64, tag = "1")]
    pub generation: u64,
    #[prost(string, repeated, tag = "2")]
    pub servers: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ManifestMessage {
    #[prost(uint64, tag = "1")]
    pub epoch: u64,
    #[prost(uint64, tag = "2")]
    pub sequence: u64,
    #[prost(message, optional, tag = "3")]
    pub manifest: ::core::option::Option<TabletManifest>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataMessage {
    #[prost(uint64, tag = "1")]
    pub epoch: u64,
    /// Sequence for deduplication.
    #[prost(uint64, tag = "2")]
    pub sequence: u64,
    #[prost(message, required, tag = "3")]
    pub temporal: Temporal,
    #[prost(message, optional, tag = "6")]
    pub closed_timestamp: ::core::option::Option<Timestamp>,
    #[prost(message, optional, tag = "7")]
    pub leader_expiration: ::core::option::Option<Timestamp>,
    /// None for epoch update.
    #[prost(oneof = "data_message::Operation", tags = "4, 5")]
    pub operation: ::core::option::Option<data_message::Operation>,
}
/// Nested message and enum types in `DataMessage`.
pub mod data_message {
    /// None for epoch update.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Operation {
        #[prost(message, tag = "4")]
        Write(super::Write),
        #[prost(message, tag = "5")]
        Batch(super::Batch),
    }
}
#[derive(Copy)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SequenceRange {
    #[prost(uint32, tag = "1")]
    pub start: u32,
    #[prost(uint32, tag = "2")]
    pub end: u32,
}
/// Unique priority: priority, start_ts, id
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TxnMeta {
    #[prost(message, required, tag = "1")]
    pub id: Uuid,
    /// Tablet locating key where this txn record resides in.
    #[prost(bytes = "vec", tag = "2")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    /// Increment on transaction restart.
    ///
    /// Changes:
    /// 1. Increments on client initiating transaction restart.
    /// 2. Increments on server transaction restart due to priority/deadlock reason.
    #[prost(uint32, tag = "3")]
    pub epoch: u32,
    /// Timestamp at which point this transaction was created and got timestamp assigned.
    ///
    /// This is required as writes of transaction record and data records are parallel.
    /// In transaction query, it is agnostic whether transaction record has been written.
    ///
    /// * In query, node use this timestamp to decide whether this transaction is aborted or
    ///    has not been created yet.
    /// * In creation, node use this timestamp to decide whether this transaction is able to create.
    ///
    /// This field must never be changed after assigned.
    #[prost(message, required, tag = "5")]
    pub start_ts: Timestamp,
    #[prost(int32, tag = "6")]
    pub priority: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Transaction {
    #[prost(message, required, tag = "1")]
    pub meta: TxnMeta,
    #[prost(enumeration = "!TxnStatus", tag = "3")]
    pub status: TxnStatus,
    /// Timestamp at which point this transaction is trying to read and write.
    ///
    /// Fallback to start_ts if zero.
    #[prost(message, required, tag = "6")]
    pub commit_ts: Timestamp,
    /// Only set and piggybacked from locating tablet.
    #[prost(message, required, tag = "7")]
    pub heartbeat_ts: Timestamp,
    /// Assistant field for local usage.
    #[prost(message, repeated, tag = "8")]
    pub write_set: ::prost::alloc::vec::Vec<KeySpan>,
    /// Commit set to resolve in txn abortion and commitment.
    ///
    /// Changes:
    /// 1. Sets in client initiating abortion or commitment, must contain all changes in latest epoch.
    /// 2. Cleared after all write set resolved so replayer knowns that.
    /// 3. Piggybacked resolved ranges.
    #[prost(message, repeated, tag = "9")]
    pub commit_set: ::prost::alloc::vec::Vec<KeySpan>,
    /// Piggybacked resolved set for txn abortion and commitment.
    #[prost(message, repeated, tag = "10")]
    pub resolved_set: ::prost::alloc::vec::Vec<KeySpan>,
    /// Rollbacked sequences due to savepoint and partial restart/retry.
    ///
    /// Changes:
    /// 1. Appends new sequence range.
    /// 2. Extends last sequence range's `end`.
    /// 3. Clears on epoch bump.
    #[prost(message, repeated, tag = "11")]
    pub rollbacked_sequences: ::prost::alloc::vec::Vec<SequenceRange>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TabletDeployRequest {
    #[prost(message, required, tag = "1")]
    pub deployment: TabletDeployment,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TabletDeployResponse {
    #[prost(message, repeated, tag = "1")]
    pub deployments: ::prost::alloc::vec::Vec<TabletDeployment>,
}
#[derive(Copy)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TabletWatermark {
    #[prost(message, required, tag = "1")]
    pub cursor: MessageId,
    #[prost(message, required, tag = "3")]
    pub closed_timestamp: Timestamp,
    #[prost(message, required, tag = "4")]
    pub leader_expiration: Timestamp,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TabletListResponse {
    #[prost(message, repeated, tag = "1")]
    pub deployments: ::prost::alloc::vec::Vec<TabletDeployment>,
}
/// cluster/descriptor
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClusterDescriptor {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub generation: u64,
    #[prost(message, required, tag = "3")]
    pub timestamp: Timestamp,
    #[prost(string, tag = "5")]
    pub manifest_log: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "6")]
    pub bootstrap_logs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "7")]
    pub obsoleted_logs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub enum DataRequest {
    #[prost(message, tag = "1")]
    Get(GetRequest),
    #[prost(message, tag = "2")]
    Put(PutRequest),
    #[prost(message, tag = "3")]
    Increment(IncrementRequest),
    #[prost(message, tag = "4")]
    Find(FindRequest),
    #[prost(message, tag = "5")]
    RefreshRead(RefreshReadRequest),
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardRequest {
    #[prost(uint64, tag = "1")]
    pub shard_id: u64,
    #[prost(message, required, tag = "2")]
    pub request: DataRequest,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardResponse {
    #[prost(message, optional, tag = "1")]
    pub shard: ::core::option::Option<ShardDescriptor>,
    #[prost(message, required, tag = "2")]
    pub response: DataResponse,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub enum DataResponse {
    #[prost(message, tag = "1")]
    Get(GetResponse),
    #[prost(message, tag = "2")]
    Put(PutResponse),
    #[prost(message, tag = "3")]
    Increment(IncrementResponse),
    #[prost(message, tag = "4")]
    Find(FindResponse),
    #[prost(message, tag = "5")]
    RefreshRead(RefreshReadResponse),
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchRequest {
    #[prost(uint64, tag = "1")]
    pub tablet_id: u64,
    #[prost(message, optional, tag = "2")]
    pub uncertainty: ::core::option::Option<Timestamp>,
    #[prost(message, required, tag = "3")]
    pub temporal: Temporal,
    #[prost(message, repeated, tag = "5")]
    pub requests: ::prost::alloc::vec::Vec<ShardRequest>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchResponse {
    /// Updated temporal.
    #[prost(message, required, tag = "1")]
    pub temporal: Temporal,
    /// 1. Responses to above requests.
    /// 2. No responses due to txn restart or abortion. Check piggybacked temporal for sure.
    #[prost(message, repeated, tag = "2")]
    pub responses: ::prost::alloc::vec::Vec<ShardResponse>,
    #[prost(message, repeated, tag = "3")]
    pub deployments: ::prost::alloc::vec::Vec<TabletDeployment>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LocateRequest {
    /// Empty for cluster deployment.
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LocateResponse {
    #[prost(message, required, tag = "1")]
    pub shard: ShardDescriptor,
    #[prost(message, required, tag = "2")]
    pub deployment: TabletDeployment,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint32, tag = "2")]
    pub sequence: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetResponse {
    #[prost(message, optional, tag = "1")]
    pub value: ::core::option::Option<TimestampedValue>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FindRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint32, tag = "2")]
    pub sequence: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FindResponse {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub value: ::core::option::Option<TimestampedValue>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PutRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub value: ::core::option::Option<Value>,
    #[prost(uint32, tag = "3")]
    pub sequence: u32,
    /// None => put anyway
    /// Some(Timestamp::default()) => put if absent
    /// Some(ts) => put if ts is newest(not tombstone)
    #[prost(message, optional, tag = "4")]
    pub expect_ts: ::core::option::Option<Timestamp>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PutResponse {
    #[prost(message, required, tag = "1")]
    pub write_ts: Timestamp,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IncrementRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(int64, tag = "2")]
    pub increment: i64,
    #[prost(uint32, tag = "3")]
    pub sequence: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IncrementResponse {
    #[prost(int64, tag = "1")]
    pub value: i64,
}
/// Bump read timestamp.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RefreshReadRequest {
    #[prost(message, required, tag = "1")]
    pub span: KeySpan,
    #[prost(message, required, tag = "2")]
    pub from: Timestamp,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RefreshReadResponse {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParticipateTxnRequest {
    #[prost(message, required, tag = "1")]
    pub txn: Transaction,
    #[prost(message, repeated, tag = "2")]
    pub dependents: ::prost::alloc::vec::Vec<TxnMeta>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParticipateTxnResponse {
    #[prost(message, required, tag = "1")]
    pub txn: Transaction,
    #[prost(message, repeated, tag = "2")]
    pub dependents: ::prost::alloc::vec::Vec<TxnMeta>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TabletHeartbeatRequest {
    #[prost(uint64, tag = "1")]
    pub tablet_id: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TabletHeartbeatResponse {
    #[prost(message, optional, tag = "1")]
    pub deployment: ::core::option::Option<TabletDeployment>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ShardMergeBounds {
    All = 0,
    Left = 1,
    Right = 2,
    None = 3,
}
impl ShardMergeBounds {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ShardMergeBounds::All => "All",
            ShardMergeBounds::Left => "Left",
            ShardMergeBounds::Right => "Right",
            ShardMergeBounds::None => "None",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "All" => Some(Self::All),
            "Left" => Some(Self::Left),
            "Right" => Some(Self::Right),
            "None" => Some(Self::None),
            _ => None,
        }
    }
}
#[derive(::num_enum::TryFromPrimitive)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TxnStatus {
    Pending = 0,
    /// Terminal status.
    ///
    /// Changes:
    /// 1. Client aborts manually.
    /// 2. Locating server eagerly aborts it if not hear within heartbeat interval.
    Aborted = 1,
    /// Terminal status.
    ///
    /// Changes:
    /// 1. Client proposes commitment to locating tablet.
    /// 2. Locating tablet accepts client commitment proposal.
    ///
    /// Invariants:
    /// 1. Never changed after accepted.
    Committed = 2,
}
impl TxnStatus {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            TxnStatus::Pending => "Pending",
            TxnStatus::Aborted => "Aborted",
            TxnStatus::Committed => "Committed",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Pending" => Some(Self::Pending),
            "Aborted" => Some(Self::Aborted),
            "Committed" => Some(Self::Committed),
            _ => None,
        }
    }
}
/// Generated client implementations.
pub mod tablet_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct TabletServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl TabletServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> TabletServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> TabletServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            TabletServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn deploy_tablet(
            &mut self,
            request: impl tonic::IntoRequest<super::TabletDeployRequest>,
        ) -> std::result::Result<
            tonic::Response<super::TabletDeployResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/seamdb.TabletService/DeployTablet",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("seamdb.TabletService", "DeployTablet"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn heartbeat_tablet(
            &mut self,
            request: impl tonic::IntoRequest<super::TabletHeartbeatRequest>,
        ) -> std::result::Result<
            tonic::Response<super::TabletHeartbeatResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/seamdb.TabletService/HeartbeatTablet",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("seamdb.TabletService", "HeartbeatTablet"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn batch(
            &mut self,
            request: impl tonic::IntoRequest<super::BatchRequest>,
        ) -> std::result::Result<tonic::Response<super::BatchResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/seamdb.TabletService/Batch",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("seamdb.TabletService", "Batch"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn participate_txn(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::ParticipateTxnRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::ParticipateTxnResponse>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/seamdb.TabletService/ParticipateTxn",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("seamdb.TabletService", "ParticipateTxn"));
            self.inner.streaming(req, path, codec).await
        }
        pub async fn locate(
            &mut self,
            request: impl tonic::IntoRequest<super::LocateRequest>,
        ) -> std::result::Result<tonic::Response<super::LocateResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/seamdb.TabletService/Locate",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("seamdb.TabletService", "Locate"));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod tablet_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with TabletServiceServer.
    #[async_trait]
    pub trait TabletService: Send + Sync + 'static {
        async fn deploy_tablet(
            &self,
            request: tonic::Request<super::TabletDeployRequest>,
        ) -> std::result::Result<
            tonic::Response<super::TabletDeployResponse>,
            tonic::Status,
        >;
        async fn heartbeat_tablet(
            &self,
            request: tonic::Request<super::TabletHeartbeatRequest>,
        ) -> std::result::Result<
            tonic::Response<super::TabletHeartbeatResponse>,
            tonic::Status,
        >;
        async fn batch(
            &self,
            request: tonic::Request<super::BatchRequest>,
        ) -> std::result::Result<tonic::Response<super::BatchResponse>, tonic::Status>;
        /// Server streaming response type for the ParticipateTxn method.
        type ParticipateTxnStream: futures_core::Stream<
                Item = std::result::Result<super::ParticipateTxnResponse, tonic::Status>,
            >
            + Send
            + 'static;
        async fn participate_txn(
            &self,
            request: tonic::Request<tonic::Streaming<super::ParticipateTxnRequest>>,
        ) -> std::result::Result<
            tonic::Response<Self::ParticipateTxnStream>,
            tonic::Status,
        >;
        async fn locate(
            &self,
            request: tonic::Request<super::LocateRequest>,
        ) -> std::result::Result<tonic::Response<super::LocateResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct TabletServiceServer<T: TabletService> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: TabletService> TabletServiceServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for TabletServiceServer<T>
    where
        T: TabletService,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/seamdb.TabletService/DeployTablet" => {
                    #[allow(non_camel_case_types)]
                    struct DeployTabletSvc<T: TabletService>(pub Arc<T>);
                    impl<
                        T: TabletService,
                    > tonic::server::UnaryService<super::TabletDeployRequest>
                    for DeployTabletSvc<T> {
                        type Response = super::TabletDeployResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::TabletDeployRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).deploy_tablet(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeployTabletSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/seamdb.TabletService/HeartbeatTablet" => {
                    #[allow(non_camel_case_types)]
                    struct HeartbeatTabletSvc<T: TabletService>(pub Arc<T>);
                    impl<
                        T: TabletService,
                    > tonic::server::UnaryService<super::TabletHeartbeatRequest>
                    for HeartbeatTabletSvc<T> {
                        type Response = super::TabletHeartbeatResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::TabletHeartbeatRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).heartbeat_tablet(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = HeartbeatTabletSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/seamdb.TabletService/Batch" => {
                    #[allow(non_camel_case_types)]
                    struct BatchSvc<T: TabletService>(pub Arc<T>);
                    impl<
                        T: TabletService,
                    > tonic::server::UnaryService<super::BatchRequest> for BatchSvc<T> {
                        type Response = super::BatchResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::BatchRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).batch(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = BatchSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/seamdb.TabletService/ParticipateTxn" => {
                    #[allow(non_camel_case_types)]
                    struct ParticipateTxnSvc<T: TabletService>(pub Arc<T>);
                    impl<
                        T: TabletService,
                    > tonic::server::StreamingService<super::ParticipateTxnRequest>
                    for ParticipateTxnSvc<T> {
                        type Response = super::ParticipateTxnResponse;
                        type ResponseStream = T::ParticipateTxnStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::ParticipateTxnRequest>,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).participate_txn(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ParticipateTxnSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/seamdb.TabletService/Locate" => {
                    #[allow(non_camel_case_types)]
                    struct LocateSvc<T: TabletService>(pub Arc<T>);
                    impl<
                        T: TabletService,
                    > tonic::server::UnaryService<super::LocateRequest>
                    for LocateSvc<T> {
                        type Response = super::LocateResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LocateRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).locate(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = LocateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: TabletService> Clone for TabletServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: TabletService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: TabletService> tonic::server::NamedService for TabletServiceServer<T> {
        const NAME: &'static str = "seamdb.TabletService";
    }
}
