use std::borrow::Borrow;

use once_cell::sync::Lazy;
use prometheus::{register_int_counter, register_int_counter_vec, IntCounter, IntCounterVec, Opts};

const NAMESPACE: &str = "toy_raft";

static PEER_RECEIVE_FAILURE: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "peer_receive_failure_total",
        "The number of failures to receive messages",
    )
    .namespace(NAMESPACE);

    register_int_counter_vec!(opts, &["peer", "status"]).unwrap()
});

pub static APPEND_ENTRIES: Lazy<IntCounter> = Lazy::new(|| {
    let opts =
        Opts::new("append_entries_total", "The number of AppendEntries calls").namespace(NAMESPACE);

    register_int_counter!(opts).unwrap()
});

pub static APPEND_ENTRIES_FAILURE: Lazy<IntCounter> = Lazy::new(|| {
    let opts = Opts::new(
        "append_entries_failure_total",
        "The number of failures to process AppendEntries calls",
    )
    .namespace(NAMESPACE);

    register_int_counter!(opts).unwrap()
});

pub fn inc_peer_receive_failure(peer: &str, status: tonic::Code) {
    PEER_RECEIVE_FAILURE
        .with_label_values(&[peer, (status as i32).to_string().borrow()])
        .inc();
}
