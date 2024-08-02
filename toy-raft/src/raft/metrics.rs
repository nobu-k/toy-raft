use std::borrow::Borrow;

use once_cell::sync::Lazy;
use prometheus::{register_int_counter_vec, IntCounterVec, Opts};

const NAMESPACE: &str = "toy_raft";

static PEER_RECEIVE_FAILURE: Lazy<IntCounterVec> = Lazy::new(|| {
    let opts = Opts::new(
        "peer_receive_failure_total",
        "The number of failures to receive messages",
    )
    .namespace(NAMESPACE);

    register_int_counter_vec!(opts, &["peer", "status"]).unwrap()
});

pub fn inc_peer_receive_failure(peer: &str, status: tonic::Code) {
    PEER_RECEIVE_FAILURE
        .with_label_values(&[peer, (status as i32).to_string().borrow()])
        .inc();
}
