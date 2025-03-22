#![warn(clippy::cognitive_complexity)]
#![deny(clippy::await_holding_lock)]
#![warn(unused_imports)]
#![warn(clippy::all)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(noop_method_call)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::semicolon_if_nothing_returned)]
#![deny(unreachable_pub)]
#![deny(clippy::await_holding_lock)]
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![deny(clippy::unnecessary_wraps)]
#![deny(clippy::redundant_pattern_matching)]
#![allow(clippy::mutable_key_type)]

pub mod model;
pub mod runner;
mod utils;
