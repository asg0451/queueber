@0xbab6e22de0402699;

using Rust = import "rust.capnp";
$Rust.parentModule("protocol");


struct AddRequest {
    contents @0 :Data;
    visibilityTimeoutSecs @1 :UInt64;
}

struct AddResponse {
    id @0 :UInt64;
}

struct RemoveRequest {
    id @0 :UInt64;
}

struct RemoveResponse {
    removed @0 :Bool;
}

struct PollRequest {
    visibilityTimeoutSecs @0 :UInt64;
}

struct Item {
    id @0 :UInt64;
    contents @1 :Data;
    visibilityTimeoutSecs @2 :UInt64;
}

struct PollResponse {
    items @0 :List(Item);
}

interface Queue {
    add @00 (req :AddRequest) -> (resp :AddResponse);
    remove @01 (req :RemoveRequest) -> (resp :RemoveResponse);
    poll @02 (req :PollRequest) -> (resp :PollResponse);
}
