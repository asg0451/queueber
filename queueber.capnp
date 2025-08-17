@0xbab6e22de0402699;

using Rust = import "rust.capnp";
$Rust.parentModule("protocol");


struct AddRequest {
    items @0 :List(Item);
}

struct AddResponse {
    ids @0 :List(Data);
}

struct RemoveRequest {
    id @0 :Data;
    lease @1 :Data;
}

struct RemoveResponse {
    removed @0 :Bool;
}

struct PollRequest {
    leaseValiditySecs @0 :UInt64;
}

struct Item {
    contents @0 :Data;
    # unset/ignored when adding
    visibilityTimeoutSecs @1 :UInt64;
}

struct PollResponse {
    items @0 :List(PolledItem);
    lease @1 :Data;
}

struct PolledItem {
    contents @0 :Data;
    id @1 :Data;
}

interface Queue {
    add @00 (req :AddRequest) -> (resp :AddResponse);
    remove @01 (req :RemoveRequest) -> (resp :RemoveResponse);
    poll @02 (req :PollRequest) -> (resp :PollResponse);
}


# internal stuff (TODO: move to own file)
struct StoredItem {
    contents @0 :Data;
    visibilityTsIndexKey @1 :Data;
    id @2 :Data;
}
