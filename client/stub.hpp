#pragma once

#include <kv/types.hpp>

#include <whirl/rpc/use/channel.hpp>

#include <await/fibers/sync/future.hpp>
#include <await/fibers/core/await.hpp>

namespace kv {

using await::fibers::Await;
using whirl::rpc::TChannel;

//////////////////////////////////////////////////////////////////////

class KVBlockingStub {
 public:
  explicit KVBlockingStub(TChannel& channel) : channel_(channel) {
  }

  void Set(Key k, Value v) {
    Await(channel_.Call("KV.Set", k, v).As<void>()).ExpectOk();
  }

  Value Get(Key k) {
    return Await(channel_.Call("KV.Get", k).As<Value>()).Value();
  }

 private:
  TChannel& channel_;
};

}  // namespace kv
