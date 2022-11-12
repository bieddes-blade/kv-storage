#pragma once

#include <kv/types.hpp>

#include <whirl/node/node_base.hpp>
#include <whirl/node/logging.hpp>
#include <whirl/helpers/serialize.hpp>

#include <whirl/rpc/use/service_base.hpp>

#include <await/fibers/sync/future.hpp>
#include <await/fibers/core/await.hpp>
#include <await/futures/combine/quorum.hpp>

#include <cereal/types/string.hpp>
#include <fmt/ostream.h>

namespace kv {

using namespace await::fibers;
using namespace whirl;

//////////////////////////////////////////////////////////////////////

// Реплики хранят версионированные значения

struct Timestamp {
  size_t ts;
  NodeId id;

  SERIALIZE(ts, id)
};

bool operator<(const Timestamp& x, const Timestamp& y) {
  return std::tie(x.ts, x.id) < std::tie(y.ts, y.id);
}

bool operator>(const Timestamp& x, const Timestamp& y) {
  return std::tie(x.ts, x.id) > std::tie(y.ts, y.id);
}

bool operator==(const Timestamp& x, const Timestamp& y) {
  return std::tie(x.ts, x.id) == std::tie(y.ts, y.id);
}

std::ostream& operator<<(std::ostream& o, const Timestamp& a) {
  std::cout << "ts: " << a.ts << "\tid: " << a.id << std::endl;
  return o;
}

struct StampedValue {
  Value value;
  Timestamp ts;

  static StampedValue NoValue() {
    return {0, 0};
  }

  // Сериализация для локального хранилища и передачи по сети
  SERIALIZE(value, ts)
};

// Для логирования
std::ostream& operator<<(std::ostream& out, const StampedValue& v) {
  out << "{" << v.value << ", ts: " << v.ts << "}";
  return out;
}

//////////////////////////////////////////////////////////////////////

// KV storage node

class KVNode : public rpc::ServiceBase<KVNode>,
               public NodeBase,
               public std::enable_shared_from_this<KVNode> {
 public:
  KVNode(NodeServices services)
      : NodeBase(std::move(services)),
        kv_(StorageBackend(), "kv"),
        max_storage_(StorageBackend(), "max_storage") {
  }

 protected:
  // NodeBase
  void RegisterRPCServices(const rpc::IServerPtr& rpc_server) override {
    rpc_server->RegisterService("KV", shared_from_this());
  }

  // ServiceBase
  void RegisterRPCMethods() override {
    RPC_REGISTER_METHOD(Set);
    RPC_REGISTER_METHOD(Get);
    RPC_REGISTER_METHOD(Write);
    RPC_REGISTER_METHOD(Read);
  }

  // Публичные операции - Set/Get

  void Set(Key k, Value v) {
    Timestamp write_ts = ChooseWriteTimestamp(k);
    NODE_LOG("Write timestamp: {}", write_ts);

    std::vector<Future<void>> writes;
    for (size_t i = 0; i < PeerCount(); ++i) {
      writes.push_back(
          PeerChannel(i).Call("KV.Write", k, StampedValue{v, write_ts}));
    }

    // Синхронно дожидаемся большинства подтверждений
    Await(Quorum(std::move(writes), Majority())).ExpectOk();
  }

  Value Get(Key k) {
    std::vector<Future<StampedValue>> reads;

    // Отправляем пирам команду Read(k)
    for (size_t i = 0; i < PeerCount(); ++i) {
      reads.push_back(PeerChannel(i).Call("KV.Read", k));
    }

    // Собираем кворум большинства
    auto values = Await(Quorum(std::move(reads), Majority())).Value();

    for (size_t i = 0; i < values.size(); ++i) {
      NODE_LOG("{}-th value in read quorum: {}", i + 1, values[i]);
    }

    auto winner = FindNewestValue(values);

    std::vector<Future<void>> writes;

    for (size_t i = 0; i < PeerCount(); ++i) {
      writes.push_back(PeerChannel(i).Call("KV.Write", k, winner));
    }

    Await(Quorum(std::move(writes), Majority())).ExpectOk();

    return winner.value;
  }

  // Внутренние команды репликам

  void Write(Key k, StampedValue v) {
    std::optional<StampedValue> local = kv_.TryGet(k);

    if (!local.has_value()) {
      // Раньше не видели данный ключ
      Update(k, v);
    } else {
      // Если временная метка записи больше, чем локальная,
      // то обновляем значение в локальном хранилище
      if (v.ts > local->ts) {
        Update(k, v);
      }
    }
  }

  void Update(Key k, StampedValue v) {
    NODE_LOG("Write '{}' -> {}", k, v);
    kv_.Set(k, v);
  }

  StampedValue Read(Key k) {
    return kv_.GetOr(k, StampedValue::NoValue());
  }

  Timestamp ChooseWriteTimestamp(Key k) {
    // Локальные часы могут быть рассинхронизированы
    // Возмонжо стоит использовать сервис TrueTime?
    // См. TrueTime()
    std::vector<Future<StampedValue>> reads;

    for (size_t i = 0; i < PeerCount(); ++i) {
      reads.push_back(PeerChannel(i).Call("KV.Read", k));
    }

    auto values = Await(Quorum(std::move(reads), Majority())).Value();

    auto max_st = max_storage_.GetOr("max_storage", 0);

    auto new_val = FindNewestValue(values).ts.ts;

    Timestamp answer;
    size_t id = Id();
    answer = Timestamp{max_st + 1, id};

    if (new_val + 1 > max_st) {
      max_storage_.Set("max_storage", new_val + 1);
      answer = Timestamp{new_val + 1, id};
    } else {
      max_storage_.Set("max_storage", max_st + 1);
    }
    return answer;
  }

  // Выбираем самый свежий результат из кворумных чтений
  StampedValue FindNewestValue(const std::vector<StampedValue>& values) const {
    auto winner = values[0];
    for (size_t i = 1; i < values.size(); ++i) {
      if (values[i].ts > winner.ts) {
        winner = values[i];
      }
    }
    return winner;
  }

  // Размер кворума
  size_t Majority() const {
    return PeerCount() / 2 + 1;
  }

 private:
  // Локальное персистентное K/V хранилище
  // Ключи - строки, значения - StampedValue
  LocalKVStorage<StampedValue> kv_;
  LocalKVStorage<size_t> max_storage_;
};

}  // namespace kv
