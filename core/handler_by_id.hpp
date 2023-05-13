#pragma once

#include <cstdint>
#include <cstdlib>

enum class group_id_t : uint8_t {
    system = 0,
    dispatcher,
    database,
    collection,
    wal,
    disk,
    wasm,
    index
};

template<class T>
constexpr uint64_t handler_id(group_id_t group, T type) {
    return 100 * static_cast<uint64_t>(group) + static_cast<uint64_t>(type);
}
