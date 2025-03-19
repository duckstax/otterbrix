#pragma once

#include "base.hpp"
#include <components/logical_plan/forward.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <cstdint>
#include <msgpack.hpp>
#include <vector>

namespace services::wal {

    using buffer_element_t = char;
    using buffer_t = std::vector<buffer_element_t>;
    using components::logical_plan::node_type;

    using size_tt = std::uint16_t;
    using crc32_t = std::uint32_t;

    template<class T>
    struct wal_entry_t final {
        size_tt size_{};
        T entry_ = nullptr;
        components::logical_plan::parameter_node_ptr params_ = nullptr;
        crc32_t last_crc32_{};
        id_t id_{};
        node_type type_{};
        crc32_t crc32_{};
    };

    crc32_t pack(buffer_t& storage, char* data, size_t size);
    buffer_t read_payload(buffer_t& input, int index_start, int index_stop);
    crc32_t read_crc32(buffer_t& input, int index_start);
    size_tt read_size_impl(buffer_t& input, int index_start);

    template<class T>
    crc32_t
    pack(buffer_t& storage, crc32_t last_crc32, id_t id, T& data, components::logical_plan::parameter_node_ptr params) {
        msgpack::sbuffer buffer;
        msgpack::packer<msgpack::sbuffer> packer(buffer);

        packer.pack_array(5);
        packer.pack_fix_uint32(last_crc32);
        packer.pack_fix_uint64(id);
        packer.pack_char(static_cast<char>(data->type()));
        packer.pack(data);
        packer.pack(params);

        return pack(storage, buffer.data(), buffer.size());
    }

    template<class T>
    inline void unpack(buffer_t& storage, wal_entry_t<T>& entry, std::pmr::memory_resource* resource) {
        msgpack::unpacked msg;
        msgpack::unpack(msg, storage.data(), storage.size());
        const auto& o = msg.get();
        entry.last_crc32_ = o.via.array.ptr[0].as<crc32_t>();
        entry.id_ = o.via.array.ptr[1].as<id_t>();
        entry.type_ = static_cast<node_type>(o.via.array.ptr[2].as<char>());
        entry.entry_ = std::move(o.via.array.ptr[3].as<T>());
        entry.params_ = components::logical_plan::to_storage_parameters(o.via.array.ptr[4], resource);
    }

    template<>
    inline void unpack(buffer_t& storage,
                       wal_entry_t<components::logical_plan::node_update_ptr>& entry,
                       std::pmr::memory_resource* resource) {
        msgpack::unpacked msg;
        msgpack::unpack(msg, storage.data(), storage.size());
        const auto& o = msg.get();
        entry.last_crc32_ = o.via.array.ptr[0].as<crc32_t>();
        entry.id_ = o.via.array.ptr[1].as<id_t>();
        entry.type_ = static_cast<node_type>(o.via.array.ptr[2].as<char>());
        assert(entry.type_ == node_type::update_t);
        entry.entry_ = std::move(components::logical_plan::to_node_update(o.via.array.ptr[3], resource));
        entry.params_ = components::logical_plan::to_storage_parameters(o.via.array.ptr[4], resource);
    }

    template<>
    inline void unpack(buffer_t& storage,
                       wal_entry_t<components::logical_plan::node_insert_ptr>& entry,
                       std::pmr::memory_resource* resource) {
        msgpack::unpacked msg;
        msgpack::unpack(msg, storage.data(), storage.size());
        const auto& o = msg.get();
        entry.last_crc32_ = o.via.array.ptr[0].as<crc32_t>();
        entry.id_ = o.via.array.ptr[1].as<id_t>();
        entry.type_ = static_cast<node_type>(o.via.array.ptr[2].as<char>());
        assert(entry.type_ == node_type::insert_t);
        entry.entry_ = std::move(components::logical_plan::to_node_insert(o.via.array.ptr[3], resource));
        entry.params_ = components::logical_plan::to_storage_parameters(o.via.array.ptr[4], resource);
    }

    template<>
    inline void unpack(buffer_t& storage,
                       wal_entry_t<components::logical_plan::node_delete_ptr>& entry,
                       std::pmr::memory_resource* resource) {
        msgpack::unpacked msg;
        msgpack::unpack(msg, storage.data(), storage.size());
        const auto& o = msg.get();
        entry.last_crc32_ = o.via.array.ptr[0].as<crc32_t>();
        entry.id_ = o.via.array.ptr[1].as<id_t>();
        entry.type_ = static_cast<node_type>(o.via.array.ptr[2].as<char>());
        assert(entry.type_ == node_type::delete_t);
        entry.entry_ = std::move(components::logical_plan::to_node_delete(o.via.array.ptr[3], resource));
        entry.params_ = components::logical_plan::to_storage_parameters(o.via.array.ptr[4], resource);
    }

    id_t unpack_wal_id(buffer_t& storage);

} //namespace services::wal
