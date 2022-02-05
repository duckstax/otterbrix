#include "dispatcher/route.hpp"
#include "goblin-engineer/core.hpp"
#include <excutor.hpp>
#include <filesystem>
#include <log/log.hpp>

#include <msgpack.hpp>
#include <msgpack/adaptor/vector.hpp>

using manager = goblin_engineer::basic_manager_service_t<goblin_engineer::base_policy_light>;

/// wal disk replicate
class wdr_t final : public manager {
public:
    wdr_t(log_t& log, size_t num_workers, size_t max_throughput)
        : manager("manager_dispatcher")
        , log_(log.clone())
        , e_(new goblin_engineer::shared_work(num_workers, max_throughput), goblin_engineer::detail::thread_pool_deleter()) {
        log_.trace("manager_dispatcher_t::manager_dispatcher_t num_workers : {} , max_throughput: {}", num_workers, max_throughput);

        log_.trace("manager_dispatcher_t start thread pool");
        e_->start();
    }

protected:
    auto executor_impl() noexcept -> goblin_engineer::abstract_executor* {
        return e_.get();
    }

    //NOTE: behold thread-safety!
    auto enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void {
        set_current_message(std::move(msg));
        execute();
    }
    auto add_actor_impl(goblin_engineer::actor a) -> void {
        actor_storage_.emplace_back(std::move(a));
    }
    auto add_supervisor_impl(goblin_engineer::supervisor) -> void {
        log_.error("manager_dispatcher_t::add_supervisor_impl");
    }

private:
    log_t log_;
    goblin_engineer::executor_ptr e_;
    std::vector<goblin_engineer::actor> actor_storage_;
    std::unordered_map<std::string, goblin_engineer::address_t> dispatcher_to_address_book_;
    std::vector<goblin_engineer::address_t> dispathers_;
};

using manager_wdr_ptr = goblin_engineer::intrusive_ptr<wdr_t>;

using buffer_element_t = char;
using buffer_t = std::vector<buffer_element_t>;

using size_tt = std::uint16_t;
using log_number_t = std::uint64_t;
using crc32_t = std::uint32_t;

struct entry_t final {
    entry_t() = default;
    entry_t(crc32_t lastCrc32, Type type, log_number_t logNumber, buffer_t payload)
        : last_crc32_(lastCrc32)
        , type_(type)
        , log_number_(logNumber)
        , payload_(std::move(payload)) {}

    crc32_t last_crc32_;
    Type type_;
    log_number_t log_number_;
    buffer_t payload_; /// msgpack
};

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<entry_t> final {
                msgpack::object const& operator()(msgpack::object const& o, entry_t& v) const {
                    if (o.type != msgpack::type::ARRAY) {
                        throw msgpack::type_error();
                    }

                    if (o.via.array.size != 4) {
                        throw msgpack::type_error();
                    }

                    auto last_crc32_ = o.via.array.ptr[0].as<crc32_t>();
                    auto type = o.via.array.ptr[1].as<uint8_t>();
                    auto log_number_ = o.via.array.ptr[2].as<log_number_t>();
                    auto buffer = o.via.array.ptr[3].as<buffer_t>();
                    v = entry_t(last_crc32_, static_cast<Type>(type), log_number_, std::move(buffer));
                    return o;
                }
            };

            template<>
            struct pack<entry_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, entry_t const& v) const {
                    o.pack_array(4);
                    o.pack_fix_uint32(v.last_crc32_);
                    o.pack_char(static_cast<char>(v.type_));
                    o.pack_fix_uint64(v.log_number_);
                    o.pack(v.payload_);
                    return o;
                }
            };

            template<>
            struct object_with_zone<entry_t> final {
                void operator()(msgpack::object::with_zone& o, entry_t const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 4;
                    o.via.array.ptr = static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size, MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v.last_crc32_, o.zone);
                    o.via.array.ptr[1] = msgpack::object(static_cast<uint8_t>(v.type_), o.zone);
                    o.via.array.ptr[2] = msgpack::object(v.log_number_, o.zone);
                    o.via.array.ptr[3] = msgpack::object(v.payload_, o.zone);
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack

struct wal_entry_t final {
    size_tt size_;
    entry_t entry_;
    crc32_t crc32_;
};

crc32_t pack(buffer_t& storage, entry_t&);

void unpack(buffer_t& storage, wal_entry_t&);

void unpack_v2(buffer_t& storage, wal_entry_t&);

class wal_t final : public goblin_engineer::abstract_service {
public:
    wal_t(goblin_engineer::supervisor_t* manager, log_t& log, std::filesystem::path path);
    void add_event(Type type, buffer_t& data);
    void last_id() {}
    ~wal_t() override;

private:
    void open_file(std::filesystem::path);
    bool file_exist_(std::filesystem::path path);

    log_t log_;
    std::filesystem::path path_;
    std::atomic<log_number_t> log_number_{0};
    crc32_t last_crc32_{0};
    std::size_t writed_{0};
    std::size_t read_{0};
    int fd_ = -1;
    buffer_t buffer_;
#ifdef DEV_MODE
public:
    size_tt read_size(size_t start_index);
    buffer_t read(size_t start_index, size_t finish_index);
#endif
};