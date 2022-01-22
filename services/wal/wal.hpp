#include "goblin-engineer/core.hpp"
#include <excutor.hpp>
#include <filesystem>
#include <log/log.hpp>
#include <msgpack.hpp>

#include <fcntl.h>

#include "wal_header.hpp"

using manager = goblin_engineer::basic_manager_service_t<goblin_engineer::base_policy_light>;

/// wal disk replicate
class wdr_t final : public manager {
public:
    wdr_t(log_t& log, size_t num_workers, size_t max_throughput)
        : manager("manager_dispatcher")
        , log_(log.clone())
        , e_(new goblin_engineer::shared_work(num_workers, max_throughput), goblin_engineer::detail::thread_pool_deleter()) {
        log_.trace("manager_dispatcher_t::manager_dispatcher_t num_workers : {} , max_throughput: {}",num_workers,max_throughput);

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


/*
void wal_event_add(goblin_engineer::address_t& address, Type type, ) {
    ///goblin_engineer::send(address, )
}*/
using buffer_element_t = unsigned char;
using buffer_t = std::vector<buffer_element_t>;

struct wal_entry_t final {
    wal_entry_t()
        : crc32_(0)
        , type_(Type::init)
        , log_number_(0){}

    crc32_t  crc32_;
    Type type_;
    log_number_t log_number_;
    buffer_t payload_;
};

void pack(Type type, buffer_t& data,log_number_t number,buffer_t& out);

void unpack(buffer_t& input,wal_entry_t&);

class wal_t final : public goblin_engineer::abstract_service {
public:

    wal_t(goblin_engineer::supervisor_t* manager, log_t& log,std::filesystem::path path);
    void add_event(Type type, buffer_t & data);
    void get_event() {}
    void last_id() {}
    ~wal_t() override;

private:
    void open_file(std::filesystem::path);
    bool file_exist_(std::filesystem::path path);
    bool header_check_(std::filesystem::path path);
    void header_write_(std::filesystem::path path);

    log_t log_;
    std::filesystem::path path_;
    std::atomic<log_number_t> log_number_{0};
    std::size_t writed_{0};
    int fd_ = -1;
    buffer_t buffer_;
};