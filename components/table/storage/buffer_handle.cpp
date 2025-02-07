#include "buffer_handle.hpp"

#include "block_handle.hpp"
#include "block_manager.hpp"
#include "buffer_manager.hpp"

namespace components::table::storage {

    buffer_handle_t::buffer_handle_t()
        : handle_(nullptr)
        , node_(nullptr) {}

    buffer_handle_t::buffer_handle_t(std::shared_ptr<block_handle_t> handle, file_buffer_t* node)
        : handle_(std::move(handle))
        , node_(node) {}

    buffer_handle_t::buffer_handle_t(buffer_handle_t&& other) noexcept
        : node_(nullptr) {
        std::swap(node_, other.node_);
        std::swap(handle_, other.handle_);
    }

    buffer_handle_t& buffer_handle_t::operator=(buffer_handle_t&& other) noexcept {
        std::swap(node_, other.node_);
        std::swap(handle_, other.handle_);
        return *this;
    }

    buffer_handle_t::~buffer_handle_t() { destroy(); }

    bool buffer_handle_t::is_valid() const { return node_ != nullptr; }

    void buffer_handle_t::destroy() {
        if (!handle_ || !is_valid()) {
            return;
        }
        handle_->block_manager.buffer_manager.unpin(handle_);
        handle_.reset();
        node_ = nullptr;
    }

    file_buffer_t& buffer_handle_t::file_buffer() {
        assert(node_);
        return *node_;
    }

} // namespace components::table::storage