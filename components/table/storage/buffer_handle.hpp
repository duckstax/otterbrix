#pragma once

#include <cassert>
#include <memory>

#include "file_buffer.hpp"

namespace components::table::storage {
    class file_buffer_t;
    class block_handle_t;

    class buffer_handle_t {
    public:
        buffer_handle_t();
        explicit buffer_handle_t(std::shared_ptr<block_handle_t> handle, file_buffer_t* node);
        ~buffer_handle_t();
        buffer_handle_t(const buffer_handle_t& other) = delete;
        buffer_handle_t& operator=(const buffer_handle_t&) = delete;
        buffer_handle_t(buffer_handle_t&& other) noexcept;
        buffer_handle_t& operator=(buffer_handle_t&&) noexcept;

        bool is_valid() const;
        std::byte* ptr() const {
            assert(is_valid());
            return node_->buffer_;
        }
        std::byte* ptr() {
            assert(is_valid());
            return node_->buffer_;
        }
        file_buffer_t& file_buffer();
        void destroy();

        const std::shared_ptr<block_handle_t>& block_handle() const { return handle_; }

    private:
        std::shared_ptr<block_handle_t> handle_;
        file_buffer_t* node_;
    };

} // namespace components::table::storage