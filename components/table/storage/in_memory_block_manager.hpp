#pragma once

#include "block_manager.hpp"

namespace components::table::storage {

    class in_memory_block_manager_t : public block_manager_t {
    public:
        using block_manager_t::block_manager_t;

        std::unique_ptr<block_t> convert_block(uint32_t block_id, file_buffer_t& source_buffer) override {
            throw std::logic_error("Cannot perform IO in in-memory database - convert_block!");
        }
        std::unique_ptr<block_t> create_block(uint32_t block_id, file_buffer_t* source_buffer) override {
            throw std::logic_error("Cannot perform IO in in-memory database - create_block!");
        }
        uint32_t free_block_id() override {
            throw std::logic_error("Cannot perform IO in in-memory database - free_block_id!");
        }
        uint32_t peek_free_block_id() override {
            throw std::logic_error("Cannot perform IO in in-memory database - peek_free_block_id!");
        }
        bool is_root_block(meta_block_pointer_t root) override {
            throw std::logic_error("Cannot perform IO in in-memory database - is_root_block!");
        }
        void mark_as_free(uint32_t block_id) override {
            throw std::logic_error("Cannot perform IO in in-memory database - mark_as_free!");
        }
        void mark_as_used(uint32_t block_id) override {
            throw std::logic_error("Cannot perform IO in in-memory database - mark_as_used!");
        }
        void mark_as_modified(uint32_t block_id) override {
            throw std::logic_error("Cannot perform IO in in-memory database - mark_as_modified!");
        }
        void increase_block_ref_count(uint32_t block_id) override {
            throw std::logic_error("Cannot perform IO in in-memory database - increase_block_ref_count!");
        }
        uint64_t meta_block() override {
            throw std::logic_error("Cannot perform IO in in-memory database - meta_block!");
        }
        void read(block_t& block) override {
            throw std::logic_error("Cannot perform IO in in-memory database - read!");
        }
        void read_blocks(file_buffer_t& buffer, uint32_t start_block, uint64_t block_count) override {
            throw std::logic_error("Cannot perform IO in in-memory database - read_blocks!");
        }
        void write(file_buffer_t& block, uint32_t block_id) override {
            throw std::logic_error("Cannot perform IO in in-memory database - write!");
        }
        bool in_memory() override { return true; }
        void file_sync() override { throw std::logic_error("Cannot perform IO in in-memory database - file_sync!"); }
        uint64_t total_blocks() override {
            throw std::logic_error("Cannot perform IO in in-memory database - total_blocks!");
        }
        uint64_t free_blocks() override {
            throw std::logic_error("Cannot perform IO in in-memory database - free_blocks!");
        }
    };

} // namespace components::table::storage