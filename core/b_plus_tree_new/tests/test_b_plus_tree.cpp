#include <catch2/catch.hpp>

#include <core/b_plus_tree_new/block.hpp>
#include <core/file/file_system.hpp>
//#include <core/b_plus_tree/b_plus_tree.hpp>
#include <log/log.hpp>
#include <thread>

#if defined(__linux__)
#include <unistd.h>
#endif

using namespace std;
using namespace core::b_plus_tree;
using namespace core::filesystem;

path_t testing_directory = "b+tree_test";

struct dummy_alloc {
    uint64_t id;
    size_t size;
    data_ptr_t buffer;
};

class limited_resource : public std::pmr::memory_resource {
public:
    explicit limited_resource(size_t memory_limit)
        : memory_limit_(memory_limit) {}

    void* do_allocate(size_t bytes, size_t alignment) override {
        if (bytes > memory_limit_ - memory_used_) {
            throw std::bad_alloc();
        } else {
            memory_used_ += bytes;
            return resource_->allocate(bytes, alignment);
        }
    }
    void do_deallocate(void* ptr, size_t bytes, size_t alignment) override {
        memory_used_ -= bytes;
        resource_->deallocate(ptr, bytes, alignment);
    }
    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

private:
    size_t memory_limit_;
    size_t memory_used_ = 0;
    std::pmr::memory_resource* resource_ = std::pmr::get_default_resource();
};

TEST_CASE("b+tree") {
    INFO("initialization") {
        local_file_system_t fs = local_file_system_t();
        if (!directory_exists(fs, testing_directory)) {
            create_directory(fs, testing_directory);
        }
        auto pid = getpid();
        testing_directory /= to_string(pid);
        if (!directory_exists(fs, testing_directory)) {
            create_directory(fs, testing_directory);
        }
    }

    INFO("block: unique ids") {
        local_file_system_t fs = local_file_system_t();
        auto fname = testing_directory;
        fname /= "block_test_file";

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            return block_t::index_t(*reinterpret_cast<uint32_t*>(data.data));
        };

        std::vector<std::string> test_data;

        for (char i = 0; i < 100; i += 2) {
            std::string str;
            str.push_back(i);
            str.push_back(0);
            str.push_back(0);
            str.push_back(0);
            for (char j = 0; j < i; j++) {
                str.push_back('a' + j);
            }
            test_data.emplace_back(str);
        }
        for (char i = 1; i < 100; i += 2) {
            std::string str;
            str.push_back(i);
            str.push_back(0);
            str.push_back(0);
            str.push_back(0);
            for (char j = 0; j < i; j++) {
                str.push_back('a' + j);
            }
            test_data.emplace_back(str);
        }

        {
            std::unique_ptr<block_t> test_block_1 = create_initialize(std::pmr::get_default_resource(), key_getter);

            REQUIRE(test_block_1->available_memory() == DEFAULT_BLOCK_SIZE - test_block_1->header_size);
            REQUIRE(test_block_1->count() == 0);
            for (uint64_t i = 0; i < test_data.size(); i++) {
                REQUIRE(test_block_1->append((data_ptr_t)(test_data[i].data()), test_data[i].size()));
                auto index = test_block_1->find_index(data_ptr_t(test_data[i].data()), test_data[i].size());
                REQUIRE(test_block_1->contains_index(index));
                REQUIRE(test_block_1->count() == test_block_1->unique_indices_count());
                // test iterators
                for (auto it = test_block_1->begin(); it != test_block_1->end(); ++it) {
                    auto test_item = std::find_if(test_data.begin(), test_data.end(), [it](const auto& item) {
                        return item.size() == it->size && std::memcmp(it->data, item.data(), it->size) == 0;
                    });
                    REQUIRE(test_item != test_data.end());
                    REQUIRE(std::memcmp(it->data, (*test_item).data(), it->size) == 0);
                }
                for (auto it = test_block_1->rbegin(); it != test_block_1->rend(); ++it) {
                    auto test_item = std::find_if(test_data.begin(), test_data.end(), [it](const auto& item) {
                        return item.size() == it->size && std::memcmp(it->data, item.data(), it->size) == 0;
                    });
                    REQUIRE(test_item != test_data.end());
                    REQUIRE(std::memcmp(it->data, (*test_item).data(), it->size) == 0);
                }
            }
            REQUIRE(test_block_1->count() == test_data.size());
            REQUIRE(test_block_1->end() - test_block_1->begin() == test_data.size());

            test_block_1->recalculate_checksum();

            unique_ptr<file_handle_t> handle =
                open_file(fs, fname, file_flags::WRITE | file_flags::FILE_CREATE, file_lock_type::NO_LOCK);
            handle->write((void*) test_block_1->internal_buffer(), test_block_1->block_size(), 0);
            // close the file
            handle.reset();
        }
        {
            std::unique_ptr<block_t> test_block_2 = create_initialize(std::pmr::get_default_resource(), key_getter);

            unique_ptr<file_handle_t> handle =
                open_file(fs, fname, file_flags::READ | file_flags::FILE_CREATE, file_lock_type::NO_LOCK);
            handle->read((void*) test_block_2->internal_buffer(), test_block_2->block_size(), 0);
            //! important to call restore_block()
            test_block_2->restore_block();

            REQUIRE(test_block_2->varify_checksum());
            REQUIRE(test_block_2->count() == test_data.size());
            REQUIRE(test_block_2->end() - test_block_2->begin() == test_data.size());
            for (uint64_t i = 0; i < test_data.size(); i++) {
                // test iterators
                for (auto it = test_block_2->begin(); it != test_block_2->end(); ++it) {
                    auto test_item = std::find_if(test_data.begin(), test_data.end(), [it](const auto& item) {
                        return item.size() == it->size && std::memcmp(it->data, item.data(), it->size) == 0;
                    });
                    REQUIRE(test_item != test_data.end());
                    REQUIRE(std::memcmp(it->data, (*test_item).data(), it->size) == 0);
                }
                for (auto it = test_block_2->rbegin(); it != test_block_2->rend(); ++it) {
                    auto test_item = std::find_if(test_data.begin(), test_data.end(), [it](const auto& item) {
                        return item.size() == it->size && std::memcmp(it->data, item.data(), it->size) == 0;
                    });
                    REQUIRE(test_item != test_data.end());
                    REQUIRE(std::memcmp(it->data, (*test_item).data(), it->size) == 0);
                }
                auto index = test_block_2->find_index(data_ptr_t(test_data[i].data()), test_data[i].size());
                REQUIRE(test_block_2->remove(data_ptr_t(test_data[i].data()), test_data[i].size()));
                REQUIRE_FALSE(test_block_2->contains_index(index));
            }
            REQUIRE(test_block_2->count() == 0);
            REQUIRE(test_block_2->available_memory() == DEFAULT_BLOCK_SIZE - test_block_2->header_size);

            // close the file
            handle.reset();
            remove_file(fs, fname);
        }
    }
    INFO("block: repeated ids") {
        local_file_system_t fs = local_file_system_t();
        auto fname = testing_directory;
        fname /= "block_test_file";
        size_t test_data_size = 100;
        size_t duplicate_count = 4;

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            return block_t::index_t(*reinterpret_cast<uint32_t*>(data.data));
        };

        std::vector<std::string> test_data;

        for (uint64_t i = 0; i < test_data_size; i++) {
            std::string str;
            str.push_back(i);
            str.push_back(0);
            str.push_back(0);
            str.push_back(0);
            for (uint64_t j = 0; j < i; j++) {
                str.push_back('a' + j);
            }
            for (size_t j = 0; j < duplicate_count; j++) {
                test_data.emplace_back(str + std::to_string(j));
            }
        }

        std::shuffle(test_data.begin(), test_data.end(), std::default_random_engine{0});

        {
            std::unique_ptr<block_t> test_block = create_initialize(std::pmr::get_default_resource(), key_getter);

            REQUIRE(test_block->available_memory() == DEFAULT_BLOCK_SIZE - test_block->header_size);
            REQUIRE(test_block->count() == 0);
            for (uint64_t i = 0; i < test_data.size(); i++) {
                REQUIRE(test_block->append((data_ptr_t)(test_data[i].data()), test_data[i].size()));
                auto index = test_block->find_index(data_ptr_t(test_data[i].data()), test_data[i].size());
                REQUIRE(test_block->contains_index(index));
                // test iterators
                for (auto it = test_block->begin(); it != test_block->end(); ++it) {
                    auto test_item = std::find_if(test_data.begin(), test_data.end(), [it](const auto& item) {
                        return item.size() == it->size && std::memcmp(it->data, item.data(), it->size) == 0;
                    });
                    REQUIRE(test_item != test_data.end());
                    REQUIRE(std::memcmp(it->data, (*test_item).data(), it->size) == 0);
                }
                for (auto it = test_block->rbegin(); it != test_block->rend(); ++it) {
                    auto test_item = std::find_if(test_data.begin(), test_data.end(), [it](const auto& item) {
                        return item.size() == it->size && std::memcmp(it->data, item.data(), it->size) == 0;
                    });
                    REQUIRE(test_item != test_data.end());
                    REQUIRE(std::memcmp(it->data, (*test_item).data(), it->size) == 0);
                }
            }
            REQUIRE(test_block->count() == test_data_size * duplicate_count);
            REQUIRE(test_block->unique_indices_count() == test_data_size);

            unique_ptr<file_handle_t> handle = open_file(fs,
                                                         fname,
                                                         file_flags::WRITE | file_flags::READ | file_flags::FILE_CREATE,
                                                         file_lock_type::NO_LOCK);
            handle->write((void*) test_block->internal_buffer(), test_block->block_size(), 0);
            handle->sync();

            // remove by index

            REQUIRE(test_block->count() == test_data_size * duplicate_count);
            REQUIRE(test_block->unique_indices_count() == test_data_size);
            for (uint32_t i = 0; i < test_data_size; i++) {
                auto index = test_block->find_index(data_ptr_t(&i), sizeof(uint32_t));
                REQUIRE(test_block->remove_index(index));
                REQUIRE_FALSE(test_block->contains_index(index));
            }
            REQUIRE(test_block->count() == 0);
            REQUIRE(test_block->unique_indices_count() == 0);

            handle->read((void*) test_block->internal_buffer(), test_block->block_size(), 0);
            test_block->restore_block();

            // remove one by one

            REQUIRE(test_block->count() == test_data_size * duplicate_count);
            REQUIRE(test_block->unique_indices_count() == test_data_size);
            for (uint64_t i = 0; i < test_data.size(); i++) {
                REQUIRE(
                    test_block->remove((data_ptr_t)(test_data[i].data()), static_cast<uint64_t>(test_data[i].size())));
            }
            REQUIRE(test_block->count() == 0);
            REQUIRE(test_block->unique_indices_count() == 0);

            // close the file
            handle.reset();
        }
    }

    /*
    INFO("segment_tree: even blocks") {
        local_file_system_t fs = local_file_system_t();
        auto fname = testing_directory;
        fname /= "segtree_test_file_1";
        unique_ptr<file_handle_t> handle =
            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);

        segment_tree_t tree(std::pmr::get_default_resource(), std::move(handle));

        std::vector<dummy_alloc> test_data;
        for (size_t i = 0; i < 500; i += 2) {
            dummy_alloc dummy;
            dummy.id = i;
            dummy.size = DEFAULT_BLOCK_SIZE / 32;
            dummy.buffer = (data_ptr_t) (std::pmr::get_default_resource()->allocate(dummy.size));
            test_data.push_back(dummy);
        }
        for (size_t i = 1; i < 500; i += 2) {
            dummy_alloc dummy;
            dummy.id = i;
            dummy.size = DEFAULT_BLOCK_SIZE / 32;
            dummy.buffer = (data_ptr_t) (std::pmr::get_default_resource()->allocate(dummy.size));
            test_data.push_back(dummy);
        }

        REQUIRE(tree.blocks_count() == 0);
        REQUIRE(tree.count() == 0);

        for (uint64_t i = 0; i < 500; i++) {
            REQUIRE_FALSE(tree.contains_id(test_data[i].id));
            REQUIRE(tree.append(test_data[i].id, test_data[i].buffer, test_data[i].size));
            // test iterators
            uint64_t j = 0;
            for (auto block = tree.begin(); block != tree.end(); block++) {
                for (auto it = block->begin(); it != block->end(); it++) {
                    auto test_item = *std::find_if(test_data.begin(), test_data.end(), [it](const auto& item) {
                        return item.id == (*it).id;
                    });
                    REQUIRE((*it).id == test_item.id);
                    REQUIRE(test_item.size == (*it).items[0].second);
                    REQUIRE(memcmp(test_item.buffer, (*it).items[0].first, (*it).items[0].second) == 0);
                    j++;
                }
            }
            j = 0;
            for (auto block = tree.rbegin(); block != tree.rend(); block++) {
                for (auto it = block->rbegin(); it != block->rend(); it++) {
                    auto test_item = *std::find_if(test_data.begin(), test_data.end(), [it](const auto& item) {
                        return item.id == (*it).id;
                    });
                    REQUIRE((*it).id == test_item.id);
                    REQUIRE(test_item.size == (*it).items[0].second);
                    REQUIRE(memcmp(test_item.buffer, (*it).items[0].first, (*it).items[0].second) == 0);
                    j++;
                }
            }
            REQUIRE(tree.contains_id(test_data[i].id));
        }

        for (uint64_t i = 0; i < 500; i++) {
            auto item = tree.get_item(test_data[i].id, 0);
            REQUIRE(test_data[i].size == item.second);
            REQUIRE(memcmp(test_data[i].buffer, item.first, item.second) == 0);
        }

        REQUIRE(tree.count() == 500);

        tree.flush();
        tree.clean_load();

        REQUIRE(tree.count() == 500);

        for (uint64_t i = 0; i < 500; i++) {
            REQUIRE(tree.count() == 500 - i);
            REQUIRE(tree.contains_id(test_data[i].id));
            REQUIRE(tree.remove_id(test_data[i].id));
            REQUIRE_FALSE(tree.contains_id(test_data[i].id));
            REQUIRE(tree.count() == 500 - i - 1);
        }

        REQUIRE(tree.count() == 0);
        tree.clean_load();
        REQUIRE(tree.count() == 500); // should be at state of last flush

        for (uint64_t i = 0; i < 500; i++) {
            auto item = tree.get_item(test_data[i].id, 0);
            REQUIRE(test_data[i].size == item.second);
            REQUIRE(memcmp(test_data[i].buffer, item.first, item.second) == 0);
        }

        for (uint64_t i = 450; i < 500; i++) {
            REQUIRE(tree.contains_id(i));
            REQUIRE(tree.remove_id(i));
            REQUIRE_FALSE(tree.contains_id(i));
        }

        REQUIRE(tree.count() == 450);

        tree.flush();
        tree.clean_load();

        REQUIRE(tree.count() == 450);

        for (uint64_t i = 0; i < 450; i++) {
            REQUIRE(tree.contains_id(i));
            REQUIRE(tree.remove_id(i));
            REQUIRE_FALSE(tree.contains_id(i));
        }

        REQUIRE(tree.blocks_count() == 0);
        REQUIRE(tree.count() == 0);

        // try again but with lazy loading
        tree.lazy_load();

        REQUIRE(tree.count() == 450);

        for (uint64_t i = 0; i < 450; i++) {
            REQUIRE(tree.contains_id(i));
            REQUIRE(tree.remove_id(i));
            REQUIRE_FALSE(tree.contains_id(i));
        }

        REQUIRE(tree.blocks_count() == 0);
        REQUIRE(tree.count() == 0);

        for (uint64_t i = 0; i < 500; i++) {
            std::pmr::get_default_resource()->deallocate(test_data[i].buffer, test_data[i].size);
        }
    }

    INFO("segment_tree: uneven blocks") {
        local_file_system_t fs = local_file_system_t();
        auto fname = testing_directory;
        fname /= "segtree_test_file_2";
        unique_ptr<file_handle_t> handle =
            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);

        segment_tree_t tree(std::pmr::get_default_resource(), std::move(handle));

        std::vector<dummy_alloc> test_data;
        for (size_t i = 0; i < 500; i += 2) {
            dummy_alloc dummy;
            dummy.id = i;
            dummy.size = DEFAULT_BLOCK_SIZE / 32 * ((i % 50) + 1);
            dummy.buffer = (data_ptr_t) (std::pmr::get_default_resource()->allocate(dummy.size));
            test_data.push_back(dummy);
        }
        for (size_t i = 1; i < 500; i += 2) {
            dummy_alloc dummy;
            dummy.id = i;
            dummy.size = DEFAULT_BLOCK_SIZE / 32 * ((i % 50) + 1);
            dummy.buffer = (data_ptr_t) (std::pmr::get_default_resource()->allocate(dummy.size));
            test_data.push_back(dummy);
        }

        REQUIRE(tree.blocks_count() == 0);
        REQUIRE(tree.count() == 0);

        for (uint64_t i = 0; i < 500; i++) {
            REQUIRE_FALSE(tree.contains_id(test_data[i].id));
            REQUIRE(tree.append(test_data[i].id, test_data[i].buffer, test_data[i].size));
            REQUIRE(tree.contains_id(test_data[i].id));
        }

        for (uint64_t i = 0; i < 500; i++) {
            auto item = tree.get_item(test_data[i].id, 0);
            REQUIRE(test_data[i].size == item.second);
            REQUIRE(memcmp(test_data[i].buffer, item.first, item.second) == 0);
        }

        REQUIRE(tree.count() == 500);

        tree.flush();
        tree.clean_load();

        REQUIRE(tree.count() == 500);

        for (uint64_t i = 0; i < 500; i++) {
            REQUIRE(tree.count() == 500 - i);
            REQUIRE(tree.contains_id(test_data[i].id));
            REQUIRE(tree.remove_id(test_data[i].id));
            REQUIRE_FALSE(tree.contains_id(test_data[i].id));
            REQUIRE(tree.count() == 500 - i - 1);
        }

        REQUIRE(tree.count() == 0);
        tree.clean_load();
        REQUIRE(tree.count() == 500); // should be at state of last flush

        for (uint64_t i = 0; i < 500; i++) {
            auto item = tree.get_item(test_data[i].id, 0);
            REQUIRE(test_data[i].size == item.second);
            REQUIRE(memcmp(test_data[i].buffer, item.first, item.second) == 0);
        }

        for (uint64_t i = 450; i < 500; i++) {
            REQUIRE(tree.contains_id(i));
            REQUIRE(tree.remove_id(i));
            REQUIRE_FALSE(tree.contains_id(i));
        }

        REQUIRE(tree.count() == 450);

        tree.flush();
        tree.clean_load();

        REQUIRE(tree.count() == 450);

        for (uint64_t i = 0; i < 450; i++) {
            REQUIRE(tree.contains_id(i));
            REQUIRE(tree.remove_id(i));
            REQUIRE_FALSE(tree.contains_id(i));
        }

        REQUIRE(tree.blocks_count() == 0);
        REQUIRE(tree.count() == 0);

        // try again but with lazy loading
        tree.lazy_load();

        REQUIRE(tree.count() == 450);

        for (uint64_t i = 0; i < 450; i++) {
            REQUIRE(tree.contains_id(i));
            REQUIRE(tree.remove_id(i));
            REQUIRE_FALSE(tree.contains_id(i));
        }

        REQUIRE(tree.blocks_count() == 0);
        REQUIRE(tree.count() == 0);

        for (uint64_t i = 0; i < 500; i++) {
            std::pmr::get_default_resource()->deallocate(test_data[i].buffer, test_data[i].size);
        }
    }

    INFO("segment_tree: memory overflow") {
        limited_resource resource(DEFAULT_BLOCK_SIZE * 16);

        local_file_system_t fs = local_file_system_t();
        auto fname = testing_directory;
        fname /= "segtree_test_file_1";
        unique_ptr<file_handle_t> handle =
            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);

        segment_tree_t tree(&resource, std::move(handle));

        size_t dummy_size = DEFAULT_BLOCK_SIZE / 32;
        size_t test_count = 5000; // about x10 of what allocator can handle
        // just use one buffer
        uint64_t* buffer = (uint64_t*) (std::pmr::get_default_resource()->allocate(dummy_size));
        std::vector<uint64_t> test_data;
        test_data.reserve(test_count);
        for (uint64_t i = 0; i < test_count; i++) {
            test_data[i] = i;
        }
        std::shuffle(test_data.begin(), test_data.end(), std::default_random_engine{0});

        for (uint64_t i = 0; i < test_count; i++) {
            *buffer = test_data[i];
            REQUIRE(tree.count() == i);
            REQUIRE(tree.append(test_data[i], reinterpret_cast<data_ptr_t>(buffer), dummy_size));
            REQUIRE(tree.count() == i + 1);
        }

        for (uint64_t i = 0; i < test_count; i++) {
            REQUIRE(tree.contains_id(i));
        }

        tree.flush();
        try {
            // should fail, because there is not enough memory for it
            tree.clean_load();
        } catch (...) {
            // this will work
            tree.lazy_load();
        }

        for (uint64_t i = 0; i < test_count; i++) {
            REQUIRE(tree.remove_id(i));
        }

        std::pmr::get_default_resource()->deallocate(buffer, dummy_size);
    }

    INFO("b+tree: semirandom") {
        local_file_system_t fs = local_file_system_t();
        auto dname = testing_directory;
        dname /= "btree_test";

        btree_t tree(std::pmr::get_default_resource(), fs, dname, 12);

        std::vector<dummy_alloc> test_data;
        for (size_t i = 0; i < 500; i += 2) {
            dummy_alloc dummy;
            dummy.id = i;
            dummy.size = DEFAULT_BLOCK_SIZE / 32 * ((i % 50) + 1);
            dummy.buffer = (data_ptr_t) (std::pmr::get_default_resource()->allocate(dummy.size));
            test_data.push_back(dummy);
        }
        for (size_t i = 1; i < 500; i += 2) {
            dummy_alloc dummy;
            dummy.id = i;
            dummy.size = DEFAULT_BLOCK_SIZE / 32 * ((i % 50) + 1);
            dummy.buffer = (data_ptr_t) (std::pmr::get_default_resource()->allocate(dummy.size));
            test_data.push_back(dummy);
        }

        for (uint64_t i = 0; i < 500; i++) {
            REQUIRE_FALSE(tree.contains_id(test_data[i].id));
            REQUIRE(tree.append(test_data[i].id, test_data[i].buffer, test_data[i].size));
            REQUIRE(tree.contains_id(test_data[i].id));
            auto item = tree.get_item(test_data[i].id, 0);
            REQUIRE(test_data[i].size == item.second);
            REQUIRE(memcmp(test_data[i].buffer, item.first, item.second) == 0);

            // test scan
            std::vector<std::pair<uint64_t, std::string>> scan_result;
            tree.scan_ascending<std::string>(
                0,
                500,
                500,
                &scan_result,
                [](void* buf, uint64_t size) { return std::string(static_cast<char*>(buf), size); },
                [](const std::string&) { return true; });
            REQUIRE(scan_result.size() == i + 1);
            for (uint64_t j = 0; j < scan_result.size(); j++) {
                uint64_t id = scan_result[j].first;
                auto dummy = std::find_if(test_data.begin(), test_data.end(), [id](const dummy_alloc& dummy) {
                    return dummy.id == id;
                });
                REQUIRE(dummy != test_data.end());
                REQUIRE(dummy->size == scan_result[j].second.size());
                REQUIRE(memcmp(dummy->buffer, scan_result[j].second.data(), dummy->size) == 0);
            }
            scan_result.clear();
            tree.scan_decending<std::string>(
                0,
                500,
                500,
                &scan_result,
                [](void* buf, uint64_t size) { return std::string(static_cast<char*>(buf), size); },
                [](const std::string&) { return true; });
            REQUIRE(scan_result.size() == i + 1);
            for (uint64_t j = 0; j < scan_result.size(); j++) {
                uint64_t id = scan_result[j].first;
                auto dummy = std::find_if(test_data.begin(), test_data.end(), [id](const dummy_alloc& dummy) {
                    return dummy.id == id;
                });
                REQUIRE(dummy != test_data.end());
                REQUIRE(dummy->size == scan_result[j].second.size());
                REQUIRE(memcmp(dummy->buffer, scan_result[j].second.data(), dummy->size) == 0);
            }
        }
        REQUIRE(tree.size() == 500);

        tree.flush();

        REQUIRE(tree.size() == 500);

        for (uint64_t i = 0; i < 500; i++) {
            REQUIRE(tree.size() == 500 - i);
            REQUIRE(tree.contains_id(test_data[i].id));
            REQUIRE(tree.remove_id(test_data[i].id));
            REQUIRE_FALSE(tree.contains_id(test_data[i].id));
            REQUIRE(tree.size() == 500 - i - 1);
        }

        tree.load();

        REQUIRE(tree.size() == 500);
        // after loading, internal nodes will be different, but functionality shouldn't change
        for (uint64_t i = 0; i < 500; i++) {
            REQUIRE(tree.size() == 500 - i);
            REQUIRE(tree.contains_id(test_data[i].id));
            REQUIRE(tree.remove_id(test_data[i].id));
            REQUIRE_FALSE(tree.contains_id(test_data[i].id));
            REQUIRE(tree.size() == 500 - i - 1);
        }

        for (uint64_t i = 0; i < 500; i++) {
            std::pmr::get_default_resource()->deallocate(test_data[i].buffer, test_data[i].size);
        }
    }

    INFO("b+tree: big item count; random order") {
        size_t key_num = 100'000;
        local_file_system_t fs = local_file_system_t();
        auto dname = testing_directory;
        dname /= "btree_test1";
        btree_t tree(std::pmr::get_default_resource(), fs, dname, 2048);

        std::vector<uint64_t> keys;
        for (uint64_t i = 0; i < key_num; i++) {
            keys.emplace_back(i);
        }
        std::shuffle(keys.begin(), keys.end(), std::default_random_engine{0});

        REQUIRE(tree.size() == 0);

        for (uint64_t i = 0; i < key_num; i++) {
            REQUIRE(tree.append(keys[i], reinterpret_cast<data_ptr_t>(&keys[i]), sizeof(uint64_t)));
        }
        REQUIRE(tree.size() == key_num);
        for (uint64_t i = 0; i < key_num; i++) {
            REQUIRE(tree.contains_id(i));
            REQUIRE(*reinterpret_cast<uint64_t*>(tree.get_item(i, 0).first) == i);
        }
        REQUIRE(tree.size() == key_num);
        for (uint64_t i = 0; i < key_num; i++) {
            REQUIRE(tree.remove_id(keys[i]));
        }
        REQUIRE(tree.size() == 0);
    }

    INFO("b+tree: multithread access") {
        size_t num_threads = 4;
        size_t key_num = 100'000;
        local_file_system_t fs = local_file_system_t();
        auto dname = testing_directory;
        dname /= "btree_test2";
        btree_t tree(std::pmr::get_default_resource(), fs, dname, 2048);

        std::vector<uint64_t> keys;
        size_t work_per_thread = key_num / num_threads;
        for (uint64_t i = 0; i < key_num; i++) {
            keys.emplace_back(i);
        }
        std::shuffle(keys.begin(), keys.end(), std::default_random_engine{0});

        std::function<void(size_t)> append_func;
        append_func = [&tree, &keys, work_per_thread](size_t id) {
            size_t start = work_per_thread * id;
            size_t end = work_per_thread * (id + 1);

            for (size_t i = start; i < end; i++) {
                REQUIRE(tree.append(keys.at(i), reinterpret_cast<data_ptr_t>(&keys.at(i)), sizeof(uint64_t)));
            }
        };

        std::function<void(size_t)> get_func;
        get_func = [&tree, &keys, work_per_thread](size_t id) {
            size_t start = work_per_thread * id;
            size_t end = work_per_thread * (id + 1);

            for (size_t i = start; i < end; i++) {
                auto result_pair = tree.get_item(keys.at(i), 0);
                REQUIRE(result_pair.first);
                REQUIRE(result_pair.second == sizeof(uint64_t));
                REQUIRE(*reinterpret_cast<uint64_t*>(result_pair.first) == keys.at(i));
            }
        };

        std::function<void(size_t)> remove_func;
        remove_func = [&tree, &keys, work_per_thread](size_t id) {
            size_t start = work_per_thread * id;
            size_t end = work_per_thread * (id + 1);

            for (size_t i = start; i < end; i++) {
                REQUIRE(tree.remove_id(keys.at(i)));
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        // appends
        REQUIRE(tree.size() == 0);

        for (size_t i = 0; i < num_threads; i++) {
            threads.emplace_back(append_func, i);
        }
        for (size_t i = 0; i < num_threads; i++) {
            threads[i].join();
        }

        threads.clear();
        REQUIRE(tree.size() == key_num);

        {
            std::vector<std::pair<uint64_t, uint64_t>> scan_result;
            scan_result.reserve(key_num);
            tree.scan_ascending<uint64_t>(
                0,
                INVALID_ID,
                key_num * 2,
                &scan_result,
                [](void* buffer, size_t) { return *reinterpret_cast<uint64_t*>(buffer); },
                [](uint64_t) { return true; });
            REQUIRE(scan_result.size() == key_num);
            for (uint64_t i = 0; i < key_num; i++) {
                REQUIRE(i == scan_result[i].first);
            }
        }

        // gets

        for (size_t i = 0; i < num_threads; i++) {
            threads.emplace_back(get_func, i);
        }
        for (size_t i = 0; i < num_threads; i++) {
            threads[i].join();
        }

        threads.clear();

        // removals
        for (size_t i = 0; i < num_threads; i++) {
            threads.emplace_back(remove_func, i);
        }
        for (size_t i = 0; i < num_threads; i++) {
            threads[i].join();
        }

        REQUIRE(tree.size() == 0);
    }

    INFO("btree: non unique ids") {
        size_t fake_item_size = 8192;
        size_t duplicate_count = 50;
        size_t key_num = 4000;
        local_file_system_t fs = local_file_system_t();
        auto dname = testing_directory;
        dname /= "btree_test3";
        btree_t tree(std::pmr::get_default_resource(), fs, dname, 128);

        std::vector<std::pair<uint64_t, uint64_t>> test_data;
        test_data.reserve(key_num * duplicate_count);
        for (uint64_t i = 0; i < key_num; i++) {
            for (uint64_t j = 0; j < duplicate_count; j++) {
                test_data.emplace_back(std::pair<uint64_t, uint64_t>{i, j});
            }
        }
        std::shuffle(test_data.begin(), test_data.end(), std::default_random_engine{0});

        std::vector<size_t> duplicates(key_num, 0);
        uint64_t* fake_buffer = (uint64_t*) (std::pmr::get_default_resource()->allocate(fake_item_size));
        REQUIRE(tree.size() == 0);
        for (uint64_t i = 0; i < key_num * duplicate_count; i++) {
            *fake_buffer = test_data[i].second;
            REQUIRE(tree.append(test_data[i].first, reinterpret_cast<data_ptr_t>(fake_buffer), fake_item_size));
            REQUIRE(tree.contains_id(test_data[i].first));
            REQUIRE(tree.contains(test_data[i].first, reinterpret_cast<data_ptr_t>(fake_buffer), fake_item_size));
            duplicates[test_data[i].first]++;
            REQUIRE(tree.item_count(test_data[i].first) == duplicates[test_data[i].first]);
        }
        REQUIRE(tree.size() == key_num * duplicate_count);
        for (uint64_t i = 0; i < key_num; i++) {
            REQUIRE(tree.contains_id(i));
        }
        REQUIRE(tree.size() == key_num * duplicate_count);
        tree.flush();
        for (uint64_t i = 0; i < key_num; i++) {
            REQUIRE(tree.remove_id(i));
            for (uint64_t j = i + 1; j < key_num; j++) {
                REQUIRE(tree.contains_id(j));
            }
            REQUIRE(tree.size() == (key_num - i - 1) * duplicate_count);
        }
        REQUIRE(tree.size() == 0);
        tree.load();
        REQUIRE(tree.size() == key_num * duplicate_count);
        for (uint64_t i = 0; i < key_num * duplicate_count; i++) {
            *fake_buffer = test_data[i].second;
            REQUIRE(tree.remove(test_data[i].first, reinterpret_cast<data_ptr_t>(fake_buffer), fake_item_size));
        }
        REQUIRE(tree.size() == 0);

        std::pmr::get_default_resource()->deallocate(fake_buffer, fake_item_size);
    }

    INFO("deinitialization") {
        local_file_system_t fs = local_file_system_t();
        if (directory_exists(fs, testing_directory)) {
            remove_directory(fs, testing_directory);
        }
    }
    */
}