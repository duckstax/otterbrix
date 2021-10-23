#pragma once
#include <components/document/document.hpp>
#include <goblin-engineer/core.hpp>
#include <vector>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set_hook.hpp>

namespace components::cursor {

    class data_cursor_t {
    public:
        data_cursor_t() = default;
        data_cursor_t(std::vector<components::storage::document_t*> data)
            : data_(std::move(data)) {}
        size_t  size() const {
            return data_.size();
        }
    private:
        std::vector<storage::document_t*> data_;
    };

    class sub_cursor_t : public boost::intrusive::list_base_hook<> {
    public:
        sub_cursor_t() = default;
        sub_cursor_t(goblin_engineer::actor_address collection, data_cursor_t* data)
            : collection_(collection)
            , data_(data) {
        }
        components::storage::document_t* next() {
            return nullptr;
        }

        goblin_engineer::actor_address& address() {
            return collection_;
        }

        size_t size() const {
            return data_->size();
        }

    private:
        goblin_engineer::actor_address collection_;
        std::vector<components::storage::document_t*>::const_iterator it;
        data_cursor_t* data_;
    };

    class cursor_t {
    public:
        cursor_t() = default;

        void push(sub_cursor_t* sub_cursor) {
            size_+= sub_cursor->size();
            sub_cursor_.push_back(*sub_cursor);

        }
        components::storage::document_t* next() {
            if (it != sub_cursor_.cend()) {
            }
            return nullptr;
        }

        std::size_t size() const {
            return size_;
        }

    private:
        uint64_t  size_;
        boost::intrusive::list<sub_cursor_t>::const_iterator it;
        boost::intrusive::list<sub_cursor_t> sub_cursor_;
    };

} // namespace components::cursor