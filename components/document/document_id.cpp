#include "document_id.hpp"
#include <cassert>
#include <utility>

namespace components::document {

    document_id_t::document_id_t(const std::string &id)
        : super(id)
        , generator_(nullptr) {}

    document_id_t::document_id_t(std::string_view id)
        : super(std::string(id))
        , generator_(nullptr) {}

    document_id_t::document_id_t(document_id_t &&id) noexcept
        : super(id)
        , generator_(std::move(id.generator_)) {
    }

    document_id_t& document_id_t::operator=(const document_id_t &id) {
        super::operator=(id);
        generator_ = id.generator_;
        return *this;
    }

    document_id_t& document_id_t::operator=(document_id_t &&id) noexcept {
        super::operator=(id);
        generator_ = std::move(id.generator_);
        return *this;
    }

    document_id_t document_id_t::generate() {
        auto generator = std::make_shared<generator_t>();
        return document_id_t(generator->get(), generator);
    }

    document_id_t document_id_t::generate(time_value_t time) {
        auto generator = std::make_shared<generator_t>(time);
        return document_id_t(generator->get(), generator);
    }

    document_id_t document_id_t::null_id() {
        return document_id_t(null(), nullptr);
    }

    document_id_t::time_value_t document_id_t::get_timestamp() const {
        time_value_t value = 0;
        for (int i = 0; i < size_timestamp; ++i) {
            value = (value << 8) + static_cast<time_value_t>(static_cast<uint8_t>(*(data() + i)));
        }
        return value;
    }

    document_id_t document_id_t::next() const {
        assert(generator_ != nullptr);
        return document_id_t(generator_->next(), generator_);
    }

    bool document_id_t::is_null() const {
        return this->compare(null_id()) == 0;
    }

    document_id_t::document_id_t(const super &id, generator_ptr generator)
        : super(id)
        , generator_(std::move(generator)) {}

}
