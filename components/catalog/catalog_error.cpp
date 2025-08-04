#include "catalog_error.hpp"

namespace components::catalog {
    catalog_error::catalog_error()
        : schema_mistake_(catalog_mistake_t::NO_MISTAKE)
        , transaction_mistake_(transaction_mistake_t::NO_MISTAKE)
        , what_() {}

    catalog_error::catalog_error(catalog_mistake_t mistake, std::string what)
        : schema_mistake_(mistake)
        , transaction_mistake_(transaction_mistake_t::NO_MISTAKE)
        , what_(std::move(what)) {}

    catalog_error::catalog_error(transaction_mistake_t mistake, std::string what)
        : schema_mistake_(catalog_mistake_t::NO_MISTAKE)
        , transaction_mistake_(mistake)
        , what_(std::move(what)) {}

    catalog_error::operator bool() const {
        return schema_mistake_ != catalog_mistake_t::NO_MISTAKE ||
               transaction_mistake_ != transaction_mistake_t::NO_MISTAKE;
    }

    catalog_mistake_t catalog_error::schema_mistake() const { return schema_mistake_; }

    transaction_mistake_t catalog_error::transaction_mistake() const { return transaction_mistake_; }

    const std::string& catalog_error::what() const { return what_; }

} // namespace components::catalog
