#include "select.hpp"

select_t::select_t(const std::string& nameTable, const std::vector<std::string>& keys)
: name_table_(nameTable)
, keys_(keys) {}