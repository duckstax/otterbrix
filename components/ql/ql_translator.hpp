#pragma once

#include <memory>

class result_translator {

};

using result_translator_ptr = std::unique_ptr<result_translator>;

auto ql_translator() -> result_translator_ptr {

};
