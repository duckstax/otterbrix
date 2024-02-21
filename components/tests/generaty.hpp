#pragma once

#include <components/document/core/array.hpp>
#include <components/document/core/dict.hpp>
#include <components/document/document.hpp>

using namespace components::document;

document::retained_t<document::impl::array_t> gen_array(int num);
document::retained_t<document::impl::dict_t> gen_dict(int num);
std::string gen_id(int num);
document_ptr gen_doc(int num);
