#pragma once

#include <components/document/document.hpp>

using namespace components::document;

void gen_array(int num, const document_ptr& array);
void gen_dict(int num, const document_ptr& dict);
std::pmr::string gen_id(int num, std::pmr::memory_resource* resource);
document_ptr gen_doc(int num, std::pmr::memory_resource* resource);
