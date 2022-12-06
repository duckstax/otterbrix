#include "generaty.hpp"

document::retained_t<document::impl::array_t> gen_array(int num) {
    auto array = document::impl::mutable_array_t::new_array();
    for (int i = 0; i < 5; ++i) {
        array->append(num + i);
    }
    return array;
}

document::retained_t<document::impl::dict_t> gen_dict(int num) {
    auto dict = document::impl::mutable_dict_t::new_dict();
    dict->set(std::string_view {"odd"}, num % 2 != 0);
    dict->set(std::string_view {"even"}, num % 2 == 0);
    dict->set(std::string_view {"three"}, num % 3 == 0);
    dict->set(std::string_view {"five"}, num % 5 == 0);
    return dict;
}

std::string gen_id(int num) {
    auto res = std::to_string(num);
    while (res.size() < 24) {
        res = "0" + res;
    }
    return res;
}

document_ptr gen_doc(int num) {
    auto doc = document::impl::mutable_dict_t::new_dict();
    doc->set(std::string_view {"_id"}, gen_id(num));
    doc->set(std::string_view {"count"}, num);
    doc->set(std::string_view {"countStr"}, std::to_string(num));
    doc->set(std::string_view {"countDouble"}, float(num) + 0.1);
    doc->set(std::string_view {"countBool"}, num % 2 != 0);
    doc->set(std::string_view {"countArray"}, gen_array(num));
    doc->set(std::string_view {"countDict"}, gen_dict(num));
    auto array = document::impl::mutable_array_t::new_array();
    for (int i = 0; i < 5; ++i) {
        array->append(gen_array(num + i));
    }
    doc->set(std::string_view {"nestedArray"}, array);
    array = document::impl::mutable_array_t::new_array();
    for (int i = 0; i < 5; ++i) {
        auto dict = document::impl::mutable_dict_t::new_dict();
        dict->set(std::string_view {"number"}, num + i);
        array->append(dict);
    }
    doc->set(std::string_view {"dictArray"}, array);
    auto dict = document::impl::mutable_dict_t::new_dict();
    for (int i = 0; i < 5; ++i) {
        dict->set(std::to_string(num + i), gen_dict(num + i));
    }
    doc->set(std::string_view {"mixedDict"}, dict);
    return make_document(doc);
}
