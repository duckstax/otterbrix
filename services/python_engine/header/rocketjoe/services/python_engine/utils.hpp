#pragma once

#include "file_manager.hpp"

namespace rocketjoe { namespace services { namespace python_engine {

inline auto add_file_read(py::module &pyrocketjoe, file_manager* fm ){

    pyrocketjoe.def(
            "file_read",
            [fm](const std::string &path) -> std::map<std::size_t, std::string> {
                auto& file =  fm->open(path);
                return file.read();
            }
    );
}

}}}