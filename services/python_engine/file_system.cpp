#include <rocketjoe/services/python_engine/file_system.hpp>

#include <pybind11/stl.h>

#include <rocketjoe/services/python_engine/file_manager.hpp>

namespace rocketjoe { namespace services { namespace python_engine {

auto add_file_system(py::module &pyrocketjoe, file_manager* fm ) -> void{

    pyrocketjoe.def(
            "file_read",
            [fm](const std::string &path) -> std::map<std::size_t, std::string> {
                std::map<std::size_t, std::string> tmp;
                auto* file =  fm->open(path);

                if(file != nullptr ){
                    tmp = file->read();
                }

                return tmp;
            }
    );
}

}}}