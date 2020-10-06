#include <detail/file_system.hpp>

#include <pybind11/stl.h>

#include <detail/file_manager.hpp>

namespace components { namespace python { namespace detail {

    auto add_file_system(py::module& pyrocketjoe, file_manager* fm) -> void {
        pyrocketjoe.def(
            "file_read",
            [fm](const std::string& path) -> std::unordered_map<std::size_t, std::string> {
                std::unordered_map<std::size_t, std::string> tmp;
                auto* file = fm->open(path);

                if (file != nullptr) {
                    tmp = file->read();
                }

                return tmp;
            });
    }

}}} // namespace components::python::detail