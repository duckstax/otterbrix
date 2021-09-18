#pragma once

#include <string>
#include <thread>

#include <boost/filesystem.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <pybind11/embed.h>

#include "detail/forward.hpp"
#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>

namespace components {

    namespace py = pybind11;

    class BOOST_SYMBOL_VISIBLE python_t final {
    public:
        python_t() = delete;
        python_t(const python_t&) = delete;
        python_t& operator=(const python_t&) = delete;

        python_t(const components::python_sandbox_configuration&, components::log_t&);

        ~python_t();

        void run_script(const std::vector<std::string>&);

        void call_task(const std::string& task, const std::string& body);

        auto init() -> void;

        auto module(const std::string&/**/) -> py::module& {
            return pyrocketjoe_;
        }

    private:
        boost::filesystem::path script_path_;
        components::sandbox_mode_t mode_;
        py::scoped_interpreter python_;
        py::module pyrocketjoe_;
        std::unique_ptr<python::detail::file_manager> file_manager_;
        std::unique_ptr<python::detail::context_manager> context_manager_;
        components::log_t log_;
    };

    template<typename Module>
    auto load(python_t&vm ,Module&module) -> void {
        module.load(vm);
    }

} // namespace components
