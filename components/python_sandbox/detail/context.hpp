#pragma once

#include <memory>
#include <list>
#include <unordered_map>

#include <boost/filesystem.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <detail/forward.hpp>

namespace rocketjoe { namespace services { namespace python_sandbox { namespace detail {

                class context final {
                public:
                    context(file_manager &file_manager);

                    auto text_file(const std::string &path) -> boost::intrusive_ptr<data_set>;

                    template <class DTYPE,class ...Args>
                    auto make_new_step(Args... args) {
                        boost::intrusive_ptr<DTYPE> ds (new DTYPE(std::forward<Args>(args)...));
                        pipeline_.push_back(ds);
                        return ds;
                    }

                private:
                    file_manager &file_manager_;
                    std::list<boost::intrusive_ptr<data_set>> pipeline_;
                };

}}}}
