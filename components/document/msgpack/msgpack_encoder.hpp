#pragma once

#include <msgpack.hpp>
#include <components/document/document.hpp>


// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<components::document::document_t> final {
                msgpack::object const& operator()(msgpack::object const& o, components::document::document_ptr& v) const {
                    if (o.type != msgpack::type::ARRAY) {
                        throw msgpack::type_error();
                    }

                    v = std::move( components::document::document_ptr());
                    return o;
                }
            };

            template<>
            struct pack<components::document::document_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, components::document::document_ptr const& v) const {
                    o.pack_array(2);

                    return o;
                }
            };

            template<>
            struct object_with_zone<components::document::document_t> final {
                void operator()(msgpack::object::with_zone& o, components::document::document_t const& v) const {
                    o.type = type::MAP;

                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack