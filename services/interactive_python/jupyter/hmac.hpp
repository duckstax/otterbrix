#pragma once

#include <map>
#include <string>

#include <openssl/hmac.h>
#include <openssl/sha.h>

namespace services { namespace interactive_python { namespace jupyter {

    class hmac final {
    public:
        hmac(const std::string& scheme, const std::string& key);

        ~hmac();

        std::string sign(
            const std::string& header
            , const std::string& parent_header
            , const std::string& meta_data
            , const std::string& content);

        bool verify(
            const std::string& header
            , const std::string& parent_header
            , const std::string& meta_data
            , const std::string& content
            , const std::string& signature);

    private:
        const std::map<std::string, const EVP_MD* (*) ()> schemes = {
            {"hmac-md5", EVP_md5},
            {"hmac-sha1", EVP_sha1},
            // MDC2 is disabled by default unless enable-mdc2 is specified
            // {"hmac-mdc2", EVP_mdc2},
            {"hmac-ripemd160", EVP_ripemd160},
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
            {"hmac-blake2b512", EVP_blake2b512},
            {"hmac-blake2s256", EVP_blake2s256},
#endif
            {"hmac-sha224", EVP_sha224},
            {"hmac-sha256", EVP_sha256},
            {"hmac-sha384", EVP_sha384},
            {"hmac-sha512", EVP_sha512}};
        std::string key_;
        const EVP_MD* evp_;
        HMAC_CTX* hmac_;
    };
}}} // namespace components::python::detail