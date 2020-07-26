#include "detail/hmac.hpp"

#include <cstddef>
#include <iomanip>
#include <ostream>
#include <vector>
#include <sstream>

namespace components { namespace python_sandbox { namespace detail {
    template<class B>
    std::string hex_string(const B& buffer) {
        std::ostringstream oss;
        oss << std::hex;
        for (std::size_t i = 0; i < buffer.size(); ++i) {
            oss << std::setw(2) << std::setfill('0') << static_cast<int>(buffer[i]);
        }
        return oss.str();
    }

    hmac::hmac(const std::string& scheme, const std::string& key)
        : key_(key)
        , evp_(schemes.at(scheme)()) {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
        // OpenSSL 1.0.x
        m_hmac = new HMAC_CTX();
        HMAC_CTX_init(m_hmac);
#else
        hmac_ = HMAC_CTX_new();
#endif
    }
    hmac::~hmac() {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
        // OpenSSL 1.0.x
        HMAC_CTX_cleanup(m_hmac);
#else
        HMAC_CTX_free(hmac_);
#endif
    }

    std::string hmac::sign(
        const std::string& header
        , const std::string& parent_header
        , const std::string& meta_data
        , const std::string& content) {
        HMAC_Init_ex(hmac_, key_.c_str(), static_cast<int>(key_.size()), evp_, nullptr);

        HMAC_Update(hmac_, reinterpret_cast<const unsigned char*>(header.c_str()), header.size());
        HMAC_Update(hmac_, reinterpret_cast<const unsigned char*>(parent_header.c_str()), parent_header.size());
        HMAC_Update(hmac_, reinterpret_cast<const unsigned char*>(meta_data.c_str()), meta_data.size());
        HMAC_Update(hmac_, reinterpret_cast<const unsigned char*>(content.c_str()), content.size());

        auto sig = std::vector<unsigned char>(static_cast<unsigned long>(EVP_MD_size(evp_)));
        HMAC_Final(hmac_, sig.data(), nullptr);

        std::string hex_sig = hex_string(sig);
        return hex_sig;
    }

    bool hmac::verify(
        const std::string& header
        , const std::string& parent_header
        , const std::string& meta_data
        , const std::string& content
        , const std::string& signature) {
        HMAC_Init_ex(hmac_, key_.c_str(), static_cast<int>(key_.size()), evp_, nullptr);

        HMAC_Update(hmac_, reinterpret_cast<const unsigned char*>(header.c_str()), header.size());
        HMAC_Update(hmac_, reinterpret_cast<const unsigned char*>(parent_header.c_str()), parent_header.size());
        HMAC_Update(hmac_, reinterpret_cast<const unsigned char*>(meta_data.c_str()), meta_data.size());
        HMAC_Update(hmac_, reinterpret_cast<const unsigned char*>(content.c_str()), content.size());

        auto sig = std::vector<unsigned char>(static_cast<unsigned long>(EVP_MD_size(evp_)));
        HMAC_Final(hmac_, sig.data(), nullptr);

        std::string hex_sig = hex_string(sig);
        auto cmp = CRYPTO_memcmp(reinterpret_cast<const void*>(hex_sig.c_str()), signature.data(), hex_sig.size());
        return cmp == 0;
    }
}}} // namespace components::python_sandbox::detail
