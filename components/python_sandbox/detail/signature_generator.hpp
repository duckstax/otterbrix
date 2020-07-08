#pragma once

#include <cstddef>
#include <iomanip>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include <openssl/hmac.h>
#include <openssl/sha.h>

template<class B>
inline std::string hex_string(const B& buffer) {
    std::ostringstream oss;
    oss << std::hex;
    for (std::size_t i = 0; i < buffer.size(); ++i) {
        oss << std::setw(2) << std::setfill('0') << static_cast<int>(buffer[i]);
    }
    return oss.str();
}

class signature_generator final {
    signature_generator(const std::string& scheme, const std::string& key)
        : m_key(key) {
        m_evp = reinterpret_cast<EVP_MD*>(schemes.at(scheme));
#if OPENSSL_VERSION_NUMBER < 0x10100000L
        // OpenSSL 1.0.x
        m_hmac = new HMAC_CTX();
        HMAC_CTX_init(m_hmac);
#else
        m_hmac = HMAC_CTX_new();
#endif
    }

    ~signature_generator() {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
        // OpenSSL 1.0.x
        HMAC_CTX_cleanup(m_hmac);
#else
        HMAC_CTX_free(m_hmac);
#endif
    }

    std::string sign(
        const std::string& header
        , const std::string& parent_header
        , const std::string& meta_data
        , const std::string& content) {
        HMAC_Init_ex(m_hmac, m_key.c_str(), m_key.size(), m_evp, nullptr);

        HMAC_Update(m_hmac, reinterpret_cast<const unsigned char*>(header.c_str()), header.size());
        HMAC_Update(m_hmac, reinterpret_cast<const unsigned char*>(parent_header.c_str()), parent_header.size());
        HMAC_Update(m_hmac, reinterpret_cast<const unsigned char*>(meta_data.c_str()), meta_data.size());
        HMAC_Update(m_hmac, reinterpret_cast<const unsigned char*>(content.c_str()), content.size());

        auto sig = std::vector<unsigned char>(EVP_MD_size(m_evp));
        HMAC_Final(m_hmac, sig.data(), nullptr);

        std::string hex_sig = hex_string(sig);
        return hex_sig;
    }

    bool verify(
        const std::string& header
        , const std::string& parent_header
        , const std::string& meta_data
        , const std::string& content
        , const std::string& signature) {
        HMAC_Init_ex(m_hmac, m_key.c_str(), m_key.size(), m_evp, nullptr);

        HMAC_Update(m_hmac, reinterpret_cast<const unsigned char*>(header.c_str()), header.size());
        HMAC_Update(m_hmac, reinterpret_cast<const unsigned char*>(parent_header.c_str()), parent_header.size());
        HMAC_Update(m_hmac, reinterpret_cast<const unsigned char*>(meta_data.c_str()), meta_data.size());
        HMAC_Update(m_hmac, reinterpret_cast<const unsigned char*>(content.c_str()), content.size());

        auto sig = std::vector<unsigned char>(EVP_MD_size(m_evp));
        HMAC_Final(m_hmac, sig.data(), nullptr);

        std::string hex_sig = hex_string(sig);
        auto cmp = CRYPTO_memcmp(reinterpret_cast<const void*>(hex_sig.c_str()), signature.data(), hex_sig.size());
        return cmp == 0;
    }

private:
    std::map<std::string, const EVP_MD* (*) ()> schemes = {
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
    EVP_MD* m_evp;
    std::string m_key;
    HMAC_CTX* m_hmac;
};