#pragma once

#include <map>
#include <string>

#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <vector>

class hmac final {

    hmac(const std::string& scheme,const std::string& key):m_key(key) {
        m_evp = reinterpret_cast<EVP_MD*>(schemes.at(scheme));
#if OPENSSL_VERSION_NUMBER < 0x10100000L
        // OpenSSL 1.0.x
        m_hmac = new HMAC_CTX();
        HMAC_CTX_init(m_hmac);
#else
        m_hmac = HMAC_CTX_new();
#endif
    }

    ~hmac() {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
        // OpenSSL 1.0.x
        HMAC_CTX_cleanup(m_hmac);
#else
        HMAC_CTX_free(m_hmac);
#endif
    }

    std::string sign() {
        HMAC_Init_ex(m_hmac, m_key.c_str(), m_key.size(), m_evp, nullptr);

        HMAC_Update(m_hmac, header.data<const unsigned char>(), header.size());
        HMAC_Update(m_hmac, parent_header.data<const unsigned char>(), parent_header.size());
        HMAC_Update(m_hmac, meta_data.data<const unsigned char>(), meta_data.size());
        HMAC_Update(m_hmac, content.data<const unsigned char>(), content.size());

        auto sig = std::vector<unsigned char>(EVP_MD_size(m_evp));
        HMAC_Final(m_hmac, sig.data(), nullptr);

        std::string hex_sig = hex_string(sig);
        return hex_sig;
    }

    bool verify() {
        HMAC_Init_ex(m_hmac, m_key.c_str(), m_key.size(), m_evp, nullptr);

        HMAC_Update(m_hmac, header.data<const unsigned char>(), header.size());
        HMAC_Update(m_hmac, parent_header.data<const unsigned char>(), parent_header.size());
        HMAC_Update(m_hmac, meta_data.data<const unsigned char>(), meta_data.size());
        HMAC_Update(m_hmac, content.data<const unsigned char>(), content.size());

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