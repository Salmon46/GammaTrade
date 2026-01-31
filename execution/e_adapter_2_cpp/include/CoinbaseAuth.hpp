#pragma once

#include <algorithm>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/ec.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/pem.h>
#include <openssl/sha.h>
#include <random>
#include <sstream>
#include <string>
#include <vector>
#include <spdlog/spdlog.h>

namespace gammatrade {
namespace utils {

class CoinbaseAuth {
public:
  static std::string generate_jwt(const std::string &key_name,
                                  const std::string &private_key_pem,
                                  const std::string &request_method,
                                  const std::string &request_path,
                                  long timestamp = 0) {
    // 1. Prepare Header
    std::string header_json =
        "{\"alg\":\"ES256\",\"kid\":\"" + key_name + "\"}";
    std::string header_b64 = base64_url_encode(header_json);

    // 2. Prepare Payload
    long now_sec;
    if (timestamp != 0) {
      now_sec = timestamp;
    } else {
      auto now = std::chrono::system_clock::now();
      now_sec = std::chrono::duration_cast<std::chrono::seconds>(
                    now.time_since_epoch())
                    .count();
    }
    std::string nonce = generate_nonce();

    // Strip query parameters from path for JWT URI claim
    std::string path_no_query = request_path;
    size_t query_pos = path_no_query.find('?');
    if (query_pos != std::string::npos) {
      path_no_query = path_no_query.substr(0, query_pos);
    }

    std::stringstream payload_ss;
    payload_ss << "{"
               << "\"iss\":\"coinbase-cloud\","
               << "\"nbf\":" << (now_sec - 60) << ","
               << "\"iat\":" << now_sec << ","
               << "\"exp\":" << (now_sec + 120) << ","
               << "\"sub\":\"" << key_name << "\","
               << "\"nonce\":\"" << nonce << "\","
               << "\"uri\":\"" << request_method << " " << "api.coinbase.com"
               << path_no_query << "\""
               << "}";
    std::string payload_json = payload_ss.str();

    std::string payload_b64 = base64_url_encode(payload_json);

    // 3. Sign
    std::string data_to_sign = header_b64 + "." + payload_b64;
    std::string signature = sign_ecdsa(data_to_sign, private_key_pem);

    return header_b64 + "." + payload_b64 + "." + signature;
  }

private:
  static std::string base64_url_encode(const std::string &data) {
    BIO *bio, *b64;
    BUF_MEM *bufferPtr;

    b64 = BIO_new(BIO_f_base64());
    bio = BIO_new(BIO_s_mem());
    bio = BIO_push(b64, bio);

    BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
    BIO_write(bio, data.c_str(), data.length());
    BIO_flush(bio);
    BIO_get_mem_ptr(bio, &bufferPtr);

    std::string encoded(bufferPtr->data, bufferPtr->length);
    BIO_free_all(bio);

    // Convert to Base64Url
    std::replace(encoded.begin(), encoded.end(), '+', '-');
    std::replace(encoded.begin(), encoded.end(), '/', '_');
    encoded.erase(std::remove(encoded.begin(), encoded.end(), '='),
                  encoded.end());

    return encoded;
  }

  static std::string sign_ecdsa(const std::string &data,
                                const std::string &private_key_pem) {
    BIO *bio = BIO_new_mem_buf(private_key_pem.c_str(), -1);
    EVP_PKEY *pkey = PEM_read_bio_PrivateKey(bio, NULL, NULL, NULL);
    BIO_free(bio);

    if (!pkey) {
      unsigned long err = ERR_get_error();
      char err_buf[256];
      ERR_error_string_n(err, err_buf, sizeof(err_buf));
      spdlog::error("CoinbaseAuth: Failed to load private key. OpenSSL Error: {}",
                err_buf);
      return "";
    }

    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestSignInit(ctx, NULL, EVP_sha256(), NULL, pkey);
    EVP_DigestSignUpdate(ctx, data.c_str(), data.length());

    size_t len = 0;
    EVP_DigestSignFinal(ctx, NULL, &len);
    std::vector<unsigned char> signature(len);
    EVP_DigestSignFinal(ctx, signature.data(), &len);

    EVP_MD_CTX_free(ctx);
    EVP_PKEY_free(pkey);

    // Convert DER to Raw (R|S)
    const unsigned char *p = signature.data();
    ECDSA_SIG *sig = d2i_ECDSA_SIG(NULL, &p, len);
    if (!sig) {
      spdlog::error("CoinbaseAuth: Failed to decode ECDSA signature");
      return "";
    }

    const BIGNUM *r = NULL;
    const BIGNUM *s = NULL;
    ECDSA_SIG_get0(sig, &r, &s);

    int r_len = BN_num_bytes(r);
    int s_len = BN_num_bytes(s);
    int size = 32; // P-256

    std::vector<unsigned char> raw_sig(2 * size, 0);

    // Copy R (padded)
    if (r_len <= size)
      BN_bn2bin(r, raw_sig.data() + (size - r_len));
    else
      BN_bn2bin(r, raw_sig.data());

    // Copy S (padded)
    if (s_len <= size)
      BN_bn2bin(s, raw_sig.data() + size + (size - s_len));
    else
      BN_bn2bin(s, raw_sig.data() + size);

    ECDSA_SIG_free(sig);

    std::string signature_str(reinterpret_cast<char *>(raw_sig.data()),
                              raw_sig.size());
    return base64_url_encode(signature_str);
  }

  static std::string generate_nonce() {
    static const char alphanum[] = "0123456789"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "abcdefghijklmnopqrstuvwxyz";
    std::string tmp_s;
    tmp_s.reserve(16);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, sizeof(alphanum) - 2);

    for (int i = 0; i < 16; ++i) {
      tmp_s += alphanum[dis(gen)];
    }
    return tmp_s;
  }
};

} // namespace utils
} // namespace gammatrade
