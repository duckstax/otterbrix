option(SANITIZE_ADDRESS "Enable AddressSanitizer for sanitized targets." Off)


if (SANITIZE_ADDRESS AND (SANITIZE_THREAD OR SANITIZE_MEMORY))
    message(FATAL_ERROR "AddressSanitizer is not compatible with ThreadSanitizer or MemorySanitizer.")
endif ()

if (SANITIZE_ADDRESS)

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fno-omit-frame-pointer -fsanitize=address")

endif ()
