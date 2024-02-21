#pragma once

#ifndef TRACY_ENABLE

#define ZoneScoped
#define ZoneScopedN(txt)
#define TracySecureAlloc(ptr, count)
#define TracySecureFree(ptr)
#define TracyFrame(data, name)
#define TracyFrameStart(name)
#define TracyFrameEnd(name)
#define TracyMessage(msg, size)
#define FrameMark

#else

#include "Tracy.hpp"

#ifdef TRACY_MEM_PROFILE

inline void* operator new(std::size_t count) {
    auto ptr = malloc(count);
    TracySecureAlloc(ptr, count);
    return ptr;
}

inline void operator delete(void* ptr) noexcept {
    TracySecureFree(ptr);
    free(ptr);
}

inline void operator delete(void* ptr, std::size_t) noexcept {
    TracySecureFree(ptr);
    free(ptr);
}

#endif

#define TracyFrame(cv_mat, name)                                                                                       \
    {                                                                                                                  \
        if (cv_mat.data) {                                                                                             \
            auto w = cv_mat.cols;                                                                                      \
            auto h = cv_mat.rows;                                                                                      \
            std::vector<uint8_t> data(cv_mat.total() * 4);                                                             \
            cv::Mat continuousRGBA(cv_mat.size(), CV_8UC4, data.data());                                               \
            cv::cvtColor(cv_mat, continuousRGBA, cv::COLOR_BGR2RGBA, 4);                                               \
            FrameImage(data.data(), w, h, 0, 0);                                                                       \
            FrameMark;                                                                                                 \
        }                                                                                                              \
    }

#define TracyFrameStart(name) FrameMarkStart(name)
#define TracyFrameEnd(name) FrameMarkEnd(name)

#endif
