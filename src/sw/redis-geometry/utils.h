/**************************************************************************
   Copyright (c) 2017 sewenew

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 *************************************************************************/

#ifndef SEWENEW_REDISGEOMETRY_UTILS_H
#define SEWENEW_REDISGEOMETRY_UTILS_H

// Refer to https://gcc.gnu.org/onlinedocs/cpp/_005f_005fhas_005finclude.html
#if __cplusplus >= 201703L
#  if defined __has_include
#    if __has_include(<string_view>)
#      define REDIS_GEOMETRY_HAS_STRING_VIEW
#    endif
#    if __has_include(<optional>)
#      define REDIS_GEOMETRY_HAS_OPTIONAL
#    endif
#  endif
#endif

#include <cstring>
#include <string>
#include <type_traits>

#if defined REDIS_GEOMETRY_HAS_STRING_VIEW
#include <string_view>
#endif

#if defined REDIS_GEOMETRY_HAS_OPTIONAL
#include <optional>
#endif

namespace sw {

namespace redis {

namespace geo {

#if defined REDIS_GEOMETRY_HAS_STRING_VIEW

using StringView = std::string_view;

#else

// By now, not all compilers support std::string_view,
// so we make our own implementation.
class StringView {
public:
    constexpr StringView() noexcept = default;

    constexpr StringView(const char *data, std::size_t size) : _data(data), _size(size) {}

    StringView(const char *data) : _data(data), _size(std::strlen(data)) {}

    StringView(const std::string &str) : _data(str.data()), _size(str.size()) {}

    constexpr StringView(const StringView &) noexcept = default;

    StringView& operator=(const StringView &) noexcept = default;

    constexpr const char* data() const noexcept {
        return _data;
    }

    constexpr std::size_t size() const noexcept {
        return _size;
    }

private:
    const char *_data = nullptr;
    std::size_t _size = 0;
};

#endif

#if defined REDIS_GEOMETRY_HAS_OPTIONAL

template <typename T>
using Optional = std::optional<T>;

#else

template <typename T>
class Optional {
public:
    Optional() = default;

    Optional(const Optional &) = default;
    Optional& operator=(const Optional &) = default;

    Optional(Optional &&) = default;
    Optional& operator=(Optional &&) = default;

    ~Optional() = default;

    template <typename ...Args>
    explicit Optional(Args &&...args) : _value(true, T(std::forward<Args>(args)...)) {}

    explicit operator bool() const {
        return _value.first;
    }

    T& value() {
        return _value.second;
    }

    const T& value() const {
        return _value.second;
    }

    T* operator->() {
        return &(_value.second);
    }

    const T* operator->() const {
        return &(_value.second);
    }

    T& operator*() {
        return _value.second;
    }

    const T& operator*() const {
        return _value.second;
    }

private:
    std::pair<bool, T> _value;
};

#endif

using OptionalString = Optional<std::string>;

using OptionalLongLong = Optional<long long>;

using OptionalDouble = Optional<double>;

}

}

}

#endif // end SEWENEW_REDISGEOMETRY_UTILS_H
