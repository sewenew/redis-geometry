/**************************************************************************
   Copyright (c) 2020 sewenew

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

#include "redis_geometry.h"
#include <cassert>
#include <string>
#include "errors.h"
#include "commands.h"

namespace {

struct StringDeleter {
    void operator()(char *str) const {
        if (str != nullptr) {
            RedisModule_Free(str);
        }
    }
};

using StringUPtr = std::unique_ptr<char, StringDeleter>;

struct RdbString {
    StringUPtr str;
    std::size_t len;
};

RdbString rdb_load_string(RedisModuleIO *rdb);

std::pair<RdbString, RdbString> rdb_load_value(RedisModuleIO *rdb);

std::pair<std::string, std::string> serialize_geo_obj(void *value);

}

namespace sw {

namespace redis {

namespace geo {

RedisGeometry& RedisGeometry::instance() {
    static RedisGeometry redis_geometry;

    return redis_geometry;
}

void RedisGeometry::load(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    assert(ctx != nullptr);

    if (RedisModule_Init(ctx,
                module_name().data(),
                module_version(),
                REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        throw Error("fail to init module of " + module_name() + " type");
    }

    _options.load(argv, argc);

    RedisModuleTypeMethods methods = {
        REDISMODULE_TYPE_METHOD_VERSION,
        _rdb_load,
        _rdb_save,
        _aof_rewrite,
        nullptr,
        nullptr,
        _free_msg
    };

    _module_type = RedisModule_CreateDataType(ctx,
            type_name().data(),
            encoding_version(),
            &methods);
    if (_module_type == nullptr) {
        throw Error(std::string("failed to create ") + type_name() + " type");
    }

    cmd::create_commands(ctx);
}

void* RedisGeometry::_rdb_load(RedisModuleIO *rdb, int encver) {
    try {
        assert(rdb != nullptr);

        auto &module = RedisGeometry::instance();

        if (encver != module.encoding_version()) {
            throw Error("cannot load data of version: " + std::to_string(encver));
        }

        RdbString type_str;
        RdbString data_str;
        std::tie(type_str, data_str) = rdb_load_value(rdb);

        // TODO: parse data into geo obj, and return it.

        return nullptr;
    } catch (const Error &e) {
        RedisModule_LogIOError(rdb, "warning", e.what());
        return nullptr;
    }
}

void RedisGeometry::_rdb_save(RedisModuleIO *rdb, void *value) {
    try {
        assert(rdb != nullptr);

        std::string type;
        std::string buf;
        std::tie(type, buf) = serialize_geo_obj(value);

        RedisModule_SaveStringBuffer(rdb, type.data(), type.size());

        RedisModule_SaveStringBuffer(rdb, buf.data(), buf.size());
    } catch (const Error &e) {
        RedisModule_LogIOError(rdb, "warning", e.what());
    }
}

void RedisGeometry::_aof_rewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    try {
        assert(aof != nullptr);

        if (key == nullptr) {
            throw Error("null key to rewrite aof");
        }

        std::string type;
        std::string buf;
        std::tie(type, buf) = serialize_geo_obj(value);

        /*
        // TODO: call RedisModule_EmitAOF to write aof file.
        RedisModule_EmitAOF(aof,
                "PB.SET",
                "sbb",
                key,
                type.data(),
                type.size(),
                buf.data(),
                buf.size());
                */
    } catch (const Error &e) {
        RedisModule_LogIOError(aof, "warning", e.what());
    }
}

void RedisGeometry::_free_msg(void *value) {
    if (value != nullptr) {
        // TODO: free geo obj here
    }
}

}

}

}

namespace {

using sw::redis::geo::Error;

RdbString rdb_load_string(RedisModuleIO *rdb) {
    std::size_t len = 0;
    auto *buf = RedisModule_LoadStringBuffer(rdb, &len);
    if (buf == nullptr) {
        throw Error("failed to load string buffer from rdb");
    }

    return {StringUPtr(buf), len};
}

std::pair<RdbString, RdbString> rdb_load_value(RedisModuleIO *rdb) {
    auto type = rdb_load_string(rdb);

    auto data = rdb_load_string(rdb);

    return {std::move(type), std::move(data)};
}

std::pair<std::string, std::string> serialize_geo_obj(void *value) {
    if (value == nullptr) {
        throw Error("Null value to serialize");
    }

    std::string type;
    std::string data;
    // TODO: searialize value to type and data
    return std::make_pair(type, data);
}

}
