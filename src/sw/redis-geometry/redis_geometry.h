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

#ifndef SEWENEW_REDISGEOMETRY_REDIS_PROTOBUF_H
#define SEWENEW_REDISGEOMETRY_REDIS_PROTOBUF_H

#include "module_api.h"
#include "options.h"

namespace sw {

namespace redis {

namespace geo {

class RedisGeometry {
public:
    static RedisGeometry& instance();

    void load(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

    int module_version() const {
        return _MODULE_VERSION;
    }

    int encoding_version() const {
        return _ENCODING_VERSION;
    }

    const std::string& module_name() const {
        return _MODULE_NAME;
    }

    const std::string& type_name() const {
        return _TYPE_NAME;
    }

    RedisModuleType* type() {
        return _module_type;
    }

    const Options& options() const {
        return _options;
    }

private:
    RedisGeometry() = default;

    static void* _rdb_load(RedisModuleIO *rdb, int encver);

    static void _rdb_save(RedisModuleIO *rdb, void *value);

    static void _aof_rewrite(RedisModuleIO *aof, RedisModuleString *key, void *value);

    static void _free_msg(void *value);

    const int _MODULE_VERSION = 0;

    const int _ENCODING_VERSION = 0;

    const std::string _MODULE_NAME = "GEO";

    const std::string _TYPE_NAME = "GEOMET-SW";

    RedisModuleType *_module_type = nullptr;

    Options _options;
};

}

}

}

#endif // end SEWENEW_REDISGEOMETRY_REDIS_PROTOBUF_H
