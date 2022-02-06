#pragma once

namespace services::storage {

    struct database_create_result final {
        database_create_result(bool created)
            : created_(created) {}
        bool created_;
    };

    struct collection_create_result final {
        collection_create_result(bool created)
            : created_(created) {}
        bool created_;
    };

} // namespace services::storage