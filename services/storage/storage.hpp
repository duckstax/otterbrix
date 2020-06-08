#pragma once

#include <goblin-engineer.hpp>

#include <components/log/log.hpp>

#include "storage_engine.hpp"

namespace services {

    class storage_hub final : public goblin_engineer::abstract_service {
    public:
        storage_hub(actor_zeta::intrusive_ptr<actor_zeta::supervisor> ptr);

        ~storage_hub() override = default;

        void put(temporary_buffer_storage&);

        void get(temporary_buffer_storage&);

    private:
        storage_engine storage_engine_;
    };

} // namespace services
