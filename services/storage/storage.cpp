#include <actor-zeta/core.hpp>
#include "storage.hpp"

namespace services  {

storage_hub::storage_hub(actor_zeta::intrusive_ptr<actor_zeta::base::supervisor> ptr):goblin_engineer::abstract_service(ptr,"storage"){
    add_handler("put",&storage_hub::put);
    add_handler("get",&storage_hub::get);
}

void storage_hub::put(temporary_buffer_storage &data) {
    auto& storage =  storage_engine_.get_store("");
    storage.put(data);
}

void storage_hub::get(temporary_buffer_storage &/**/){
    auto& storage =  storage_engine_.get_store("");
    services::temporary_buffer_storage buffers;
    storage.get(buffers);
}

}
