#include "esphome/core/util.h"
#include "esphome/core/defines.h"
#include "esphome/core/application.h"
#include "esphome/core/version.h"
#include "esphome/core/log.h"

#ifdef USE_API
#include "esphome/components/api/api_server.h"
#endif

namespace esphome {

bool api_is_connected() {
#ifdef USE_API
  if (api::global_api_server != nullptr) {
    return api::global_api_server->is_connected();
  }
#endif
  return false;
}

#ifndef USE_SECONDARY_CONTROLER

bool is_secondary_controller_connected() { return false; }

#endif

bool remote_is_connected() {
  return api_is_connected()
#ifdef USE_SECONDARY_CONTROLER
         || is_secondary_controller_connected()
#endif
      ;
}

}  // namespace esphome
