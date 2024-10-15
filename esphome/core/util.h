#pragma once

#include <string>
namespace esphome {

/// Return whether the node has at least one client connected to the native API
bool api_is_connected();

/// Return whether the node has an active connection to a secondary controler
bool is_secondary_controller_connected();

/// DEPRECATED. Return whether the node has an active connection to an MQTT broker
inline bool mqtt_is_connected() { return is_secondary_controller_connected(); }

/// Return whether the node has any form of "remote" connection via the API or to an MQTT broker
bool remote_is_connected();

}  // namespace esphome
