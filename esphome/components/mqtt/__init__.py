import re

from esphome import automation, controler
from esphome.automation import Condition
import esphome.codegen as cg
from esphome.components import logger
from esphome.components.esp32 import add_idf_sdkconfig_option
import esphome.config_validation as cv
from esphome.const import (
    CONF_ACTION_STATE_TOPIC,
    CONF_AVAILABILITY,
    CONF_AWAY_COMMAND_TOPIC,
    CONF_AWAY_STATE_TOPIC,
    CONF_BIRTH_MESSAGE,
    CONF_BROKER,
    CONF_CERTIFICATE_AUTHORITY,
    CONF_CLEAN_SESSION,
    CONF_CLIENT_CERTIFICATE,
    CONF_CLIENT_CERTIFICATE_KEY,
    CONF_CLIENT_ID,
    CONF_COMMAND_RETAIN,
    CONF_COMMAND_TOPIC,
    CONF_CURRENT_HUMIDITY_STATE_TOPIC,
    CONF_CURRENT_TEMPERATURE_STATE_TOPIC,
    CONF_DISCOVERY,
    CONF_DISCOVERY_OBJECT_ID_GENERATOR,
    CONF_DISCOVERY_PREFIX,
    CONF_DISCOVERY_RETAIN,
    CONF_DISCOVERY_UNIQUE_ID_GENERATOR,
    CONF_EXPIRE_AFTER,
    CONF_FAN_MODE_COMMAND_TOPIC,
    CONF_FAN_MODE_STATE_TOPIC,
    CONF_ID,
    CONF_KEEPALIVE,
    CONF_LEVEL,
    CONF_LOG_TOPIC,
    CONF_MODE_COMMAND_TOPIC,
    CONF_MODE_STATE_TOPIC,
    CONF_ON_CONNECT,
    CONF_ON_DISCONNECT,
    CONF_ON_JSON_MESSAGE,
    CONF_ON_MESSAGE,
    CONF_OSCILLATION_COMMAND_TOPIC,
    CONF_OSCILLATION_STATE_TOPIC,
    CONF_PASSWORD,
    CONF_PAYLOAD,
    CONF_PAYLOAD_AVAILABLE,
    CONF_PAYLOAD_NOT_AVAILABLE,
    CONF_PORT,
    CONF_POSITION_COMMAND_TOPIC,
    CONF_POSITION_STATE_TOPIC,
    CONF_PRESET_COMMAND_TOPIC,
    CONF_PRESET_STATE_TOPIC,
    CONF_QOS,
    CONF_REBOOT_TIMEOUT,
    CONF_RETAIN,
    CONF_SHUTDOWN_MESSAGE,
    CONF_SPEED_COMMAND_TOPIC,
    CONF_SPEED_LEVEL_COMMAND_TOPIC,
    CONF_SPEED_LEVEL_STATE_TOPIC,
    CONF_SPEED_STATE_TOPIC,
    CONF_SSL_FINGERPRINTS,
    CONF_STATE_TOPIC,
    CONF_SWING_MODE_COMMAND_TOPIC,
    CONF_SWING_MODE_STATE_TOPIC,
    CONF_TARGET_HUMIDITY_COMMAND_TOPIC,
    CONF_TARGET_HUMIDITY_STATE_TOPIC,
    CONF_TARGET_TEMPERATURE_COMMAND_TOPIC,
    CONF_TARGET_TEMPERATURE_HIGH_COMMAND_TOPIC,
    CONF_TARGET_TEMPERATURE_HIGH_STATE_TOPIC,
    CONF_TARGET_TEMPERATURE_LOW_COMMAND_TOPIC,
    CONF_TARGET_TEMPERATURE_LOW_STATE_TOPIC,
    CONF_TARGET_TEMPERATURE_STATE_TOPIC,
    CONF_TILT_COMMAND_TOPIC,
    CONF_TILT_STATE_TOPIC,
    CONF_TOPIC,
    CONF_TOPIC_PREFIX,
    CONF_TRIGGER_ID,
    CONF_USE_ABBREVIATIONS,
    CONF_USERNAME,
    CONF_WILL_MESSAGE,
    PLATFORM_BK72XX,
    PLATFORM_ESP32,
    PLATFORM_ESP8266,
)
from esphome.controler import ComponentType
from esphome.core import CORE, coroutine_with_priority

DEPENDENCIES = ["network"]
_UNDEF = object()


def AUTO_LOAD():
    if CORE.is_esp8266 or CORE.is_libretiny:
        return ["async_tcp", "json"]
    return ["json"]


CONF_DISCOVER_IP = "discover_ip"
CONF_IDF_SEND_ASYNC = "idf_send_async"
CONF_SKIP_CERT_CN_CHECK = "skip_cert_cn_check"


def _valid_topic(value):
    """Validate that this is a valid topic name/filter."""
    if value is None:  # Used to disable publishing and subscribing
        return ""
    if isinstance(value, dict):
        raise cv.Invalid("Can't use dictionary with topic")
    value = cv.string(value)
    try:
        raw_value = value.encode("utf-8")
    except UnicodeError as err:
        raise cv.Invalid("MQTT topic name/filter must be valid UTF-8 string.") from err
    if not raw_value:
        raise cv.Invalid("MQTT topic name/filter must not be empty.")
    if len(raw_value) > 65535:
        raise cv.Invalid(
            "MQTT topic name/filter must not be longer than 65535 encoded bytes."
        )
    if "\0" in value:
        raise cv.Invalid("MQTT topic name/filter must not contain null character.")
    return value


def subscribe_topic(value):
    """Validate that we can subscribe using this MQTT topic."""
    value = _valid_topic(value)
    for i in (i for i, c in enumerate(value) if c == "+"):
        if (i > 0 and value[i - 1] != "/") or (
            i < len(value) - 1 and value[i + 1] != "/"
        ):
            raise cv.Invalid(
                "Single-level wildcard must occupy an entire level of the filter"
            )

    index = value.find("#")
    if index != -1:
        if index != len(value) - 1:
            # If there are multiple wildcards, this will also trigger
            raise cv.Invalid(
                "Multi-level wildcard must be the last "
                "character in the topic filter."
            )
        if len(value) > 1 and value[index - 1] != "/":
            raise cv.Invalid(
                "Multi-level wildcard must be after a topic level separator."
            )

    return value


def publish_topic(value):
    """Validate that we can publish using this MQTT topic."""
    value = _valid_topic(value)
    if "+" in value or "#" in value:
        raise cv.Invalid("Wildcards can not be used in topic names")
    return value


def mqtt_payload(value):
    if value is None:
        return ""
    return cv.string(value)


def mqtt_qos(value):
    try:
        value = int(value)
    except (TypeError, ValueError):
        # pylint: disable=raise-missing-from
        raise cv.Invalid(f"MQTT Quality of Service must be integer, got {value}")
    return cv.one_of(0, 1, 2)(value)


def validate_message_just_topic(value):
    value = publish_topic(value)
    return MQTT_MESSAGE_BASE({CONF_TOPIC: value})


MQTT_MESSAGE_BASE = cv.Schema(
    {
        cv.Required(CONF_TOPIC): publish_topic,
        cv.Optional(CONF_QOS, default=0): mqtt_qos,
        cv.Optional(CONF_RETAIN, default=True): cv.boolean,
    }
)

MQTT_MESSAGE_TEMPLATE_SCHEMA = cv.Any(
    None, MQTT_MESSAGE_BASE, validate_message_just_topic
)

MQTT_MESSAGE_SCHEMA = cv.Any(
    None,
    MQTT_MESSAGE_BASE.extend(
        {
            cv.Required(CONF_PAYLOAD): mqtt_payload,
        }
    ),
)


MQTT_COMPONENT_AVAILABILITY_SCHEMA = cv.Schema(
    {
        cv.Required(CONF_TOPIC): subscribe_topic,
        cv.Optional(CONF_PAYLOAD_AVAILABLE, default="online"): mqtt_payload,
        cv.Optional(CONF_PAYLOAD_NOT_AVAILABLE, default="offline"): mqtt_payload,
    }
)

MQTT_COMPONENT_SCHEMA = cv.Schema(
    {
        cv.Optional(CONF_QOS): cv.All(
            cv.requires_component("mqtt"), cv.int_range(min=0, max=2)
        ),
        cv.Optional(CONF_RETAIN): cv.All(cv.requires_component("mqtt"), cv.boolean),
        cv.Optional(CONF_DISCOVERY): cv.All(cv.requires_component("mqtt"), cv.boolean),
        cv.Optional(CONF_STATE_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_AVAILABILITY): cv.All(
            cv.requires_component("mqtt"),
            cv.Any(None, MQTT_COMPONENT_AVAILABILITY_SCHEMA),
        ),
    }
)

MQTT_COMMAND_COMPONENT_SCHEMA = MQTT_COMPONENT_SCHEMA.extend(
    {
        cv.Optional(CONF_COMMAND_TOPIC): cv.All(
            cv.requires_component("mqtt"), subscribe_topic
        ),
        cv.Optional(CONF_COMMAND_RETAIN): cv.All(
            cv.requires_component("mqtt"), cv.boolean
        ),
    }
)

mqtt_ns = cg.esphome_ns.namespace("mqtt")
MQTTMessage = mqtt_ns.struct("MQTTMessage")
MQTTClientComponent = mqtt_ns.class_("MQTTClientComponent", cg.Component)
MQTTPublishAction = mqtt_ns.class_("MQTTPublishAction", automation.Action)
MQTTPublishJsonAction = mqtt_ns.class_("MQTTPublishJsonAction", automation.Action)
MQTTMessageTrigger = mqtt_ns.class_(
    "MQTTMessageTrigger", automation.Trigger.template(cg.std_string), cg.Component
)
MQTTJsonMessageTrigger = mqtt_ns.class_(
    "MQTTJsonMessageTrigger", automation.Trigger.template(cg.JsonObjectConst)
)
MQTTConnectTrigger = mqtt_ns.class_("MQTTConnectTrigger", automation.Trigger.template())
MQTTDisconnectTrigger = mqtt_ns.class_(
    "MQTTDisconnectTrigger", automation.Trigger.template()
)
MQTTComponent = mqtt_ns.class_("MQTTComponent", cg.Component)
MQTTConnectedCondition = mqtt_ns.class_("MQTTConnectedCondition", Condition)

MQTTAlarmControlPanelComponent = mqtt_ns.class_(
    "MQTTAlarmControlPanelComponent", MQTTComponent
)
MQTTBinarySensorComponent = mqtt_ns.class_("MQTTBinarySensorComponent", MQTTComponent)
MQTTClimateComponent = mqtt_ns.class_("MQTTClimateComponent", MQTTComponent)
MQTTCoverComponent = mqtt_ns.class_("MQTTCoverComponent", MQTTComponent)
MQTTFanComponent = mqtt_ns.class_("MQTTFanComponent", MQTTComponent)
MQTTJSONLightComponent = mqtt_ns.class_("MQTTJSONLightComponent", MQTTComponent)
MQTTSensorComponent = mqtt_ns.class_("MQTTSensorComponent", MQTTComponent)
MQTTSwitchComponent = mqtt_ns.class_("MQTTSwitchComponent", MQTTComponent)
MQTTTextSensor = mqtt_ns.class_("MQTTTextSensor", MQTTComponent)
MQTTNumberComponent = mqtt_ns.class_("MQTTNumberComponent", MQTTComponent)
MQTTDateComponent = mqtt_ns.class_("MQTTDateComponent", MQTTComponent)
MQTTTimeComponent = mqtt_ns.class_("MQTTTimeComponent", MQTTComponent)
MQTTDateTimeComponent = mqtt_ns.class_("MQTTDateTimeComponent", MQTTComponent)
MQTTTextComponent = mqtt_ns.class_("MQTTTextComponent", MQTTComponent)
MQTTSelectComponent = mqtt_ns.class_("MQTTSelectComponent", MQTTComponent)
MQTTButtonComponent = mqtt_ns.class_("MQTTButtonComponent", MQTTComponent)
MQTTLockComponent = mqtt_ns.class_("MQTTLockComponent", MQTTComponent)
MQTTEventComponent = mqtt_ns.class_("MQTTEventComponent", MQTTComponent)
MQTTUpdateComponent = mqtt_ns.class_("MQTTUpdateComponent", MQTTComponent)
MQTTValveComponent = mqtt_ns.class_("MQTTValveComponent", MQTTComponent)

MQTTDiscoveryUniqueIdGenerator = mqtt_ns.enum("MQTTDiscoveryUniqueIdGenerator")
MQTT_DISCOVERY_UNIQUE_ID_GENERATOR_OPTIONS = {
    "legacy": MQTTDiscoveryUniqueIdGenerator.MQTT_LEGACY_UNIQUE_ID_GENERATOR,
    "mac": MQTTDiscoveryUniqueIdGenerator.MQTT_MAC_ADDRESS_UNIQUE_ID_GENERATOR,
}

MQTTDiscoveryObjectIdGenerator = mqtt_ns.enum("MQTTDiscoveryObjectIdGenerator")
MQTT_DISCOVERY_OBJECT_ID_GENERATOR_OPTIONS = {
    "none": MQTTDiscoveryObjectIdGenerator.MQTT_NONE_OBJECT_ID_GENERATOR,
    "device_name": MQTTDiscoveryObjectIdGenerator.MQTT_DEVICE_NAME_OBJECT_ID_GENERATOR,
}


class MqttController(controler.BaseControler):
    CONF_MQTT_ID = "mqtt_id"

    def __init__(self):
        pass

    CUSTOM_SCHEMA_VALVE = {
        cv.Optional(CONF_POSITION_COMMAND_TOPIC): cv.All(
            cv.requires_component("mqtt"), subscribe_topic
        ),
        cv.Optional(CONF_POSITION_STATE_TOPIC): cv.All(
            cv.requires_component("mqtt"), subscribe_topic
        ),
    }

    CUSTOM_SCHEMA_SENSOR = {
        cv.Optional(CONF_EXPIRE_AFTER): cv.All(
            cv.requires_component("mqtt"),
            cv.Any(None, cv.positive_time_period_milliseconds),
        ),
    }

    CUSTOM_SCHEMA_FAN = {
        cv.Optional(CONF_OSCILLATION_STATE_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_OSCILLATION_COMMAND_TOPIC): cv.All(
            cv.requires_component("mqtt"), subscribe_topic
        ),
        cv.Optional(CONF_SPEED_LEVEL_STATE_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_SPEED_LEVEL_COMMAND_TOPIC): cv.All(
            cv.requires_component("mqtt"), subscribe_topic
        ),
        cv.Optional(CONF_SPEED_STATE_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_SPEED_COMMAND_TOPIC): cv.All(
            cv.requires_component("mqtt"), subscribe_topic
        ),
    }

    CUSTOM_SCHEMA_COVER = {
        cv.Optional(CONF_POSITION_COMMAND_TOPIC): cv.All(
            cv.requires_component("mqtt"), subscribe_topic
        ),
        cv.Optional(CONF_POSITION_STATE_TOPIC): cv.All(
            cv.requires_component("mqtt"), subscribe_topic
        ),
        cv.Optional(CONF_TILT_COMMAND_TOPIC): cv.All(
            cv.requires_component("mqtt"), subscribe_topic
        ),
        cv.Optional(CONF_TILT_STATE_TOPIC): cv.All(
            cv.requires_component("mqtt"), subscribe_topic
        ),
    }

    CUSTOM_SCHEMA_CLIMATE = {
        cv.Optional(CONF_ACTION_STATE_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_AWAY_COMMAND_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_AWAY_STATE_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_CURRENT_TEMPERATURE_STATE_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_CURRENT_HUMIDITY_STATE_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_FAN_MODE_COMMAND_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_FAN_MODE_STATE_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_MODE_COMMAND_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_MODE_STATE_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_PRESET_COMMAND_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_PRESET_STATE_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_SWING_MODE_COMMAND_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_SWING_MODE_STATE_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_TARGET_TEMPERATURE_COMMAND_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_TARGET_TEMPERATURE_STATE_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_TARGET_TEMPERATURE_HIGH_COMMAND_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_TARGET_TEMPERATURE_HIGH_STATE_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_TARGET_TEMPERATURE_LOW_COMMAND_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_TARGET_TEMPERATURE_LOW_STATE_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_TARGET_HUMIDITY_COMMAND_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
        cv.Optional(CONF_TARGET_HUMIDITY_STATE_TOPIC): cv.All(
            cv.requires_component("mqtt"), publish_topic
        ),
    }

    def extend_component_schema(self, component: ComponentType, schema):
        component_class = {
            ComponentType.switch: MQTTSwitchComponent,
            ComponentType.sensor: MQTTSensorComponent,
            ComponentType.number: MQTTNumberComponent,
            ComponentType.alarm_control_panel: MQTTAlarmControlPanelComponent,
            ComponentType.binary_sensoor: MQTTBinarySensorComponent,
            ComponentType.button: MQTTButtonComponent,
            ComponentType.climate: MQTTClimateComponent,
            ComponentType.cover: MQTTCoverComponent,
            ComponentType.date: MQTTDateComponent,
            ComponentType.time: MQTTTimeComponent,
            ComponentType.date_time: MQTTDateTimeComponent,
            ComponentType.event: MQTTEventComponent,
            ComponentType.fan: MQTTFanComponent,
            ComponentType.light: MQTTJSONLightComponent,
            ComponentType.lock: MQTTLockComponent,
            ComponentType.select: MQTTSelectComponent,
            ComponentType.text: MQTTTextComponent,
            ComponentType.text_sensor: MQTTTextSensor,
            ComponentType.update: MQTTUpdateComponent,
            ComponentType.valve: MQTTValveComponent,
        }

        component_schema = {
            ComponentType.switch: MQTT_COMMAND_COMPONENT_SCHEMA,
            ComponentType.sensor: MQTT_COMPONENT_SCHEMA,
            ComponentType.number: MQTT_COMMAND_COMPONENT_SCHEMA,
            ComponentType.alarm_control_panel: MQTT_COMMAND_COMPONENT_SCHEMA,
            ComponentType.binary_sensoor: MQTT_COMPONENT_SCHEMA,
            ComponentType.button: MQTT_COMMAND_COMPONENT_SCHEMA,
            ComponentType.climate: MQTT_COMMAND_COMPONENT_SCHEMA,
            ComponentType.cover: MQTT_COMMAND_COMPONENT_SCHEMA,
            ComponentType.date: MQTT_COMMAND_COMPONENT_SCHEMA,
            ComponentType.time: MQTT_COMMAND_COMPONENT_SCHEMA,
            ComponentType.date_time: MQTT_COMMAND_COMPONENT_SCHEMA,
            ComponentType.event: MQTT_COMPONENT_SCHEMA,
            ComponentType.fan: MQTT_COMMAND_COMPONENT_SCHEMA,
            ComponentType.light: MQTT_COMMAND_COMPONENT_SCHEMA,
            ComponentType.lock: MQTT_COMMAND_COMPONENT_SCHEMA,
            ComponentType.select: MQTT_COMMAND_COMPONENT_SCHEMA,
            ComponentType.text: MQTT_COMPONENT_SCHEMA,
            ComponentType.text_sensor: MQTT_COMPONENT_SCHEMA,
            ComponentType.update: MQTT_COMMAND_COMPONENT_SCHEMA,
            ComponentType.valve: MQTT_COMMAND_COMPONENT_SCHEMA,
        }

        custom_schena = {
            ComponentType.valve: self.CUSTOM_SCHEMA_VALVE,
            ComponentType.sensor: self.CUSTOM_SCHEMA_SENSOR,
            ComponentType.fan: self.CUSTOM_SCHEMA_FAN,
            ComponentType.climate: self.CUSTOM_SCHEMA_CLIMATE,
            ComponentType.cover: self.CUSTOM_SCHEMA_COVER,
        }

        schema.extend(
            {
                cv.OnlyWith(self.CONF_MQTT_ID, "mqtt"): cv.declare_id(
                    component_class[component]
                ),
            }
        )
        schema.extend(component_schema[component])

        if component in custom_schena:
            schema.extend(custom_schena[component])

    async def register_component(self, component: ComponentType, var, config):
        mqtt_id = config.get(self.CONF_MQTT_ID)
        if not mqtt_id:
            return

        mqtt_ = cg.new_Pvariable(mqtt_id, var)
        await register_mqtt_controler_component(mqtt_, config)

        component = component.value

        if component == "sensor":
            if (expire_after := config.get(CONF_EXPIRE_AFTER, _UNDEF)) is not _UNDEF:
                if expire_after is None:
                    cg.add(mqtt_.disable_expire_after())
                else:
                    cg.add(mqtt_.set_expire_after(expire_after))
        elif component == "climate":
            if (action_state_topic := config.get(CONF_ACTION_STATE_TOPIC)) is not None:
                cg.add(mqtt_.set_custom_action_state_topic(action_state_topic))
            if (away_command_topic := config.get(CONF_AWAY_COMMAND_TOPIC)) is not None:
                cg.add(mqtt_.set_custom_away_command_topic(away_command_topic))
            if (away_state_topic := config.get(CONF_AWAY_STATE_TOPIC)) is not None:
                cg.add(mqtt_.set_custom_away_state_topic(away_state_topic))
            if (
                current_temperature_state_topic := config.get(
                    CONF_CURRENT_TEMPERATURE_STATE_TOPIC
                )
            ) is not None:
                cg.add(
                    mqtt_.set_custom_current_temperature_state_topic(
                        current_temperature_state_topic
                    )
                )
            if (
                current_humidity_state_topic := config.get(
                    CONF_CURRENT_HUMIDITY_STATE_TOPIC
                )
            ) is not None:
                cg.add(
                    mqtt_.set_custom_current_humidity_state_topic(
                        current_humidity_state_topic
                    )
                )
            if (
                fan_mode_command_topic := config.get(CONF_FAN_MODE_COMMAND_TOPIC)
            ) is not None:
                cg.add(mqtt_.set_custom_fan_mode_command_topic(fan_mode_command_topic))
            if (
                fan_mode_state_topic := config.get(CONF_FAN_MODE_STATE_TOPIC)
            ) is not None:
                cg.add(mqtt_.set_custom_fan_mode_state_topic(fan_mode_state_topic))
            if (mode_command_topic := config.get(CONF_MODE_COMMAND_TOPIC)) is not None:
                cg.add(mqtt_.set_custom_mode_command_topic(mode_command_topic))
            if (mode_state_topic := config.get(CONF_MODE_STATE_TOPIC)) is not None:
                cg.add(mqtt_.set_custom_mode_state_topic(mode_state_topic))
            if (
                preset_command_topic := config.get(CONF_PRESET_COMMAND_TOPIC)
            ) is not None:
                cg.add(mqtt_.set_custom_preset_command_topic(preset_command_topic))
            if (preset_state_topic := config.get(CONF_PRESET_STATE_TOPIC)) is not None:
                cg.add(mqtt_.set_custom_preset_state_topic(preset_state_topic))
            if (
                swing_mode_command_topic := config.get(CONF_SWING_MODE_COMMAND_TOPIC)
            ) is not None:
                cg.add(
                    mqtt_.set_custom_swing_mode_command_topic(swing_mode_command_topic)
                )
            if (
                swing_mode_state_topic := config.get(CONF_SWING_MODE_STATE_TOPIC)
            ) is not None:
                cg.add(mqtt_.set_custom_swing_mode_state_topic(swing_mode_state_topic))
            if (
                target_temperature_command_topic := config.get(
                    CONF_TARGET_TEMPERATURE_COMMAND_TOPIC
                )
            ) is not None:
                cg.add(
                    mqtt_.set_custom_target_temperature_command_topic(
                        target_temperature_command_topic
                    )
                )
            if (
                target_temperature_state_topic := config.get(
                    CONF_TARGET_TEMPERATURE_STATE_TOPIC
                )
            ) is not None:
                cg.add(
                    mqtt_.set_custom_target_temperature_state_topic(
                        target_temperature_state_topic
                    )
                )
            if (
                target_temperature_high_command_topic := config.get(
                    CONF_TARGET_TEMPERATURE_HIGH_COMMAND_TOPIC
                )
            ) is not None:
                cg.add(
                    mqtt_.set_custom_target_temperature_high_command_topic(
                        target_temperature_high_command_topic
                    )
                )
            if (
                target_temperature_high_state_topic := config.get(
                    CONF_TARGET_TEMPERATURE_HIGH_STATE_TOPIC
                )
            ) is not None:
                cg.add(
                    mqtt_.set_custom_target_temperature_high_state_topic(
                        target_temperature_high_state_topic
                    )
                )
            if (
                target_temperature_low_command_topic := config.get(
                    CONF_TARGET_TEMPERATURE_LOW_COMMAND_TOPIC
                )
            ) is not None:
                cg.add(
                    mqtt_.set_custom_target_temperature_low_command_topic(
                        target_temperature_low_command_topic
                    )
                )
            if (
                target_temperature_low_state_topic := config.get(
                    CONF_TARGET_TEMPERATURE_LOW_STATE_TOPIC
                )
            ) is not None:
                cg.add(
                    mqtt_.set_custom_target_temperature_state_topic(
                        target_temperature_low_state_topic
                    )
                )
            if (
                target_humidity_command_topic := config.get(
                    CONF_TARGET_HUMIDITY_COMMAND_TOPIC
                )
            ) is not None:
                cg.add(
                    mqtt_.set_custom_target_humidity_command_topic(
                        target_humidity_command_topic
                    )
                )
            if (
                target_humidity_state_topic := config.get(
                    CONF_TARGET_HUMIDITY_STATE_TOPIC
                )
            ) is not None:
                cg.add(
                    mqtt_.set_custom_target_humidity_state_topic(
                        target_humidity_state_topic
                    )
                )
        elif component == "cover":
            if (
                position_state_topic := config.get(CONF_POSITION_STATE_TOPIC)
            ) is not None:
                cg.add(mqtt_.set_custom_position_state_topic(position_state_topic))
            if (
                position_command_topic := config.get(CONF_POSITION_COMMAND_TOPIC)
            ) is not None:
                cg.add(mqtt_.set_custom_position_command_topic(position_command_topic))
            if (tilt_state_topic := config.get(CONF_TILT_STATE_TOPIC)) is not None:
                cg.add(mqtt_.set_custom_tilt_state_topic(tilt_state_topic))
            if (tilt_command_topic := config.get(CONF_TILT_COMMAND_TOPIC)) is not None:
                cg.add(mqtt_.set_custom_tilt_command_topic(tilt_command_topic))
        elif component == "fan":
            if (
                oscillation_state_topic := config.get(CONF_OSCILLATION_STATE_TOPIC)
            ) is not None:
                cg.add(
                    mqtt_.set_custom_oscillation_state_topic(oscillation_state_topic)
                )
            if (
                oscillation_command_topic := config.get(CONF_OSCILLATION_COMMAND_TOPIC)
            ) is not None:
                cg.add(
                    mqtt_.set_custom_oscillation_command_topic(
                        oscillation_command_topic
                    )
                )
            if (
                speed_level_state_topic := config.get(CONF_SPEED_LEVEL_STATE_TOPIC)
            ) is not None:
                cg.add(
                    mqtt_.set_custom_speed_level_state_topic(speed_level_state_topic)
                )
            if (
                speed_level_command_topic := config.get(CONF_SPEED_LEVEL_COMMAND_TOPIC)
            ) is not None:
                cg.add(
                    mqtt_.set_custom_speed_level_command_topic(
                        speed_level_command_topic
                    )
                )
            if (speed_state_topic := config.get(CONF_SPEED_STATE_TOPIC)) is not None:
                cg.add(mqtt_.set_custom_speed_state_topic(speed_state_topic))
            if (
                speed_command_topic := config.get(CONF_SPEED_COMMAND_TOPIC)
            ) is not None:
                cg.add(mqtt_.set_custom_speed_command_topic(speed_command_topic))
        elif component == "valve":
            if position_state_topic_config := config.get(CONF_POSITION_STATE_TOPIC):
                cg.add(
                    mqtt_.set_custom_position_state_topic(position_state_topic_config)
                )
            if position_command_topic_config := config.get(CONF_POSITION_COMMAND_TOPIC):
                cg.add(
                    mqtt_.set_custom_position_command_topic(
                        position_command_topic_config
                    )
                )


controler.add_secondary_controller(MqttController())


def validate_config(value):
    # Populate default fields
    out = value.copy()
    topic_prefix = value[CONF_TOPIC_PREFIX]
    # If the topic prefix is not null and these messages are not configured, then set them to the default
    # If the topic prefix is null and these messages are not configured, then set them to null
    if CONF_BIRTH_MESSAGE not in value:
        if topic_prefix != "":
            out[CONF_BIRTH_MESSAGE] = {
                CONF_TOPIC: f"{topic_prefix}/status",
                CONF_PAYLOAD: "online",
                CONF_QOS: 0,
                CONF_RETAIN: True,
            }
        else:
            out[CONF_BIRTH_MESSAGE] = {}
    if CONF_WILL_MESSAGE not in value:
        if topic_prefix != "":
            out[CONF_WILL_MESSAGE] = {
                CONF_TOPIC: f"{topic_prefix}/status",
                CONF_PAYLOAD: "offline",
                CONF_QOS: 0,
                CONF_RETAIN: True,
            }
        else:
            out[CONF_WILL_MESSAGE] = {}
    if CONF_SHUTDOWN_MESSAGE not in value:
        if topic_prefix != "":
            out[CONF_SHUTDOWN_MESSAGE] = {
                CONF_TOPIC: f"{topic_prefix}/status",
                CONF_PAYLOAD: "offline",
                CONF_QOS: 0,
                CONF_RETAIN: True,
            }
        else:
            out[CONF_SHUTDOWN_MESSAGE] = {}
    if CONF_LOG_TOPIC not in value:
        if topic_prefix != "":
            out[CONF_LOG_TOPIC] = {
                CONF_TOPIC: f"{topic_prefix}/debug",
                CONF_QOS: 0,
                CONF_RETAIN: True,
            }
        else:
            out[CONF_LOG_TOPIC] = {}
    return out


def validate_fingerprint(value):
    value = cv.string(value)
    if re.match(r"^[0-9a-f]{40}$", value) is None:
        raise cv.Invalid("fingerprint must be valid SHA1 hash")
    return value


CONFIG_SCHEMA = cv.All(
    cv.Schema(
        {
            cv.GenerateID(): cv.declare_id(MQTTClientComponent),
            cv.Required(CONF_BROKER): cv.string_strict,
            cv.Optional(CONF_PORT, default=1883): cv.port,
            cv.Optional(CONF_USERNAME, default=""): cv.string,
            cv.Optional(CONF_PASSWORD, default=""): cv.string,
            cv.Optional(CONF_CLEAN_SESSION, default=False): cv.boolean,
            cv.Optional(CONF_CLIENT_ID): cv.string,
            cv.SplitDefault(CONF_IDF_SEND_ASYNC, esp32_idf=False): cv.All(
                cv.boolean, cv.only_with_esp_idf
            ),
            cv.Optional(CONF_CERTIFICATE_AUTHORITY): cv.All(
                cv.string, cv.only_with_esp_idf
            ),
            cv.Inclusive(CONF_CLIENT_CERTIFICATE, "cert-key-pair"): cv.All(
                cv.string, cv.only_on_esp32
            ),
            cv.Inclusive(CONF_CLIENT_CERTIFICATE_KEY, "cert-key-pair"): cv.All(
                cv.string, cv.only_on_esp32
            ),
            cv.SplitDefault(CONF_SKIP_CERT_CN_CHECK, esp32_idf=False): cv.All(
                cv.boolean, cv.only_with_esp_idf
            ),
            cv.Optional(CONF_DISCOVERY, default=True): cv.Any(
                cv.boolean, cv.one_of("CLEAN", upper=True)
            ),
            cv.Optional(CONF_DISCOVERY_RETAIN, default=True): cv.boolean,
            cv.Optional(CONF_DISCOVER_IP, default=True): cv.boolean,
            cv.Optional(CONF_DISCOVERY_PREFIX, default="homeassistant"): publish_topic,
            cv.Optional(CONF_DISCOVERY_UNIQUE_ID_GENERATOR, default="legacy"): cv.enum(
                MQTT_DISCOVERY_UNIQUE_ID_GENERATOR_OPTIONS
            ),
            cv.Optional(CONF_DISCOVERY_OBJECT_ID_GENERATOR, default="none"): cv.enum(
                MQTT_DISCOVERY_OBJECT_ID_GENERATOR_OPTIONS
            ),
            cv.Optional(CONF_USE_ABBREVIATIONS, default=True): cv.boolean,
            cv.Optional(CONF_BIRTH_MESSAGE): MQTT_MESSAGE_SCHEMA,
            cv.Optional(CONF_WILL_MESSAGE): MQTT_MESSAGE_SCHEMA,
            cv.Optional(CONF_SHUTDOWN_MESSAGE): MQTT_MESSAGE_SCHEMA,
            cv.Optional(CONF_TOPIC_PREFIX, default=lambda: CORE.name): publish_topic,
            cv.Optional(CONF_LOG_TOPIC): cv.Any(
                None,
                MQTT_MESSAGE_BASE.extend(
                    {
                        cv.Optional(CONF_LEVEL): logger.is_log_level,
                    }
                ),
                validate_message_just_topic,
            ),
            cv.Optional(CONF_SSL_FINGERPRINTS): cv.All(
                cv.only_on_esp8266, cv.ensure_list(validate_fingerprint)
            ),
            cv.Optional(CONF_KEEPALIVE, default="15s"): cv.positive_time_period_seconds,
            cv.Optional(
                CONF_REBOOT_TIMEOUT, default="15min"
            ): cv.positive_time_period_milliseconds,
            cv.Optional(CONF_ON_CONNECT): automation.validate_automation(
                {
                    cv.GenerateID(CONF_TRIGGER_ID): cv.declare_id(MQTTConnectTrigger),
                }
            ),
            cv.Optional(CONF_ON_DISCONNECT): automation.validate_automation(
                {
                    cv.GenerateID(CONF_TRIGGER_ID): cv.declare_id(
                        MQTTDisconnectTrigger
                    ),
                }
            ),
            cv.Optional(CONF_ON_MESSAGE): automation.validate_automation(
                {
                    cv.GenerateID(CONF_TRIGGER_ID): cv.declare_id(MQTTMessageTrigger),
                    cv.Required(CONF_TOPIC): subscribe_topic,
                    cv.Optional(CONF_QOS, default=0): mqtt_qos,
                    cv.Optional(CONF_PAYLOAD): cv.string_strict,
                }
            ),
            cv.Optional(CONF_ON_JSON_MESSAGE): automation.validate_automation(
                {
                    cv.GenerateID(CONF_TRIGGER_ID): cv.declare_id(
                        MQTTJsonMessageTrigger
                    ),
                    cv.Required(CONF_TOPIC): subscribe_topic,
                    cv.Optional(CONF_QOS, default=0): mqtt_qos,
                }
            ),
        }
    ),
    validate_config,
    cv.only_on([PLATFORM_ESP32, PLATFORM_ESP8266, PLATFORM_BK72XX]),
)


def exp_mqtt_message(config):
    if config is None:
        return cg.optional(cg.TemplateArguments(MQTTMessage))
    exp = cg.StructInitializer(
        MQTTMessage,
        ("topic", config[CONF_TOPIC]),
        ("payload", config.get(CONF_PAYLOAD, "")),
        ("qos", config[CONF_QOS]),
        ("retain", config[CONF_RETAIN]),
    )
    return exp


@coroutine_with_priority(40.0)
async def to_code(config):
    var = cg.new_Pvariable(config[CONF_ID])
    await cg.register_component(var, config)
    # Add required libraries for ESP8266 and LibreTiny
    if CORE.is_esp8266 or CORE.is_libretiny:
        # https://github.com/heman/async-mqtt-client/blob/master/library.json
        cg.add_library("heman/AsyncMqttClient-esphome", "2.0.0")

    cg.add_define("USE_MQTT")
    cg.add_global(mqtt_ns.using)

    cg.add(var.set_broker_address(config[CONF_BROKER]))
    cg.add(var.set_broker_port(config[CONF_PORT]))
    cg.add(var.set_username(config[CONF_USERNAME]))
    cg.add(var.set_password(config[CONF_PASSWORD]))
    cg.add(var.set_clean_session(config[CONF_CLEAN_SESSION]))
    if CONF_CLIENT_ID in config:
        cg.add(var.set_client_id(config[CONF_CLIENT_ID]))

    discovery = config[CONF_DISCOVERY]
    discovery_retain = config[CONF_DISCOVERY_RETAIN]
    discovery_prefix = config[CONF_DISCOVERY_PREFIX]
    discovery_unique_id_generator = config[CONF_DISCOVERY_UNIQUE_ID_GENERATOR]
    discovery_object_id_generator = config[CONF_DISCOVERY_OBJECT_ID_GENERATOR]
    discover_ip = config[CONF_DISCOVER_IP]

    if not discovery:
        discovery_prefix = ""

    if not discovery and not discover_ip:
        cg.add(var.disable_discovery())
    elif discovery == "CLEAN":
        cg.add(
            var.set_discovery_info(
                discovery_prefix,
                discovery_unique_id_generator,
                discovery_object_id_generator,
                discovery_retain,
                discover_ip,
                True,
            )
        )
    elif CONF_DISCOVERY_RETAIN in config or CONF_DISCOVERY_PREFIX in config:
        cg.add(
            var.set_discovery_info(
                discovery_prefix,
                discovery_unique_id_generator,
                discovery_object_id_generator,
                discovery_retain,
                discover_ip,
            )
        )

    cg.add(var.set_topic_prefix(config[CONF_TOPIC_PREFIX]))

    if config[CONF_USE_ABBREVIATIONS]:
        cg.add_define("USE_MQTT_ABBREVIATIONS")

    birth_message = config[CONF_BIRTH_MESSAGE]
    if not birth_message:
        cg.add(var.disable_birth_message())
    else:
        cg.add(var.set_birth_message(exp_mqtt_message(birth_message)))
    will_message = config[CONF_WILL_MESSAGE]
    if not will_message:
        cg.add(var.disable_last_will())
    else:
        cg.add(var.set_last_will(exp_mqtt_message(will_message)))
    shutdown_message = config[CONF_SHUTDOWN_MESSAGE]
    if not shutdown_message:
        cg.add(var.disable_shutdown_message())
    else:
        cg.add(var.set_shutdown_message(exp_mqtt_message(shutdown_message)))

    log_topic = config[CONF_LOG_TOPIC]
    if not log_topic:
        cg.add(var.disable_log_message())
    else:
        cg.add(var.set_log_message_template(exp_mqtt_message(log_topic)))

        if CONF_LEVEL in log_topic:
            cg.add(var.set_log_level(logger.LOG_LEVELS[log_topic[CONF_LEVEL]]))

    if CONF_SSL_FINGERPRINTS in config:
        for fingerprint in config[CONF_SSL_FINGERPRINTS]:
            arr = [
                cg.RawExpression(f"0x{fingerprint[i:i + 2]}") for i in range(0, 40, 2)
            ]
            cg.add(var.add_ssl_fingerprint(arr))
        cg.add_build_flag("-DASYNC_TCP_SSL_ENABLED=1")

    cg.add(var.set_keep_alive(config[CONF_KEEPALIVE]))

    cg.add(var.set_reboot_timeout(config[CONF_REBOOT_TIMEOUT]))

    # esp-idf only
    if CONF_CERTIFICATE_AUTHORITY in config:
        cg.add(var.set_ca_certificate(config[CONF_CERTIFICATE_AUTHORITY]))
        cg.add(var.set_skip_cert_cn_check(config[CONF_SKIP_CERT_CN_CHECK]))
        if CONF_CLIENT_CERTIFICATE in config:
            cg.add(var.set_cl_certificate(config[CONF_CLIENT_CERTIFICATE]))
            cg.add(var.set_cl_key(config[CONF_CLIENT_CERTIFICATE_KEY]))

        # prevent error -0x428e
        # See https://github.com/espressif/esp-idf/issues/139
        add_idf_sdkconfig_option("CONFIG_MBEDTLS_HARDWARE_MPI", False)

    if CONF_IDF_SEND_ASYNC in config and config[CONF_IDF_SEND_ASYNC]:
        cg.add_define("USE_MQTT_IDF_ENQUEUE")
    # end esp-idf

    for conf in config.get(CONF_ON_MESSAGE, []):
        trig = cg.new_Pvariable(conf[CONF_TRIGGER_ID], conf[CONF_TOPIC])
        cg.add(trig.set_qos(conf[CONF_QOS]))
        if CONF_PAYLOAD in conf:
            cg.add(trig.set_payload(conf[CONF_PAYLOAD]))
        await cg.register_component(trig, conf)
        await automation.build_automation(trig, [(cg.std_string, "x")], conf)

    for conf in config.get(CONF_ON_JSON_MESSAGE, []):
        trig = cg.new_Pvariable(conf[CONF_TRIGGER_ID], conf[CONF_TOPIC], conf[CONF_QOS])
        await automation.build_automation(trig, [(cg.JsonObjectConst, "x")], conf)

    for conf in config.get(CONF_ON_CONNECT, []):
        trigger = cg.new_Pvariable(conf[CONF_TRIGGER_ID], var)
        await automation.build_automation(trigger, [], conf)

    for conf in config.get(CONF_ON_DISCONNECT, []):
        trigger = cg.new_Pvariable(conf[CONF_TRIGGER_ID], var)
        await automation.build_automation(trigger, [], conf)


MQTT_PUBLISH_ACTION_SCHEMA = cv.Schema(
    {
        cv.GenerateID(): cv.use_id(MQTTClientComponent),
        cv.Required(CONF_TOPIC): cv.templatable(publish_topic),
        cv.Required(CONF_PAYLOAD): cv.templatable(mqtt_payload),
        cv.Optional(CONF_QOS, default=0): cv.templatable(mqtt_qos),
        cv.Optional(CONF_RETAIN, default=False): cv.templatable(cv.boolean),
    }
)


@automation.register_action(
    "mqtt.publish", MQTTPublishAction, MQTT_PUBLISH_ACTION_SCHEMA
)
async def mqtt_publish_action_to_code(config, action_id, template_arg, args):
    paren = await cg.get_variable(config[CONF_ID])
    var = cg.new_Pvariable(action_id, template_arg, paren)
    template_ = await cg.templatable(config[CONF_TOPIC], args, cg.std_string)
    cg.add(var.set_topic(template_))

    template_ = await cg.templatable(config[CONF_PAYLOAD], args, cg.std_string)
    cg.add(var.set_payload(template_))
    template_ = await cg.templatable(config[CONF_QOS], args, cg.uint8)
    cg.add(var.set_qos(template_))
    template_ = await cg.templatable(config[CONF_RETAIN], args, bool)
    cg.add(var.set_retain(template_))
    return var


MQTT_PUBLISH_JSON_ACTION_SCHEMA = cv.Schema(
    {
        cv.GenerateID(): cv.use_id(MQTTClientComponent),
        cv.Required(CONF_TOPIC): cv.templatable(publish_topic),
        cv.Required(CONF_PAYLOAD): cv.lambda_,
        cv.Optional(CONF_QOS, default=0): cv.templatable(mqtt_qos),
        cv.Optional(CONF_RETAIN, default=False): cv.templatable(cv.boolean),
    }
)


@automation.register_action(
    "mqtt.publish_json", MQTTPublishJsonAction, MQTT_PUBLISH_JSON_ACTION_SCHEMA
)
async def mqtt_publish_json_action_to_code(config, action_id, template_arg, args):
    paren = await cg.get_variable(config[CONF_ID])
    var = cg.new_Pvariable(action_id, template_arg, paren)
    template_ = await cg.templatable(config[CONF_TOPIC], args, cg.std_string)
    cg.add(var.set_topic(template_))

    args_ = args + [(cg.JsonObject, "root")]
    lambda_ = await cg.process_lambda(config[CONF_PAYLOAD], args_, return_type=cg.void)
    cg.add(var.set_payload(lambda_))
    template_ = await cg.templatable(config[CONF_QOS], args, cg.uint8)
    cg.add(var.set_qos(template_))
    template_ = await cg.templatable(config[CONF_RETAIN], args, bool)
    cg.add(var.set_retain(template_))
    return var


def get_default_topic_for(data, component_type, name, suffix):
    allowlist = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
    sanitized_name = "".join(
        x for x in name.lower().replace(" ", "_") if x in allowlist
    )
    return f"{data.topic_prefix}/{component_type}/{sanitized_name}/{suffix}"


async def register_mqtt_controler_component(var, config):
    await cg.register_component(var, {})

    if CONF_QOS in config:
        cg.add(var.set_qos(config[CONF_QOS]))
    if CONF_RETAIN in config:
        cg.add(var.set_retain(config[CONF_RETAIN]))
    if not config.get(CONF_DISCOVERY, True):
        cg.add(var.disable_discovery())
    if CONF_STATE_TOPIC in config:
        cg.add(var.set_custom_state_topic(config[CONF_STATE_TOPIC]))
    if CONF_COMMAND_TOPIC in config:
        cg.add(var.set_custom_command_topic(config[CONF_COMMAND_TOPIC]))
    if CONF_COMMAND_RETAIN in config:
        cg.add(var.set_command_retain(config[CONF_COMMAND_RETAIN]))
    if CONF_AVAILABILITY in config:
        availability = config[CONF_AVAILABILITY]
        if not availability:
            cg.add(var.disable_availability())
        else:
            cg.add(
                var.set_availability(
                    availability[CONF_TOPIC],
                    availability[CONF_PAYLOAD_AVAILABLE],
                    availability[CONF_PAYLOAD_NOT_AVAILABLE],
                )
            )


@automation.register_condition(
    "mqtt.connected",
    MQTTConnectedCondition,
    cv.Schema(
        {
            cv.GenerateID(): cv.use_id(MQTTClientComponent),
        }
    ),
)
async def mqtt_connected_to_code(config, condition_id, template_arg, args):
    paren = await cg.get_variable(config[CONF_ID])
    return cg.new_Pvariable(condition_id, template_arg, paren)
