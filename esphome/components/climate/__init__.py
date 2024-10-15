from esphome import automation, controler
import esphome.codegen as cg
from esphome.components import web_server
import esphome.config_validation as cv
from esphome.const import (
    CONF_AWAY,
    CONF_CUSTOM_FAN_MODE,
    CONF_CUSTOM_PRESET,
    CONF_FAN_MODE,
    CONF_ID,
    CONF_MAX_TEMPERATURE,
    CONF_MIN_TEMPERATURE,
    CONF_MODE,
    CONF_ON_CONTROL,
    CONF_ON_STATE,
    CONF_PRESET,
    CONF_SWING_MODE,
    CONF_TARGET_TEMPERATURE,
    CONF_TARGET_TEMPERATURE_HIGH,
    CONF_TARGET_TEMPERATURE_LOW,
    CONF_TEMPERATURE_STEP,
    CONF_TRIGGER_ID,
    CONF_VISUAL,
    CONF_WEB_SERVER,
)
from esphome.controler import ComponentType
from esphome.core import CORE, coroutine_with_priority
from esphome.cpp_helpers import setup_entity

IS_PLATFORM_COMPONENT = True

CODEOWNERS = ["@esphome/core"]
climate_ns = cg.esphome_ns.namespace("climate")

Climate = climate_ns.class_("Climate", cg.EntityBase)
ClimateCall = climate_ns.class_("ClimateCall")
ClimateTraits = climate_ns.class_("ClimateTraits")

ClimateMode = climate_ns.enum("ClimateMode")
CLIMATE_MODES = {
    "OFF": ClimateMode.CLIMATE_MODE_OFF,
    "HEAT_COOL": ClimateMode.CLIMATE_MODE_HEAT_COOL,
    "COOL": ClimateMode.CLIMATE_MODE_COOL,
    "HEAT": ClimateMode.CLIMATE_MODE_HEAT,
    "DRY": ClimateMode.CLIMATE_MODE_DRY,
    "FAN_ONLY": ClimateMode.CLIMATE_MODE_FAN_ONLY,
    "AUTO": ClimateMode.CLIMATE_MODE_AUTO,
}
validate_climate_mode = cv.enum(CLIMATE_MODES, upper=True)

ClimateFanMode = climate_ns.enum("ClimateFanMode")
CLIMATE_FAN_MODES = {
    "ON": ClimateFanMode.CLIMATE_FAN_ON,
    "OFF": ClimateFanMode.CLIMATE_FAN_OFF,
    "AUTO": ClimateFanMode.CLIMATE_FAN_AUTO,
    "LOW": ClimateFanMode.CLIMATE_FAN_LOW,
    "MEDIUM": ClimateFanMode.CLIMATE_FAN_MEDIUM,
    "HIGH": ClimateFanMode.CLIMATE_FAN_HIGH,
    "MIDDLE": ClimateFanMode.CLIMATE_FAN_MIDDLE,
    "FOCUS": ClimateFanMode.CLIMATE_FAN_FOCUS,
    "DIFFUSE": ClimateFanMode.CLIMATE_FAN_DIFFUSE,
    "QUIET": ClimateFanMode.CLIMATE_FAN_QUIET,
}

validate_climate_fan_mode = cv.enum(CLIMATE_FAN_MODES, upper=True)

ClimatePreset = climate_ns.enum("ClimatePreset")
CLIMATE_PRESETS = {
    "NONE": ClimatePreset.CLIMATE_PRESET_NONE,
    "ECO": ClimatePreset.CLIMATE_PRESET_ECO,
    "AWAY": ClimatePreset.CLIMATE_PRESET_AWAY,
    "BOOST": ClimatePreset.CLIMATE_PRESET_BOOST,
    "COMFORT": ClimatePreset.CLIMATE_PRESET_COMFORT,
    "HOME": ClimatePreset.CLIMATE_PRESET_HOME,
    "SLEEP": ClimatePreset.CLIMATE_PRESET_SLEEP,
    "ACTIVITY": ClimatePreset.CLIMATE_PRESET_ACTIVITY,
}

validate_climate_preset = cv.enum(CLIMATE_PRESETS, upper=True)

ClimateSwingMode = climate_ns.enum("ClimateSwingMode")
CLIMATE_SWING_MODES = {
    "OFF": ClimateSwingMode.CLIMATE_SWING_OFF,
    "BOTH": ClimateSwingMode.CLIMATE_SWING_BOTH,
    "VERTICAL": ClimateSwingMode.CLIMATE_SWING_VERTICAL,
    "HORIZONTAL": ClimateSwingMode.CLIMATE_SWING_HORIZONTAL,
}

validate_climate_swing_mode = cv.enum(CLIMATE_SWING_MODES, upper=True)

CONF_CURRENT_TEMPERATURE = "current_temperature"
CONF_MIN_HUMIDITY = "min_humidity"
CONF_MAX_HUMIDITY = "max_humidity"
CONF_TARGET_HUMIDITY = "target_humidity"

visual_temperature = cv.float_with_unit(
    "visual_temperature", "(°C|° C|°|C|° K|° K|K|°F|° F|F)?"
)


def single_visual_temperature(value):
    if isinstance(value, dict):
        return value

    value = visual_temperature(value)
    return VISUAL_TEMPERATURE_STEP_SCHEMA(
        {
            CONF_TARGET_TEMPERATURE: value,
            CONF_CURRENT_TEMPERATURE: value,
        }
    )


# Actions
ControlAction = climate_ns.class_("ControlAction", automation.Action)
StateTrigger = climate_ns.class_(
    "StateTrigger", automation.Trigger.template(Climate.operator("ref"))
)
ControlTrigger = climate_ns.class_(
    "ControlTrigger", automation.Trigger.template(ClimateCall.operator("ref"))
)

VISUAL_TEMPERATURE_STEP_SCHEMA = cv.Any(
    single_visual_temperature,
    cv.Schema(
        {
            cv.Required(CONF_TARGET_TEMPERATURE): visual_temperature,
            cv.Required(CONF_CURRENT_TEMPERATURE): visual_temperature,
        }
    ),
)

CLIMATE_SCHEMA = (
    cv.ENTITY_BASE_SCHEMA.extend(web_server.WEBSERVER_SORTING_SCHEMA)
    .extend(controler.gen_component_schema(ComponentType.climate))
    .extend(
        {
            cv.GenerateID(): cv.declare_id(Climate),
            cv.Optional(CONF_VISUAL, default={}): cv.Schema(
                {
                    cv.Optional(CONF_MIN_TEMPERATURE): cv.temperature,
                    cv.Optional(CONF_MAX_TEMPERATURE): cv.temperature,
                    cv.Optional(CONF_TEMPERATURE_STEP): VISUAL_TEMPERATURE_STEP_SCHEMA,
                    cv.Optional(CONF_MIN_HUMIDITY): cv.percentage_int,
                    cv.Optional(CONF_MAX_HUMIDITY): cv.percentage_int,
                }
            ),
            cv.Optional(CONF_ON_CONTROL): automation.validate_automation(
                {
                    cv.GenerateID(CONF_TRIGGER_ID): cv.declare_id(ControlTrigger),
                }
            ),
            cv.Optional(CONF_ON_STATE): automation.validate_automation(
                {
                    cv.GenerateID(CONF_TRIGGER_ID): cv.declare_id(StateTrigger),
                }
            ),
        }
    )
)


async def setup_climate_core_(var, config):
    await setup_entity(var, config)

    visual = config[CONF_VISUAL]
    if (min_temp := visual.get(CONF_MIN_TEMPERATURE)) is not None:
        cg.add(var.set_visual_min_temperature_override(min_temp))
    if (max_temp := visual.get(CONF_MAX_TEMPERATURE)) is not None:
        cg.add(var.set_visual_max_temperature_override(max_temp))
    if (temp_step := visual.get(CONF_TEMPERATURE_STEP)) is not None:
        cg.add(
            var.set_visual_temperature_step_override(
                temp_step[CONF_TARGET_TEMPERATURE],
                temp_step[CONF_CURRENT_TEMPERATURE],
            )
        )
    if (min_humidity := visual.get(CONF_MIN_HUMIDITY)) is not None:
        cg.add(var.set_visual_min_humidity_override(min_humidity))
    if (max_humidity := visual.get(CONF_MAX_HUMIDITY)) is not None:
        cg.add(var.set_visual_max_humidity_override(max_humidity))

    for conf in config.get(CONF_ON_STATE, []):
        trigger = cg.new_Pvariable(conf[CONF_TRIGGER_ID], var)
        await automation.build_automation(
            trigger, [(Climate.operator("ref"), "x")], conf
        )

    for conf in config.get(CONF_ON_CONTROL, []):
        trigger = cg.new_Pvariable(conf[CONF_TRIGGER_ID], var)
        await automation.build_automation(
            trigger, [(ClimateCall.operator("ref"), "x")], conf
        )

    await controler.setup_component(ComponentType.climate, var, config)

    if web_server_config := config.get(CONF_WEB_SERVER):
        await web_server.add_entity_config(var, web_server_config)


async def register_climate(var, config):
    if not CORE.has_id(config[CONF_ID]):
        var = cg.Pvariable(config[CONF_ID], var)
    cg.add(cg.App.register_climate(var))
    await setup_climate_core_(var, config)


CLIMATE_CONTROL_ACTION_SCHEMA = cv.Schema(
    {
        cv.Required(CONF_ID): cv.use_id(Climate),
        cv.Optional(CONF_MODE): cv.templatable(validate_climate_mode),
        cv.Optional(CONF_TARGET_TEMPERATURE): cv.templatable(cv.temperature),
        cv.Optional(CONF_TARGET_TEMPERATURE_LOW): cv.templatable(cv.temperature),
        cv.Optional(CONF_TARGET_TEMPERATURE_HIGH): cv.templatable(cv.temperature),
        cv.Optional(CONF_TARGET_HUMIDITY): cv.templatable(cv.percentage_int),
        cv.Optional(CONF_AWAY): cv.invalid("Use preset instead"),
        cv.Exclusive(CONF_FAN_MODE, "fan_mode"): cv.templatable(
            validate_climate_fan_mode
        ),
        cv.Exclusive(CONF_CUSTOM_FAN_MODE, "fan_mode"): cv.templatable(
            cv.string_strict
        ),
        cv.Exclusive(CONF_PRESET, "preset"): cv.templatable(validate_climate_preset),
        cv.Exclusive(CONF_CUSTOM_PRESET, "preset"): cv.templatable(cv.string_strict),
        cv.Optional(CONF_SWING_MODE): cv.templatable(validate_climate_swing_mode),
    }
)


@automation.register_action(
    "climate.control", ControlAction, CLIMATE_CONTROL_ACTION_SCHEMA
)
async def climate_control_to_code(config, action_id, template_arg, args):
    paren = await cg.get_variable(config[CONF_ID])
    var = cg.new_Pvariable(action_id, template_arg, paren)
    if (mode := config.get(CONF_MODE)) is not None:
        template_ = await cg.templatable(mode, args, ClimateMode)
        cg.add(var.set_mode(template_))
    if (target_temp := config.get(CONF_TARGET_TEMPERATURE)) is not None:
        template_ = await cg.templatable(target_temp, args, float)
        cg.add(var.set_target_temperature(template_))
    if (target_temp_low := config.get(CONF_TARGET_TEMPERATURE_LOW)) is not None:
        template_ = await cg.templatable(target_temp_low, args, float)
        cg.add(var.set_target_temperature_low(template_))
    if (target_temp_high := config.get(CONF_TARGET_TEMPERATURE_HIGH)) is not None:
        template_ = await cg.templatable(target_temp_high, args, float)
        cg.add(var.set_target_temperature_high(template_))
    if (target_humidity := config.get(CONF_TARGET_HUMIDITY)) is not None:
        template_ = await cg.templatable(target_humidity, args, float)
        cg.add(var.set_target_humidity(template_))
    if (fan_mode := config.get(CONF_FAN_MODE)) is not None:
        template_ = await cg.templatable(fan_mode, args, ClimateFanMode)
        cg.add(var.set_fan_mode(template_))
    if (custom_fan_mode := config.get(CONF_CUSTOM_FAN_MODE)) is not None:
        template_ = await cg.templatable(custom_fan_mode, args, cg.std_string)
        cg.add(var.set_custom_fan_mode(template_))
    if (preset := config.get(CONF_PRESET)) is not None:
        template_ = await cg.templatable(preset, args, ClimatePreset)
        cg.add(var.set_preset(template_))
    if (custom_preset := config.get(CONF_CUSTOM_PRESET)) is not None:
        template_ = await cg.templatable(custom_preset, args, cg.std_string)
        cg.add(var.set_custom_preset(template_))
    if (swing_mode := config.get(CONF_SWING_MODE)) is not None:
        template_ = await cg.templatable(swing_mode, args, ClimateSwingMode)
        cg.add(var.set_swing_mode(template_))
    return var


@coroutine_with_priority(100.0)
async def to_code(config):
    cg.add_define("USE_CLIMATE")
    cg.add_global(climate_ns.using)
