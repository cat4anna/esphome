from enum import Enum

import esphome.config_validation as cv

CONTROLERS = []


class ComponentType(str, Enum):
    switch = "switch"
    sensor = "sensor"
    number = "number"
    alarm_control_panel = "alarm_control_panel"
    binary_sensoor = "binary_sensoor"
    button = "button"
    climate = "climate"
    cover = "cover"
    date = "date"
    time = "time"
    date_time = "date_time"
    event = "event"
    fan = "fan"
    light = "light"
    lock = "lock"
    select = "select"
    text = "text"
    text_sensor = "text_sensor"
    update = "update"
    valve = "valve"


class BaseControler:
    def extend_component_schema(self, component: ComponentType, schema):
        pass

    async def register_component(self, component: ComponentType, var, config):
        pass


def gen_component_schema(component: ComponentType):
    result = cv.ENTITY_BASE_SCHEMA.extend({})
    for item in CONTROLERS:
        result = result.extend(
            item.extend_component_schema(component, cv.ENTITY_BASE_SCHEMA.extend({}))
        )
    return result


def add_secondary_controller(controler: BaseControler):
    CONTROLERS.append(controler)


async def setup_component(component: ComponentType, var, config):
    for item in CONTROLERS:
        await item.register_component(component, var, config)
