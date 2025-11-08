import esphome.config_validation as cv

CONTROLLERS = []


class BaseController:
    def extend_component_schema(self, component: str, schema):
        pass

    async def register_component(self, component: str, var, config, output=None):
        pass


def gen_component_schema(component: str):
    result = cv.ENTITY_BASE_SCHEMA.extend({})
    for item in CONTROLLERS:
        result = result.extend(
            item.extend_component_schema(component, cv.ENTITY_BASE_SCHEMA.extend({}))
        )
    return result


def register_secondary_controller(controller: BaseController):
    CONTROLLERS.append(controller)


def get_controller(controller: str) -> BaseController:
    for item in CONTROLLERS:
        if item.CONTROLLER_NAME == controller:
            return item
    return None


async def setup_component(component: str, var, config, output=None):
    for item in CONTROLLERS:
        await item.register_component(component, var, config, output=output)
