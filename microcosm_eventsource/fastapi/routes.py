"""
Event routes

"""
from microcosm_fastapi.conventions.crud import configure_crud
from microcosm_fastapi.operations import Operation
from microcosm_pubsub.producer import deferred_batch


def configure_event_crud(
    graph,
    controller,
    # event_schema,
    # new_event_schema,
    # search_event_schema,
    use_deferred_batch=False,
):
    if use_deferred_batch:
        create_func = deferred_batch(controller)(controller.create)
    else:
        create_func = controller.create

    # TODO - this will have to be re-worked with typings
    mappings = {
        Operation.Create: create_func,
        Operation.Delete: controller.delete,
        Operation.Replace: controller.replace,
        Operation.Retrieve: controller.retrieve,
        Operation.Search: controller.search,
    }
    configure_crud(graph, controller.ns, mappings)
    return controller.ns
