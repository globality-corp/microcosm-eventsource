"""
Event routes

"""
from microcosm_flask.conventions.base import EndpointDefinition
from microcosm_flask.conventions.crud import configure_crud
from microcosm_flask.operations import Operation
from microcosm_postgres.context import transactional
from microcosm_pubsub.producer import deferred_batch


def configure_event_crud(
    graph,
    controller,
    event_schema,
    new_event_schema,
    search_event_schema,
    use_deferred_batch=False,
):
    if use_deferred_batch:
        create_func = transactional(deferred_batch(controller)(controller.create))
    else:
        create_func = transactional(controller.create)

    mappings = {
        Operation.Create: EndpointDefinition(
            func=create_func,
            request_schema=new_event_schema,
            response_schema=event_schema,
        ),
        Operation.Delete: EndpointDefinition(
            func=transactional(controller.delete),
        ),
        Operation.Replace: EndpointDefinition(
            func=transactional(controller.replace),
            request_schema=new_event_schema,
            response_schema=event_schema,
        ),
        Operation.Retrieve: EndpointDefinition(
            func=controller.retrieve,
            response_schema=event_schema,
        ),
        Operation.Search: EndpointDefinition(
            func=controller.search,
            request_schema=search_event_schema,
            response_schema=event_schema,
        ),
    }
    configure_crud(graph, controller.ns, mappings)
    return controller.ns
