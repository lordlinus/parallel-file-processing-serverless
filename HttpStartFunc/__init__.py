# This function an HTTP starter function for Durable Functions.

import logging
import json

import azure.functions as func
import azure.durable_functions as df


async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)
    requestBody = json.loads(req.get_body().decode())

    instance_id = await client.start_new(
        req.route_params["functionName"], client_input=requestBody
    )

    logging.info(f"Started orchestration with ID = '{instance_id}'.")

    return client.create_check_status_response(req, instance_id)
