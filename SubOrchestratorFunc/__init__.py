# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.


import logging

import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    filename: str = context.get_input()

    if not filename:
        raise Exception("A filename or path is required as input")
    logging.info(f"Input file to be processed: {filename}")

    # function chaining to process the received input file in 4 steps, each step takes input from previous step
    result1 = yield context.call_activity("step1", filename)
    result2 = yield context.call_activity("step2", result1)
    result3 = yield context.call_activity("step3", result2)
    result4 = yield context.call_activity("step4", result3)
    return [result1, result2, result3, result4]


main = df.Orchestrator.create(orchestrator_function)
