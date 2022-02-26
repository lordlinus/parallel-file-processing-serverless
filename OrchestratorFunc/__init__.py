# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    httpPostInput: str = context.get_input()

    if not httpPostInput:
        raise Exception(
            "A root directory, number of rows and number of columns per file is required as input"
        )

    # Generate input data for testing
    data_gen_path = yield context.call_activity("GenerateData", httpPostInput)

    # Pass to step 0 to get a list of files from folder to process
    files = yield context.call_activity("step0", data_gen_path)
    tasks = []
    # for each file in the list from step0, process step1, step2, step3 and step4 as SubOrchestrator
    for file in files:
        tasks.append(context.call_sub_orchestrator("SubOrchestratorFunc", file))

    results = yield context.task_all(tasks)

    return results


main = df.Orchestrator.create(orchestrator_function)
