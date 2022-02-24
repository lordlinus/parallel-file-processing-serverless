# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    root_directory: str = context.get_input()

    if not root_directory:
        raise Exception("A root directory path with raw files is required as input")

    files = yield context.call_activity("step0", root_directory)
    tasks = []
    for file in files:
        tasks.append(context.call_sub_orchestrator("SubOrchestratorFunc", file))

    results = yield context.task_all(tasks)

    return results


main = df.Orchestrator.create(orchestrator_function)
