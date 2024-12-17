import logging

import azure.functions as func

app = func.FunctionApp()
logger = logging.getLogger(__name__)


@app.route("hello-world", methods=["GET"])
def hello_world(req: func.HttpRequest) -> func.HttpResponse:
    logger.info("Python HTTP trigger function processed a request.")
    return func.HttpResponse("Hello, World!", status_code=200)
