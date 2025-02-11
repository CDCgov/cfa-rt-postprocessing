from flask import Flask, Response, request

from src.cfa_rt_postprocessing.main_functions import (
    merge_and_render_anomaly,
    validate_args,
)

app = Flask(__name__)


@app.route("/")
def base():
    """Root server route to ensure proper startup.
    Returns:
        str: Startup message.
    """
    return "<p>Rt postprocessing server running.</p>"


@app.post("/merge_and_render")
def merge():
    """Route to merge Rt output files, and render the anomaly report.
    Takes inputs (from the request body):
        - release_name
        - min_runat
        - max_runat
        - (Optional) rt_output_container_name
        - (Optional) post_process_container_name
        - (Optional) overwrite_blobs
    Returns:
        flask.Response: JSON response with status and message.
    """
    try:
        validated_args = validate_args(request.json)
        merge_and_render_anomaly(**validated_args)
        response_message = f"Task files successfully merged at {validated_args['post_process_container_name']}/{validated_args['release_name']}"
        return Response(
            response=response_message, status=200, mimetype="application/json"
        )
    except Exception as e:
        return Response(response=str(e), status=400, mimetype="application/json")
