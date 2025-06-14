import json
import logging
import os
import re
from functools import wraps

from flask import (
    Flask,
    jsonify,
    redirect,
    render_template_string,
    request,
    send_from_directory,
    session,
)
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# --- Version Configuration ---
__version__ = "0.4.0"

# --- Log Archiver Imports ---
from log_archiver import start_log_cleanup_job, watch_pods_and_archive

# --- Flask App Setup ---
app = Flask(__name__, static_folder=".", static_url_path="")  # Serve static files from current dir

# --- Logging Configuration ---
# Basic logging to see Flask and K8s client interactions
logging.basicConfig(level=logging.INFO)
# Quieter Kubernetes client library logging for routine calls, unless debugging.
logging.getLogger("kubernetes").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


# Add custom filter to prevent logging of /ready endpoint
class ReadyEndpointFilter(logging.Filter):
    def filter(self, record):
        # Check both the message and the args for the /ready endpoint
        if isinstance(record.msg, str):
            if "GET /ready" in record.msg:
                return False
        if isinstance(record.args, tuple):
            for arg in record.args:
                if isinstance(arg, str) and "GET /ready" in arg:
                    return False
        return True


# Apply filter to both Werkzeug and Flask loggers
logging.getLogger("werkzeug").addFilter(ReadyEndpointFilter())
app.logger.addFilter(ReadyEndpointFilter())

# --- Kubernetes Configuration ---
# This section attempts to configure the Kubernetes client.
# It first tries in-cluster configuration (if running inside a K8s pod).
# If that fails, it tries to load the local kubeconfig file (for development).
try:
    config.load_incluster_config()
    app.logger.info("Loaded in-cluster Kubernetes configuration.")
except config.ConfigException:
    try:
        config.load_kube_config()
        app.logger.info("Loaded local Kubernetes configuration (kubeconfig).")
    except config.ConfigException as e:
        app.logger.error(f"Could not configure Kubernetes client: {e}. Ensure KUBECONFIG is set or app is in-cluster.")
        # For a real app, you might want to prevent startup or have a clear error state.
        # Here, we'll let it proceed, and API calls will fail if K8s client isn't configured.

v1 = client.CoreV1Api()  # Kubernetes CoreV1API client

# Determine Kubernetes Namespace
# Reads from 'K8S_NAMESPACE' environment variable, defaults to 'default'.
KUBE_NAMESPACE = os.environ.get("K8S_NAMESPACE", "default")
KUBE_POD_NAME = os.environ.get("K8S_POD_NAME", "NOT-SET")
app.logger.info(f"Targeting Kubernetes namespace: {KUBE_NAMESPACE}")

# --- Log Archival Configuration ---
RETAIN_ALL_POD_LOGS = os.environ.get("RETAIN_ALL_POD_LOGS", "false").lower() == "true"
MAX_LOG_RETENTION_MINUTES = int(os.environ.get("MAX_LOG_RETENTION_MINUTES", "10080"))  # Default 7 days
ALLOW_PREVIOUS_LOG_PURGE = os.environ.get("ALLOW_PREVIOUS_LOG_PURGE", "true").lower() == "true"
LOG_DIR = "/logs"

if RETAIN_ALL_POD_LOGS:
    if not os.path.exists(LOG_DIR):
        try:
            os.makedirs(LOG_DIR)
            app.logger.info(f"Created log directory: {LOG_DIR}")
        except OSError as e:
            app.logger.error(f"Failed to create log directory {LOG_DIR}: {e}")
            # Potentially exit or disable previous pod logs if directory creation fails
            RETAIN_ALL_POD_LOGS = False  # Disable if cannot create dir

app.logger.info(f"Targeting Kubernetes namespace: {KUBE_NAMESPACE}")

app.secret_key = os.urandom(24)  # Required for session

# --- Start Background Jobs (if applicable) ---
if RETAIN_ALL_POD_LOGS:
    # Start the previous pod logs cleanup job
    import threading

    start_log_cleanup_job(LOG_DIR, MAX_LOG_RETENTION_MINUTES, app.logger)
    # Start the pod watcher and previous pod logs archiver job
    app.logger.info("Previous pod logs enabled. Starting pod watcher...")
    watch_thread = threading.Thread(
        target=watch_pods_and_archive,
        args=(KUBE_NAMESPACE, v1, LOG_DIR, app.logger),
        daemon=True,
    )
    watch_thread.name = "PodLogArchiverThread"
    watch_thread.start()
else:
    app.logger.info("Previous pod logs are disabled (RETAIN_ALL_POD_LOGS is false).")


def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        api_key = os.environ.get("API_KEY")
        if not api_key or api_key == "no-key":
            return f(*args, **kwargs)

        # Check query string first
        provided_key = request.args.get("api_key")
        # Then check header
        if not provided_key:
            provided_key = request.headers.get("X-API-Key")
        # Finally check session
        if not provided_key:
            provided_key = session.get("api_key")

        if not provided_key or provided_key != api_key:
            if request.headers.get("Accept") == "application/json":
                return jsonify({"error": "API key required"}), 401
            return render_template_string(
                """
                <form method="POST" action="/login">
                    <h2>API Key Required</h2>
                    <input type="text" name="api_key" placeholder="Enter API Key">
                    <button type="submit">Submit</button>
                </form>
            """
            )
        return f(*args, **kwargs)

    return decorated


@app.route("/login", methods=["POST"])
def login():
    api_key = os.environ.get("API_KEY")
    provided_key = request.form.get("api_key")

    if not api_key or api_key == "no-key":
        session["api_key"] = "no-key"
        app.logger.info("Login successful - no API key required")
        return redirect(request.referrer or "/")

    if provided_key == api_key:
        session["api_key"] = provided_key
        app.logger.info("Login successful with valid API key")
        return redirect(request.referrer or "/")

    app.logger.warning("Login failed - invalid API key provided")
    return jsonify({"error": "Invalid API key"}), 401


# --- Helper Functions ---
def parse_log_line(line_str):
    """
    Parses a log line that typically starts with an RFC3339Nano timestamp.
    Example: "2021-09-01T12:34:56.123456789Z This is the log message."
    Returns a dictionary {'timestamp': str, 'message': str} or
    {'timestamp': None, 'message': original_line} if no timestamp is parsed.
    """
    # Regex to capture RFC3339Nano timestamp (YYYY-MM-DDTHH:MM:SS.sssssssssZ)
    # and the rest of the line as the message.
    # The (\.\d+)? part handles optional fractional seconds.
    match = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?Z)\s(.*)", line_str)
    if match:
        timestamp_str = match.group(1)
        message_str = match.group(3).strip()  # Get the message part and strip any trailing whitespace
        return {"timestamp": timestamp_str, "message": message_str}
    else:
        # If no timestamp is found at the beginning, return the whole line as the message.
        return {"timestamp": None, "message": line_str.strip()}


# --- Routes ---
@app.route("/")
@require_api_key
def serve_index():
    """
    Serves the main HTML page for the log viewer.
    Assumes 'index.html' (the frontend code) is in the same directory as this script.
    """
    app.logger.info(f"Serving index.html for request from {request.remote_addr}")
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/pods", methods=["GET"])
def get_pods():
    """
    API endpoint to list pods in the configured Kubernetes namespace.
    Returns a JSON object with the namespace, a list of pod names with their containers,
    and the current pod name.
    """
    global KUBE_NAMESPACE, KUBE_POD_NAME
    app.logger.info(f"Request for /api/pods in namespace '{KUBE_NAMESPACE}'")

    exclude_self = request.args.get("exclude_self", "").lower() == "true"

    try:
        pod_list_response = v1.list_namespaced_pod(namespace=KUBE_NAMESPACE)
        pod_info = []
        for pod in pod_list_response.items:
            if exclude_self and pod.metadata.name == KUBE_POD_NAME:
                continue
            pod_name = pod.metadata.name
            containers = [container.name for container in pod.spec.containers]
            init_containers = [container.name for container in (pod.spec.init_containers or [])]

            # Add init containers with "init-" prefix to distinguish them
            for init_container in init_containers:
                pod_info.append(f"{pod_name}/init-{init_container}")

            # Add regular containers
            if len(containers) == 1 and not init_containers:
                pod_info.append(pod_name)
            else:
                # For pods with multiple containers or any init containers, add each container as pod/container
                for container in containers:
                    pod_info.append(f"{pod_name}/{container}")

        app.logger.info(f"Found {len(pod_info)} pod/container combinations in namespace '{KUBE_NAMESPACE}'")
        return jsonify(
            {
                "namespace": KUBE_NAMESPACE,
                "pods": pod_info,
                "current_pod": KUBE_POD_NAME,
            }
        )
    except ApiException as e:
        app.logger.error(f"Kubernetes API error fetching pods: {e.status} - {e.reason} - {e.body}")
        return jsonify({"message": f"Error fetching pods: {e.reason}"}), e.status
    except Exception as e:
        app.logger.error(f"Unexpected error fetching pods: {str(e)}")
        return jsonify({"message": f"An unexpected error occurred: {str(e)}"}), 500


@app.route("/ready", methods=["GET"])
def readiness_probe():
    """
    Readiness probe endpoint that checks if the /api/pods endpoint is working.
    Returns 200 if pods can be listed, 503 otherwise.
    Does not log requests to avoid log spam.
    """
    try:
        # Temporarily disable logging for this check
        original_level = app.logger.level
        app.logger.setLevel(logging.ERROR)

        # Try to list pods
        v1.list_namespaced_pod(namespace=KUBE_NAMESPACE)

        # Restore logging level
        app.logger.setLevel(original_level)
        return "", 200
    except Exception:
        # Restore logging level in case of error
        app.logger.setLevel(original_level)
        return "", 503


@app.route("/api/logs", methods=["GET"])
def get_logs():
    """
    API endpoint to fetch logs for a specific pod/container or all pods.
    Query Parameters:
        - pod_name (required): The name of the pod/container (format: 'pod' or 'pod/container') or 'all' for all pods.
        - sort_order (optional, default 'desc'): 'asc' (oldest first) or 'desc' (newest first).
        - tail_lines (optional, default '100'): Number of lines to fetch. '0' means all lines. When searching, all logs are fetched first, then filtered by search term, then limited by tail_lines.
        - search_string (optional): String to filter log messages by.
        - case_sensitive (optional, default 'false'): 'true' for case-sensitive search, 'false' for case-insensitive.
    Returns a JSON object with a list of log entries, each with 'timestamp', 'message', 'pod_name', and 'container_name'.
    """
    global KUBE_NAMESPACE, v1
    pod_name_req = request.args.get("pod_name")
    sort_order = request.args.get("sort_order", "desc").lower()
    tail_lines_str = request.args.get("tail_lines", "100")
    search_string = request.args.get("search_string", "").strip()
    case_sensitive = request.args.get("case_sensitive", "false").lower() == "true"

    app.logger.info(
        f"Request for /api/logs: pod='{pod_name_req}', sort='{sort_order}', lines='{tail_lines_str}', search='{search_string}'"
    )

    if not pod_name_req:
        app.logger.warning("Bad request to /api/logs: pod_name missing.")
        return jsonify({"message": "Pod name is required"}), 400

    try:
        tail_lines = int(tail_lines_str) if tail_lines_str != "0" else None
        if tail_lines is not None and tail_lines < 0:
            return jsonify({"message": "tail_lines must be non-negative or 0."}), 400
    except ValueError:
        return jsonify({"message": "Invalid number for tail_lines."}), 400

    # When searching, fetch more logs to ensure we don't miss results
    # If there's a search term, use a larger window or all logs
    k8s_tail_lines = None if search_string else tail_lines

    try:
        if pod_name_req == "all":
            pod_list_response = v1.list_namespaced_pod(namespace=KUBE_NAMESPACE)
            all_logs = []

            for pod in pod_list_response.items:
                if pod.metadata.name == KUBE_POD_NAME:
                    continue

                pod_name = pod.metadata.name

                # Process init containers first
                for init_container in pod.spec.init_containers or []:
                    container_name = f"init-{init_container.name}"
                    try:
                        log_data_stream = v1.read_namespaced_pod_log(
                            name=pod_name,
                            namespace=KUBE_NAMESPACE,
                            container=init_container.name,
                            timestamps=True,
                            tail_lines=k8s_tail_lines,
                            follow=False,
                            _preload_content=True,
                        )
                        raw_log_lines = log_data_stream.splitlines()
                        for line_str in raw_log_lines:
                            if not line_str:
                                continue
                            log_entry = parse_log_line(line_str)
                            if search_string:
                                search_text = log_entry["message"] if case_sensitive else log_entry["message"].lower()
                                search_term = search_string if case_sensitive else search_string.lower()
                                if search_term not in search_text:
                                    continue
                            log_entry["pod_name"] = pod_name
                            log_entry["container_name"] = container_name
                            all_logs.append(log_entry)
                    except ApiException as e:
                        app.logger.warning(
                            f"Could not fetch logs for pod {pod_name} init container {init_container.name}: {e.status} - {e.reason}"
                        )
                        all_logs.append(
                            {
                                "pod_name": pod_name,
                                "container_name": container_name,
                                "timestamp": None,
                                "message": f"Error fetching logs: {e.reason}",
                                "error": True,
                            }
                        )

                # Process regular containers
                for container in pod.spec.containers:
                    container_name = container.name
                    try:
                        log_data_stream = v1.read_namespaced_pod_log(
                            name=pod_name,
                            namespace=KUBE_NAMESPACE,
                            container=container_name,
                            timestamps=True,
                            tail_lines=k8s_tail_lines,
                            follow=False,
                            _preload_content=True,
                        )
                        raw_log_lines = log_data_stream.splitlines()
                        for line_str in raw_log_lines:
                            if not line_str:
                                continue
                            log_entry = parse_log_line(line_str)
                            if search_string:
                                search_text = log_entry["message"] if case_sensitive else log_entry["message"].lower()
                                search_term = search_string if case_sensitive else search_string.lower()
                                if search_term not in search_text:
                                    continue
                            log_entry["pod_name"] = pod_name
                            log_entry["container_name"] = container_name
                            all_logs.append(log_entry)
                    except ApiException as e:
                        app.logger.warning(
                            f"Could not fetch logs for pod {pod_name} container {container_name}: {e.status} - {e.reason}"
                        )
                        all_logs.append(
                            {
                                "pod_name": pod_name,
                                "container_name": container_name,
                                "timestamp": None,
                                "message": f"Error fetching logs: {e.reason}",
                                "error": True,
                            }
                        )

            all_logs.sort(
                key=lambda x: x.get("timestamp") or "0000-00-00T00:00:00Z",
                reverse=(sort_order == "desc"),
            )

            if tail_lines is not None and tail_lines > 0:
                if sort_order == "desc":
                    all_logs = all_logs[:tail_lines]
                else:
                    all_logs = all_logs[-tail_lines:]

            return jsonify({"logs": all_logs})
        else:  # Single pod/container
            # Split pod_name into pod and container if it contains a slash
            pod_name = pod_name_req
            container_name = None

            if "/" in pod_name_req:
                pod_name, container_name = pod_name_req.split("/", 1)
                # Check if this is an init container (prefixed with "init-")
                if container_name.startswith("init-"):
                    # Remove the "init-" prefix to get the actual container name
                    actual_container_name = container_name[5:]
                else:
                    actual_container_name = container_name
            else:
                actual_container_name = container_name

            log_data_stream = v1.read_namespaced_pod_log(
                name=pod_name,
                namespace=KUBE_NAMESPACE,
                container=actual_container_name,
                timestamps=True,
                tail_lines=k8s_tail_lines,
                follow=False,
                _preload_content=True,
            )

            raw_log_lines = log_data_stream.splitlines()
            processed_logs = []
            for line_str in raw_log_lines:
                if not line_str:
                    continue
                log_entry = parse_log_line(line_str)
                if search_string:
                    search_text = log_entry["message"] if case_sensitive else log_entry["message"].lower()
                    search_term = search_string if case_sensitive else search_string.lower()
                    if search_term not in search_text:
                        continue
                log_entry["pod_name"] = pod_name
                if container_name:
                    log_entry["container_name"] = container_name
                processed_logs.append(log_entry)

            processed_logs.sort(
                key=lambda x: x.get("timestamp") or "0000-00-00T00:00:00Z",
                reverse=(sort_order == "desc"),
            )

            if tail_lines is not None and tail_lines > 0:
                if sort_order == "desc":
                    processed_logs = processed_logs[:tail_lines]
                else:
                    processed_logs = processed_logs[-tail_lines:]

            return jsonify({"logs": processed_logs})

    except ApiException as e:
        app.logger.error(
            f"Kubernetes API error processing logs for '{pod_name_req}': {e.status} - {e.reason} - {e.body}"
        )
        error_message = e.reason
        if e.body:
            try:
                error_details = json.loads(e.body)
                error_message = error_details.get("message", e.reason)
            except json.JSONDecodeError:  # pragma: no cover
                error_message = f"{e.reason} (Details: {e.body[:200]})"
        return jsonify({"message": f"Error fetching logs: {error_message}"}), e.status
    except Exception as e:
        app.logger.error(
            f"Unexpected error processing logs for '{pod_name_req}': {str(e)}",
            exc_info=True,
        )
        return jsonify({"message": f"An unexpected error occurred: {str(e)}"}), 500


@app.route("/api/archived_pods", methods=["GET"])
@require_api_key
def get_archived_pods():
    """
    API endpoint to list previous pod log files.
    Only active if RETAIN_ALL_POD_LOGS is true.
    Returns a JSON list of pod/container names for which previous pod logs exist
    but are no longer running.
    """
    global LOG_DIR, RETAIN_ALL_POD_LOGS, KUBE_NAMESPACE, v1
    if not RETAIN_ALL_POD_LOGS:
        return (
            jsonify({"archived_pods": [], "message": "Previous pod logs are not enabled."}),
            200,
        )

    archived_pod_names = []
    if os.path.exists(LOG_DIR):
        try:
            # Get list of currently running pods to exclude from archived list
            running_pod_containers = set()
            try:
                pod_list_response = v1.list_namespaced_pod(namespace=KUBE_NAMESPACE)
                for pod in pod_list_response.items:
                    pod_name = pod.metadata.name
                    containers = [container.name for container in pod.spec.containers]
                    init_containers = [container.name for container in (pod.spec.init_containers or [])]

                    # Add init containers with "init-" prefix
                    for init_container in init_containers:
                        running_pod_containers.add(f"{pod_name}/init-{init_container}")

                    # Add regular containers
                    if len(containers) == 1 and not init_containers:
                        running_pod_containers.add(pod_name)
                    else:
                        # For pods with multiple containers or any init containers, add each container as pod/container
                        for container in containers:
                            running_pod_containers.add(f"{pod_name}/{container}")
                app.logger.info(f"Found {len(running_pod_containers)} currently running pod/container combinations.")
            except ApiException as e:
                app.logger.warning(f"Could not fetch running pods for archived filter: {e.status} - {e.reason}")
                # Continue with empty set - this will show all archived pods if we can't fetch running ones
                running_pod_containers = set()

            # List archived log files and exclude currently running pods
            # Use os.walk to search subdirectories since multi-container pods create subdirectories
            for root, dirs, files in os.walk(LOG_DIR):
                for filename in files:
                    if filename.endswith(".log"):
                        # Get relative path from LOG_DIR to construct pod/container name
                        file_path = os.path.join(root, filename)
                        relative_path = os.path.relpath(file_path, LOG_DIR)
                        # Remove .log extension to get pod/container name
                        pod_container = relative_path[:-4]
                        # Exclude current pod and any currently running pods
                        if KUBE_POD_NAME not in pod_container and pod_container not in running_pod_containers:
                            archived_pod_names.append(pod_container)

            app.logger.info(f"Found {len(archived_pod_names)} previous (non-running) pod/container logs in {LOG_DIR}.")
        except OSError as e:
            app.logger.error(f"Error listing previous pod logs directory {LOG_DIR}: {e}")
            return jsonify({"message": f"Error accessing log archive: {str(e)}"}), 500
    else:
        app.logger.info(f"Previous pod logs directory {LOG_DIR} does not exist.")

    return jsonify({"archived_pods": archived_pod_names})


@app.route("/api/archived_logs", methods=["GET"])
@require_api_key
def get_archived_logs():
    """
    API endpoint to fetch logs for a specific previous pod/container log file.
    Query Parameters:
        - pod_name (required): The name of the pod/container (format: 'pod' or 'pod/container').
        - sort_order (optional, default 'desc'): 'asc' or 'desc'.
        - tail_lines (optional, default '0'): Number of lines. '0' for all. When searching, all logs are searched first, then filtered by search term, then limited by tail_lines.
        - search_string (optional): String to filter log messages.
        - case_sensitive (optional, default 'false'): 'true' for case-sensitive search, 'false' for case-insensitive.
    """
    global LOG_DIR, RETAIN_ALL_POD_LOGS
    if not RETAIN_ALL_POD_LOGS:
        return (
            jsonify({"message": "Previous pod logs are not enabled."}),
            403,
        )  # Forbidden

    pod_name = request.args.get("pod_name")
    sort_order = request.args.get("sort_order", "desc").lower()
    tail_lines_str = request.args.get("tail_lines", "0")  # Default to all for previous
    search_string = request.args.get("search_string", "").strip()
    case_sensitive = request.args.get("case_sensitive", "false").lower() == "true"

    app.logger.info(
        f"Request for /api/archived_logs: pod='{pod_name}', sort='{sort_order}', lines='{tail_lines_str}', search='{search_string}'"
    )

    if not pod_name:
        app.logger.warning("Bad request to /api/archived_logs: pod_name missing.")
        return jsonify({"message": "Pod name is required for archived logs"}), 400

    log_file_path = os.path.join(LOG_DIR, f"{pod_name}.log")

    if not os.path.exists(log_file_path):
        app.logger.warning(f"Archived log file not found: {log_file_path}")
        return jsonify({"message": f"Previous log for pod/container {pod_name} not found."}), 404

    try:
        tail_lines = int(tail_lines_str) if tail_lines_str != "0" else None
        if tail_lines is not None and tail_lines < 0:
            return jsonify({"message": "tail_lines must be non-negative or 0."}), 400
    except ValueError:
        return jsonify({"message": "Invalid number for tail_lines."}), 400

    try:
        with open(log_file_path, "r", encoding="utf-8") as f:
            raw_log_lines = f.readlines()

        app.logger.info(f"Read {len(raw_log_lines)} lines from archived file {log_file_path}.")

        processed_logs = []
        for line_str in raw_log_lines:
            if not line_str.strip():
                continue
            log_entry = parse_log_line(line_str)
            if search_string:
                search_text = log_entry["message"] if case_sensitive else log_entry["message"].lower()
                search_term = search_string if case_sensitive else search_string.lower()
                if search_term not in search_text:
                    continue

            # Add pod and container information
            if "/" in pod_name:
                pod, container = pod_name.split("/", 1)
                log_entry["pod_name"] = pod
                log_entry["container_name"] = container
            else:
                log_entry["pod_name"] = pod_name

            processed_logs.append(log_entry)

        app.logger.info(f"{len(processed_logs)} lines after search filter for archived pod/container '{pod_name}'.")

        # Sort by timestamp
        processed_logs.sort(
            key=lambda x: x.get("timestamp") or "0000-00-00T00:00:00Z",
            reverse=(sort_order == "desc"),
        )

        if tail_lines is not None and tail_lines > 0:
            if sort_order == "desc":
                processed_logs = processed_logs[:tail_lines]
            else:
                processed_logs = processed_logs[-tail_lines:]

        return jsonify({"logs": processed_logs})

    except Exception as e:
        app.logger.error(
            f"Unexpected error fetching archived logs for pod/container '{pod_name}': {str(e)}",
            exc_info=True,
        )
        return jsonify({"message": f"An unexpected error occurred: {str(e)}"}), 500


@app.route("/api/logDirStats", methods=["GET"])
@require_api_key
def get_log_dir_stats():
    """
    API endpoint to get statistics about the log directory.
    Returns:
        - total_size_miBytes: Total size of all log files in miBytes
        - file_count: Number of log files
        - oldest_file_date: Creation date of the oldest log file
        - enabled: Whether log archiving is enabled
    """
    global LOG_DIR, RETAIN_ALL_POD_LOGS

    if not RETAIN_ALL_POD_LOGS:
        return jsonify({"enabled": False, "message": "Previous pod logs are not enabled."}), 200

    if not os.path.exists(LOG_DIR):
        return (
            jsonify(
                {
                    "enabled": True,
                    "total_size_mibytes": 0,
                    "file_count": 0,
                    "oldest_file_date": None,
                    "message": "Log directory does not exist.",
                }
            ),
            200,
        )

    try:
        total_size = 0
        file_count = 0
        oldest_date = None

        # Walk through directory recursively
        for root, _, files in os.walk(LOG_DIR):
            for filename in files:
                if filename.endswith(".log"):
                    file_path = os.path.join(root, filename)
                    file_stats = os.stat(file_path)

                    # Update total size
                    total_size += file_stats.st_size
                    file_count += 1

                    # Update oldest date
                    creation_time = file_stats.st_ctime
                    if oldest_date is None or creation_time < oldest_date:
                        oldest_date = creation_time

        # Convert oldest_date to ISO format if it exists
        oldest_date_iso = None
        if oldest_date is not None:
            from datetime import datetime

            oldest_date_iso = datetime.fromtimestamp(oldest_date).isoformat()

        return jsonify(
            {
                "enabled": True,
                "total_size_mibytes": total_size / 1024 / 1024,
                "file_count": file_count,
                "oldest_file_date": oldest_date_iso,
                "log_directory": LOG_DIR,
            }
        )

    except Exception as e:
        app.logger.error(f"Error getting log directory stats: {str(e)}", exc_info=True)
        return jsonify({"message": f"An unexpected error occurred: {str(e)}"}), 500


@app.route("/api/purgeCapability", methods=["GET"])
@require_api_key
def get_purge_capability():
    """
    API endpoint to check if previous log purging is allowed.
    Returns a JSON object indicating if purge functionality is available.
    """
    global RETAIN_ALL_POD_LOGS, ALLOW_PREVIOUS_LOG_PURGE

    return jsonify(
        {
            "purge_allowed": RETAIN_ALL_POD_LOGS and ALLOW_PREVIOUS_LOG_PURGE,
            "logs_enabled": RETAIN_ALL_POD_LOGS,
            "purge_enabled": ALLOW_PREVIOUS_LOG_PURGE,
        }
    )


@app.route("/api/purgePreviousLogs", methods=["POST"])
@require_api_key
def purge_previous_logs():
    """
    API endpoint to purge only previous pod log files.
    Returns a JSON object with the number of files deleted and any errors.
    """
    global LOG_DIR, RETAIN_ALL_POD_LOGS, ALLOW_PREVIOUS_LOG_PURGE

    if not RETAIN_ALL_POD_LOGS:
        return jsonify({"success": False, "message": "Previous pod logs are not enabled."}), 403

    if not ALLOW_PREVIOUS_LOG_PURGE:
        return jsonify({"success": False, "message": "Previous log purging is not allowed."}), 403

    try:
        from log_archiver import purge_previous_pod_logs

        deleted_count, error_count = purge_previous_pod_logs(LOG_DIR, app.logger)

        return jsonify(
            {
                "success": True,
                "deleted_count": deleted_count,
                "error_count": error_count,
                "message": f"Successfully purged {deleted_count} previous pod log files. {error_count} errors occurred.",
            }
        )
    except Exception as e:
        app.logger.error(f"Error purging previous pod logs: {str(e)}", exc_info=True)
        return jsonify({"success": False, "message": f"An unexpected error occurred: {str(e)}"}), 500


@app.route("/api/version", methods=["GET"])
def get_version():
    """
    API endpoint to get the application version.
    Returns a JSON object with the current version.
    """
    return jsonify({"version": __version__})


# --- Main Execution ---
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)
