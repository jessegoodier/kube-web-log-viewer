import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone

import requests
from kubernetes import client
from kubernetes.client.rest import ApiException

# Create logger for this module
logger = logging.getLogger("log_archiver")


def delete_old_logs(log_dir, max_age_minutes, logger):
    """
    Deletes log files in the specified directory older than max_age_minutes.
    Uses file creation time to determine age.
    """
    if not os.path.exists(log_dir):
        logger.warning(f"Log directory {log_dir} does not exist. Skipping cleanup.")
        return

    logger.info(f"Starting cleanup of logs older than {max_age_minutes} minutes in {log_dir}...")
    cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
    deleted_count = 0
    error_count = 0

    for filename in os.listdir(log_dir):
        if filename.endswith(".log"):  # Process only .log files
            file_path = os.path.join(log_dir, filename)
            try:
                # Get both creation and modification times
                file_stats = os.stat(file_path)
                file_creation_time = datetime.fromtimestamp(file_stats.st_ctime, timezone.utc)
                file_mod_time = datetime.fromtimestamp(file_stats.st_mtime, timezone.utc)

                if file_creation_time < cutoff_time:
                    os.remove(file_path)
                    logger.info(
                        f"Deleted old log file: {file_path}\n"
                        f"  Created: {file_creation_time}\n"
                        f"  Last Modified: {file_mod_time}\n"
                        f"  Age: {(datetime.now(timezone.utc) - file_creation_time).total_seconds() / 60:.1f} minutes"
                    )
                    deleted_count += 1
                else:
                    logger.debug(
                        f"Keeping log file: {file_path}\n"
                        f"  Created: {file_creation_time}\n"
                        f"  Last Modified: {file_mod_time}\n"
                        f"  Age: {(datetime.now(timezone.utc) - file_creation_time).total_seconds() / 60:.1f} minutes"
                    )
            except OSError as e:
                logger.error(f"Error deleting file {file_path}: {e}")
                error_count += 1
            except Exception as e:
                logger.error(f"Unexpected error processing file {file_path}: {e}")
                error_count += 1
    logger.info(f"Log cleanup finished. Deleted: {deleted_count}, Errors: {error_count}")


def start_log_cleanup_job(log_dir, max_age_minutes, logger, interval_minutes=10):
    """
    Starts a periodic job to delete old logs.
    Runs in a daemon thread so it doesn't block application exit.
    """
    logger.info(f"Initializing log cleanup job. Will run every {interval_minutes} minutes.")

    def job():
        while True:
            try:
                delete_old_logs(log_dir, max_age_minutes, logger)
            except Exception as e:
                logger.error(f"Unhandled exception in log cleanup job: {e}", exc_info=True)
            time.sleep(interval_minutes * 60)

    thread = threading.Thread(target=job, daemon=True)
    thread.name = "LogCleanupThread"
    thread.start()
    logger.info("Log cleanup job thread started.")


# --- Log Archival Functions ---

# Dictionary to keep track of active log archival threads and their stop events
# Key: pod_name, Value: {"thread": threading.Thread, "stop_event": threading.Event}
active_archivals = {}
archival_lock = threading.Lock()  # To protect access to active_archivals


def get_pod_logs(v1, namespace, pod_name, container_name=None):
    """
    Fetch logs for a specific pod and container.
    Returns the raw log data as a string.
    """
    try:
        return v1.read_namespaced_pod_log(
            name=pod_name,
            namespace=namespace,
            container=container_name,
            timestamps=True,
            follow=False,
            _preload_content=True,
        )
    except ApiException as e:
        # Reduce log noise for containers that aren't ready yet
        if e.status == 400 and e.body:
            try:
                import json
                error_details = json.loads(e.body)
                message = error_details.get("message", "").lower()
                if ("waiting to start" in message or "containercreating" in message):
                    logger.debug(f"Container {pod_name}/{container_name} not ready for log archival: {message}")
                    return None
            except json.JSONDecodeError:
                pass
        logger.error(f"Error fetching logs for pod {pod_name} container {container_name}: {e}")
        return None


def archive_pod_logs(v1, namespace, pod_name, log_dir):
    """
    Archive logs for all containers and init containers in a pod.
    """
    try:
        pod = v1.read_namespaced_pod(name=pod_name, namespace=namespace)
        containers = pod.spec.containers or []
        init_containers = pod.spec.init_containers or []

        # Archive init container logs first
        for init_container in init_containers:
            container_name = init_container.name
            log_data = get_pod_logs(v1, namespace, pod_name, container_name)
            if log_data:
                # Always use pod/container format with "init-" prefix for init containers
                filename = f"{pod_name}/init-{container_name}.log"

                log_path = os.path.join(log_dir, filename)
                os.makedirs(os.path.dirname(log_path), exist_ok=True)

                with open(log_path, "w") as f:
                    f.write(log_data)
                logger.info(f"Archived logs for pod {pod_name} init container {container_name}")

        # Archive regular container logs
        for container in containers:
            container_name = container.name
            log_data = get_pod_logs(v1, namespace, pod_name, container_name)
            if log_data:
                # Always use pod/container format
                filename = f"{pod_name}/{container_name}.log"

                log_path = os.path.join(log_dir, filename)
                os.makedirs(os.path.dirname(log_path), exist_ok=True)

                with open(log_path, "w") as f:
                    f.write(log_data)
                logger.info(f"Archived logs for pod {pod_name} container {container_name}")
    except ApiException as e:
        logger.error(f"Error archiving logs for pod {pod_name}: {e}")


def get_log_dir_stats(log_dir):
    """
    Get statistics about the log directory.
    Returns a tuple of (total_size_bytes, file_count, oldest_date)
    """
    if not os.path.exists(log_dir):
        return 0, 0, None

    total_size = 0
    file_count = 0
    oldest_date = None

    for filename in os.listdir(log_dir):
        if filename.endswith(".log"):
            file_path = os.path.join(log_dir, filename)
            file_stats = os.stat(file_path)

            # Update total size
            total_size += file_stats.st_size
            file_count += 1

            # Update oldest date
            creation_time = file_stats.st_ctime
            if oldest_date is None or creation_time < oldest_date:
                oldest_date = creation_time

    return total_size, file_count, oldest_date


def watch_pods_and_archive(namespace, v1, log_dir, logger):
    """
    Watch for pod changes and archive logs when pods are terminated.
    """
    logger.info(f"Starting pod watcher for namespace {namespace}")

    while True:
        try:
            # Get all pods in the namespace
            pod_list = v1.list_namespaced_pod(namespace=namespace)

            # Archive logs for each pod
            for pod in pod_list.items:
                if pod.metadata.name != os.environ.get("K8S_POD_NAME", "NOT-SET"):
                    archive_pod_logs(v1, namespace, pod.metadata.name, log_dir)

            # Get log directory statistics from the API endpoint
            try:
                response = requests.get("http://localhost:5001/api/logDirStats")
                if response.status_code == 200:
                    stats = response.json()
                    if stats.get("enabled"):
                        logger.info(
                            f"Log directory stats - Total size: {stats['total_size_mibytes']:.2f} MiB, "
                            f"Files: {stats['file_count']}, Oldest file: {stats['oldest_file_date']}"
                        )
            except Exception as e:
                logger.warning(f"Could not fetch log directory stats: {e}")

            # Sleep for a while before checking again
            time.sleep(60)  # Check every minute

        except Exception as e:
            logger.error(f"Error in pod watcher: {e}")
            time.sleep(60)  # Wait before retrying


def purge_previous_pod_logs(log_dir, logger):
    """
    Deletes only the previous pod log files in the specified directory.
    Returns a tuple of (deleted_count, error_count)
    """
    if not os.path.exists(log_dir):
        logger.warning(f"Log directory {log_dir} does not exist. Nothing to purge.")
        return 0, 0

    logger.info(f"Starting purge of previous pod logs in {log_dir}...")
    deleted_count = 0
    error_count = 0

    # Get list of current pod/container combinations
    try:
        v1 = client.CoreV1Api()
        namespace = os.environ.get("K8S_NAMESPACE", "default")
        pod_list = v1.list_namespaced_pod(namespace=namespace)
        current_pod_containers = set()

        for pod in pod_list.items:
            pod_name = pod.metadata.name
            containers = [container.name for container in pod.spec.containers]
            init_containers = [container.name for container in (pod.spec.init_containers or [])]

            # Add init containers with "init-" prefix
            for init_container in init_containers:
                current_pod_containers.add(f"{pod_name}/init-{init_container}")

            # Add regular containers (always use pod/container format)
            for container in containers:
                current_pod_containers.add(f"{pod_name}/{container}")

    except Exception as e:
        logger.error(f"Error getting current pod list: {e}")
        return 0, 1

    # Use os.walk to search subdirectories since multi-container pods create subdirectories
    for root, _, files in os.walk(log_dir):
        for filename in files:
            if filename.endswith(".log"):  # Process only .log files
                file_path = os.path.join(root, filename)
                try:
                    # Get relative path from log_dir to construct pod/container name
                    relative_path = os.path.relpath(file_path, log_dir)
                    # Remove .log extension to get pod/container name
                    pod_container = relative_path[:-4]

                    # Only delete if this pod/container is not in the current pod list
                    if pod_container not in current_pod_containers:
                        os.remove(file_path)
                        logger.info(f"Purged previous pod log file: {file_path}")
                        deleted_count += 1
                except OSError as e:
                    logger.error(f"Error purging file {file_path}: {e}")
                    error_count += 1
                except Exception as e:
                    logger.error(f"Unexpected error processing file {file_path}: {e}")
                    error_count += 1

    logger.info(f"Previous pod logs purge finished. Deleted: {deleted_count}, Errors: {error_count}")
    return deleted_count, error_count
