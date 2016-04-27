"""
This module is for the following use case:

 - Talos jobs:

    * Trigger talos jobs twice if they are from PGO build.
"""
import logging

from mozci import query_jobs
from pulse_actions.utils.misc import filter_invalid_builders
from mozci.errors import PushlogError
from mozci.mozci import (
    trigger_job
)
from mozci.sources import buildjson
from requests.exceptions import ConnectionError

LOG = logging.getLogger()


def on_event(data, message, dry_run):
    """Trigger talos jobs twice in PGO builds."""
    # Cleaning mozci caches
    buildjson.BUILDS_CACHE = {}
    query_jobs.JOBS_CACHE = {}
    payload = data["payload"]
    status = payload["status"]
    buildername = payload["buildername"]

    if "pgo" in buildername and ("mozilla-inbound" in buildername or "fx-team" in buildername):
        buildername = filter_invalid_builders(buildername)

        # Treeherder can send us invalid builder names
        # https://bugzilla.mozilla.org/show_bug.cgi?id=1242038
        if buildername is None:
            if not dry_run:
                # We need to ack the message to remove it from our queue
                message.ack()
            return

        revision = payload["revision"]

        try:

            trigger_job(
                revision=revision,
                buildername=buildername,
                times=2,
                dry_run=dry_run,
                trigger_build_if_missing=False
            )

            if not dry_run:
                # We need to ack the message to remove it from our queue
                message.ack()

        except ConnectionError:
            # The message has not been acked so we will try again
            LOG.warning("Connection error. Trying again")

        except PushlogError, e:
            # Unable to retrieve pushlog data. Please check repo_url and revision specified.
            LOG.warning(str(e))

        except Exception, e:
            # The message has not been acked so we will try again
            LOG.warning(str(e))
            raise
    else:
        if not dry_run:
            # We need to ack the message to remove it from our queue
            message.ack()

        LOG.debug("'%s' with status %i. Nothing to be done.",
                  buildername, status)
