import logging
import os
import traceback
import uuid

from datafeeds import db, config
from datafeeds.common.index import (
    _get_es_connection,
    get_index_doc,
)
from datafeeds.common.typing import Status
from datafeeds.db import dbtask
from datafeeds.smd.authorization import (
    authorize,
    SiteError,
    LoginFailure,
    AccountParseFailure,
    NoAccountsFailure,
)
from datafeeds.smd.models import Workflow, State, AuthorizationSummary

log = logging.getLogger(__name__)


TASK_ID = os.environ.get("AWS_BATCH_JOB_ID", str(uuid.uuid4()))


def index_provisioning_operation(
    workflow_oid: int,
    operation: str,
    status: Status,
):
    """Upload the logs for this task to elasticsearch for later analysis."""
    es = _get_es_connection()
    doc, index = get_index_doc(TASK_ID)
    if not doc:
        # Make a document with fundamental information about the run.
        doc = dict(
            scraper="smd-automated-provisioner",
            workflow_oid=workflow_oid,
            operation=operation,
            status=str(status.name),
        )
    try:
        with open(config.LOGPATH, "r") as f:
            log_contents = f.read()
        doc["log"] = log_contents
        es.index(index, doc_type="_doc", id=TASK_ID, body=doc)
    except:  # noqa E722
        log.exception("Failed to upload run logs to elasticsearch.")
        return

    log.info("Successfully uploaded run logs to elasticsearch.")


@dbtask(application_name="datafeeds.smd.tasks.run_authorization_step")
def run_authorization_step(workflow_oid) -> Status:
    """Authorize the Share My Data account associated with this workflow, if it isn"t already."""

    workflow = db.session.query(Workflow).get(workflow_oid)

    if workflow is None:
        log.error(
            "The SMD authorization scraper was triggered for a non-existing workflow (%s).",
            workflow_oid,
        )
        return Status.FAILED

    username = workflow.credential.username
    password = workflow.credential.password
    snapmeter_account = workflow.parent.account
    provider = "gridium" if snapmeter_account.domain == "gridium.com" else "ce"

    status = Status.FAILED
    try:
        results = authorize(username, password, provider)
        n_accounts = len(results.accounts)
        n_meters = sum(len(a.meters) for a in results.accounts)

        a_noun = "account" if n_accounts == 1 else "accounts"
        m_noun = "service" if n_meters == 1 else "services"

        message = "Authorization completed successfully. Identified %s %s and %s %s."
        workflow.create_event(
            State.authorized,
            message % (n_accounts, a_noun, n_meters, m_noun),
            metadata=results,
        )

        if results.subscription_id is not None:
            # This account was already authorized (say, by a person),
            # we can mark the verification step as complete.
            message = (
                "Share My Data authorization confirmed, subscription ID %s."
                % results.subscription_id
            )
            workflow.create_event(State.verified, message, metadata=results)

        status = Status.SUCCEEDED
    except LoginFailure as error:
        log.error(
            "SMD authorization failed for workflow (%s) - Unable to log in to Share My Data."
        )
        workflow.create_event(State.authorized, str(error), error=True)
    except NoAccountsFailure as e:
        workflow.create_event(State.authorized, str(e), error=True)
        message = (
            "No accounts/services are associated with this login in ShareMyData; "
            "there is nothing to provision from these credentials."
        )
        workflow.create_event(State.failed, message)
    except (SiteError, AccountParseFailure) as e:
        log.exception("SMD authorization failed for workflow (%s).", workflow_oid)
        workflow.create_event(State.authorized, str(e), error=True)
    except:  # noqa E722
        message = (
            "SMD authorization failed for workflow (%s) due to an unexpected issue."
            % workflow_oid
        )
        log.exception(message)
        metadata = AuthorizationSummary(error=traceback.format_exc())
        workflow.create_event(State.authorized, message, error=True, metadata=metadata)

    db.session.add(workflow)
    db.session.flush()

    index_provisioning_operation(workflow_oid, "Authorizing.", status)
    return status


@dbtask(application_name="datafeeds.smd.tasks.verify")
def run_validation_step(workflow_oid) -> Status:
    """Determine whether the input CE credentials successfully authorized SMD."""

    workflow = db.session.query(Workflow).get(workflow_oid)

    if workflow is None:
        log.error(
            "The SMD authorization scraper was triggered for a non-existing workflow (%s).",
            workflow_oid,
        )
        return Status.FAILED

    username = workflow.credential.username
    password = workflow.credential.password
    snapmeter_account = workflow.parent.account
    provider = "gridium" if snapmeter_account.domain == "gridium.com" else "ce"

    error_message = (
        "We were unable to determine the Share My Data subscription ID. "
        "PG&E may still be processing this authorization. We will retry later."
    )

    status = Status.FAILED
    try:
        results = authorize(username, password, provider, verify=True)

        if results.subscription_id is None:
            workflow.create_event(
                State.verified, error_message, error=True, metadata=results
            )
        else:
            message = (
                "Share My Data authorization confirmed, subscription ID %s."
                % results.subscription_id
            )
            workflow.create_event(State.verified, message, metadata=results)
        status = Status.SUCCEEDED
    except (SiteError, LoginFailure, AccountParseFailure, NoAccountsFailure):
        log.exception("SMD authorization check failed for workflow (%s).", workflow_oid)
        workflow.create_event(State.verified, error_message, error=True)
    except Exception:  # noqa E722
        message = (
            "Share My Data authorization failed for workflow (%s) due to an unexpected issue."
            % workflow_oid
        )
        log.exception(message)
        metadata = AuthorizationSummary(error=traceback.format_exc())
        workflow.create_event(State.verified, message, error=True, metadata=metadata)

    db.session.add(workflow)
    db.session.flush()

    index_provisioning_operation(workflow_oid, "Verifying authorization.", status)
    return status
