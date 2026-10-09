"""
Compile the Dataform repository from a Git branch and run all its actions.

Dataform orders the actions by their ref() dependencies (bronze -> silver -> gold)
and runs each assertion after the table it checks, so no order is given here.
"""

from __future__ import annotations

import argparse
import time

from google.cloud import dataform_v1

PROJECT_ID = "deloitte-ha"
LOCATION = "us-central1"
REPOSITORY = "deloitte-ha"
GIT_BRANCH = "main"
SERVICE_ACCOUNT = f"dataform-runner@{PROJECT_ID}.iam.gserviceaccount.com"
POLL_SECONDS = 10

State = dataform_v1.WorkflowInvocation.State
FINAL_STATES = {State.SUCCEEDED, State.FAILED, State.CANCELLED}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Dataform workflow from a Git branch.")
    parser.add_argument("--project", default=PROJECT_ID)
    parser.add_argument("--location", default=LOCATION)
    parser.add_argument("--repository", default=REPOSITORY)
    parser.add_argument("--branch", default=GIT_BRANCH)
    parser.add_argument("--service-account", default=SERVICE_ACCOUNT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    client = dataform_v1.DataformClient()
    repository = client.repository_path(args.project, args.location, args.repository)

    compilation = client.create_compilation_result(
        parent=repository,
        compilation_result=dataform_v1.CompilationResult(git_commitish=args.branch),
    )
    if compilation.compilation_errors:
        for error in compilation.compilation_errors:
            print(f"  {error.path}: {error.message}")
        raise RuntimeError("Dataform compilation failed")
    print(f"Compiled {args.branch} at commit {compilation.resolved_git_commit_sha[:8]}")

    invocation = client.create_workflow_invocation(
        parent=repository,
        workflow_invocation=dataform_v1.WorkflowInvocation(
            compilation_result=compilation.name,
            invocation_config=dataform_v1.InvocationConfig(service_account=args.service_account),
        ),
    )
    print(f"Started {invocation.name}")

    while invocation.state not in FINAL_STATES:
        time.sleep(POLL_SECONDS)
        invocation = client.get_workflow_invocation(name=invocation.name)

    actions = client.query_workflow_invocation_actions(
        request=dataform_v1.QueryWorkflowInvocationActionsRequest(name=invocation.name)
    )
    for action in actions:
        line = f"  {action.state.name:<10} {action.target.schema}.{action.target.name}"
        if action.failure_reason:
            line += f"  {action.failure_reason}"
        print(line)

    if invocation.state != State.SUCCEEDED:
        raise RuntimeError(f"Dataform run ended in state {invocation.state.name}")
    print("Dataform run succeeded")


if __name__ == "__main__":
    main()
