"""NetObserv CLI — entry point for operator commands."""

from __future__ import annotations

import asyncio
import os

import typer
import uvicorn

from netobserv.config import get_settings, reset_settings_cache

app = typer.Typer(
    name="netobserv",
    help="NetObserv: Advanced Network Discovery and Topology Mapping Observability Engine",
    add_completion=False,
)
runtime_app = typer.Typer(name="runtime", help="Runtime control commands")
app.add_typer(runtime_app, name="runtime")


def _apply_runtime_overrides(**overrides: object) -> None:
    """Apply environment-backed overrides before settings are read."""
    env_map = {
        "host": "NETOBSERV_API_HOST",
        "port": "NETOBSERV_API_PORT",
        "workers": "NETOBSERV_API_WORKERS",
        "reload": "NETOBSERV_API_RELOAD",
        "log_level": "NETOBSERV_LOG_LEVEL",
        "database_url": "NETOBSERV_DATABASE_URL",
    }
    for key, value in overrides.items():
        if value is None or key not in env_map:
            continue
        os.environ[env_map[key]] = str(value)
    reset_settings_cache()


@app.command()
def serve(
    host: str | None = typer.Option(None, help="Bind host"),
    port: int | None = typer.Option(None, help="Bind port"),
    workers: int | None = typer.Option(None, help="Number of worker processes"),
    reload: bool | None = typer.Option(None, help="Enable auto-reload (dev only)"),
    log_level: str | None = typer.Option(None, help="Log level"),
) -> None:
    """Start the NetObserv API server using validated settings."""
    _apply_runtime_overrides(
        host=host,
        port=port,
        workers=workers,
        reload=reload,
        log_level=log_level,
    )
    settings = get_settings()

    uvicorn.run(
        "netobserv.api.app:create_app",
        factory=True,
        host=settings.api_host,
        port=settings.api_port,
        workers=settings.api_workers if not settings.api_reload else 1,
        reload=settings.api_reload,
        log_level=settings.log_level.lower(),
    )


@app.command()
def discover(
    inventory: str = typer.Argument(..., help="Path to inventory YAML/JSON file"),
    scope: str = typer.Option("global", help="Discovery scope label"),
    concurrency: int | None = typer.Option(None, help="Max concurrent connections"),
    read_only: bool | None = typer.Option(None, help="Read-only mode"),
) -> None:
    """Run a discovery pass from a static inventory file."""
    settings = get_settings()
    resolved_concurrency = concurrency or settings.discovery_default_concurrency
    resolved_read_only = settings.discovery_read_only if read_only is None else read_only

    typer.echo(f"Starting discovery from: {inventory}")
    asyncio.run(_run_discovery(inventory, scope, resolved_concurrency, resolved_read_only))


async def _run_discovery(
    inventory_path: str,
    scope: str,
    concurrency: int,
    read_only: bool,
) -> None:
    from netobserv.inventory_sources.static import StaticFileInventorySource
    from netobserv.observability.logging import configure_logging
    from netobserv.workflows.discovery import DiscoveryWorkflow
    from netobserv.workflows.manager import WorkflowManager

    settings = get_settings()
    configure_logging(level=settings.log_level, fmt="console")

    source = StaticFileInventorySource(inventory_path)
    targets = await source.load_targets()

    if not targets:
        typer.echo("No targets loaded from inventory. Exiting.")
        raise typer.Exit(code=1)

    typer.echo(f"Loaded {len(targets)} targets.")

    manager = WorkflowManager()
    workflow = DiscoveryWorkflow(
        manager=manager,
        concurrency_limit=concurrency,
        read_only=read_only,
    )

    workflow_id = await workflow.run(targets, scope=scope)
    status = await manager.get_status(workflow_id)

    typer.echo(f"\nWorkflow ID: {workflow_id}")
    typer.echo(f"Status: {status['status']}")

    for step in status.get("steps", []):
        typer.echo(
            f"  [{step['status']:>10}] {step['stage']:30} {step.get('elapsed_seconds', 0):.2f}s"
        )


@app.command(name="init-db")
def init_db_command(
    url: str | None = typer.Option(None, help="Database URL"),
) -> None:
    """Initialize the database schema using the configured database URL."""
    if url is not None:
        _apply_runtime_overrides(database_url=url)

    async def _init() -> None:
        from netobserv.storage.database import configure_storage, init_db as _init_db

        configure_storage(get_settings())
        await _init_db()
        typer.echo("Database initialized.")

    asyncio.run(_init())


@app.command()
def version() -> None:
    """Print the NetObserv version."""
    from netobserv import __version__

    typer.echo(f"NetObserv {__version__}")


if __name__ == "__main__":
    app()


def _build_runtime_components():
    from netobserv.runtime.bootstrap import build_runtime_components
    from netobserv.storage.database import configure_storage, init_runtime_db
    settings = get_settings()
    configure_storage(settings)
    asyncio.run(init_runtime_db())
    return build_runtime_components(settings)


@runtime_app.command("status")
def runtime_status(limit: int = typer.Option(20, help="Max jobs to show")) -> None:
    """Show runtime job status from the durable repository."""
    components = _build_runtime_components()
    from netobserv.runtime.repository import JobSearchFilter
    jobs = components.repository.list_jobs(JobSearchFilter(limit=limit))
    typer.echo(f"Runtime jobs: {len(jobs)}")
    for job in jobs:
        typer.echo(f"{job.job_id}  {job.job_type}  {job.state.value}  queue={job.queue_name}  attempt={job.attempt_number}")


@runtime_app.command("cancel")
def runtime_cancel(job_id: str = typer.Argument(..., help="Runtime job ID")) -> None:
    """Request cooperative cancellation for a runtime job."""
    components = _build_runtime_components()
    decision = components.dispatcher.cancel(job_id)
    typer.echo(f"Cancellation requested for {decision.job.job_id} (state={decision.job.state.value})")


@runtime_app.command("start")
def runtime_start(
    max_iterations: int = typer.Option(1, help="Number of service loop iterations to run"),
) -> None:
    """Run the runtime service loop.

    Important honesty note: domain-specific stage executors are not fully wired
    yet, so this command is primarily for scheduler/recovery/status plumbing.
    """
    components = _build_runtime_components()
    from netobserv.runtime.service import RuntimeServiceConfig
    result = components.service.run(
        config=RuntimeServiceConfig(max_iterations=max_iterations)
    )
    typer.echo(
        f"Runtime service completed: iterations={result.iterations}, "
        f"worker_runs={result.worker_runs}, scheduler_runs={result.scheduler_runs}, "
        f"errors={len(result.errors)}"
    )
