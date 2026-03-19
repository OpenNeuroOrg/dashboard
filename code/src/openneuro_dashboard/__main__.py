"""OpenNeuro Dashboard CLI."""

from pathlib import Path
from typing import Annotated, Optional

import typer

app = typer.Typer(
    name="openneuro-dashboard",
    help="Data-population pipeline for the OpenNeuro dashboard.",
)

OutputDir = Annotated[
    Path,
    typer.Option("--output-dir", help="Directory to write output data."),
]
Verbose = Annotated[
    bool,
    typer.Option("--verbose", "-v", help="Enable verbose output."),
]


@app.command()
def fetch_graphql(
    output_dir: OutputDir = Path("data"),
    verbose: Verbose = False,
    page_size: Annotated[
        int, typer.Option("--page-size", help="Number of datasets per page.")
    ] = 100,
    prefetch: Annotated[
        int, typer.Option("--prefetch", help="Number of pages to prefetch.")
    ] = 2,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Run without writing files.")
    ] = False,
    validate: Annotated[
        bool, typer.Option("--validate", help="Validate fetched data.")
    ] = False,
    max_datasets: Annotated[
        Optional[int],
        typer.Option("--max-datasets", help="Maximum number of datasets to fetch."),
    ] = None,
) -> None:
    """Fetch dataset metadata from the OpenNeuro GraphQL API."""
    typer.echo("fetch-graphql: not yet implemented")
    raise typer.Exit(1)


@app.command()
def check_github(
    output_dir: OutputDir = Path("data"),
    verbose: Verbose = False,
    concurrency: Annotated[
        int, typer.Option("--concurrency", help="Number of concurrent requests.")
    ] = 10,
) -> None:
    """Check GitHub repository status for each dataset."""
    typer.echo("check-github: not yet implemented")
    raise typer.Exit(1)


@app.command()
def check_s3_version(
    output_dir: OutputDir = Path("data"),
    verbose: Verbose = False,
    concurrency: Annotated[
        int, typer.Option("--concurrency", help="Number of concurrent S3 requests.")
    ] = 20,
) -> None:
    """Check S3 version consistency for each dataset."""
    typer.echo("check-s3-version: not yet implemented")
    raise typer.Exit(1)


@app.command()
def check_s3_files(
    output_dir: OutputDir = Path("data"),
    verbose: Verbose = False,
    cache_dir: Annotated[
        Path,
        typer.Option("--cache-dir", help="Directory to cache git repositories."),
    ] = Path("~/.cache/openneuro-dashboard/repos"),
    git_concurrency: Annotated[
        int, typer.Option("--git-concurrency", help="Number of concurrent git ops.")
    ] = 10,
    s3_concurrency: Annotated[
        int, typer.Option("--s3-concurrency", help="Number of concurrent S3 requests.")
    ] = 20,
) -> None:
    """Check S3 files against git tree for each dataset."""
    typer.echo("check-s3-files: not yet implemented")
    raise typer.Exit(1)


@app.command()
def summarize(
    output_dir: OutputDir = Path("data"),
    verbose: Verbose = False,
) -> None:
    """Summarize collected data into dashboard JSON."""
    typer.echo("summarize: not yet implemented")
    raise typer.Exit(1)


@app.command()
def run_all(
    output_dir: OutputDir = Path("data"),
    verbose: Verbose = False,
    cache_dir: Annotated[
        Path,
        typer.Option("--cache-dir", help="Directory to cache git repositories."),
    ] = Path("~/.cache/openneuro-dashboard/repos"),
    max_datasets: Annotated[
        Optional[int],
        typer.Option("--max-datasets", help="Maximum number of datasets to process."),
    ] = None,
) -> None:
    """Run the full data-population pipeline."""
    typer.echo("run-all: not yet implemented")
    raise typer.Exit(1)


@app.command()
def gen_data(
    output_dir: OutputDir = Path("data"),
    verbose: Verbose = False,
    num_datasets: Annotated[
        int, typer.Option("--num-datasets", help="Number of datasets to generate.")
    ] = 50,
    seed: Annotated[
        Optional[int],
        typer.Option("--seed", help="Random seed for reproducibility."),
    ] = None,
) -> None:
    """Generate synthetic test data."""
    typer.echo("gen-data: not yet implemented")
    raise typer.Exit(1)


if __name__ == "__main__":
    app()
