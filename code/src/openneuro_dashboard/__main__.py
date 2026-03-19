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
    import asyncio

    from .fetch_graphql import fetch_and_write as _fetch_and_write
    from .fetch_graphql import validate_output as _validate_output

    asyncio.run(
        _fetch_and_write(
            output_dir,
            page_size,
            prefetch,
            dry_run,
            verbose,
            max_datasets,
        )
    )

    if validate:
        _validate_output(output_dir)


@app.command()
def check_github(
    output_dir: OutputDir = Path("data"),
    verbose: Verbose = False,
    concurrency: Annotated[
        int, typer.Option("--concurrency", help="Number of concurrent requests.")
    ] = 10,
) -> None:
    """Check GitHub repository status for each dataset."""
    import asyncio

    from .check_github import check_all_datasets as _check_all_datasets

    asyncio.run(_check_all_datasets(output_dir, concurrency, verbose))


@app.command()
def check_s3_version(
    output_dir: OutputDir = Path("data"),
    verbose: Verbose = False,
    concurrency: Annotated[
        int, typer.Option("--concurrency", help="Number of concurrent S3 requests.")
    ] = 20,
) -> None:
    """Check S3 version consistency for each dataset."""
    import asyncio

    from .check_s3_version import check_all_datasets as _check_all_datasets

    asyncio.run(_check_all_datasets(output_dir, concurrency, verbose))


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
    import asyncio

    from .check_s3_files import check_all_datasets as _check_all_datasets

    asyncio.run(
        _check_all_datasets(
            output_dir,
            cache_dir.expanduser(),
            git_concurrency,
            s3_concurrency,
            verbose,
        )
    )


@app.command()
def summarize(
    output_dir: OutputDir = Path("data"),
    verbose: Verbose = False,
) -> None:
    """Summarize collected data into dashboard JSON."""
    from .summarize import generate_summary

    generate_summary(output_dir)


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
    import asyncio

    from .check_github import check_all_datasets as _check_github
    from .check_s3_files import check_all_datasets as _check_s3_files
    from .check_s3_version import check_all_datasets as _check_s3_version
    from .fetch_graphql import fetch_and_write as _fetch_graphql
    from .summarize import generate_summary as _summarize

    resolved_cache = cache_dir.expanduser()

    async def _run() -> None:
        await _fetch_graphql(output_dir, verbose=verbose, max_datasets=max_datasets)
        await _check_github(output_dir, verbose=verbose)
        await _check_s3_version(output_dir, verbose=verbose)
        await _check_s3_files(output_dir, resolved_cache, verbose=verbose)

    asyncio.run(_run())
    _summarize(output_dir)


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
    from .gen_data import git_tree, github, graphql, s3_diff, s3_version
    from .summarize import generate_summary

    if seed is not None:
        import random

        random.seed(seed)

    graphql.generate(output_dir, num_datasets)
    github.generate(output_dir)
    s3_version.generate(output_dir)
    git_tree.generate(output_dir)
    s3_diff.generate(output_dir)
    generate_summary(output_dir)


if __name__ == "__main__":
    app()
