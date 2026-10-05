"""Builds build/package/ from pipeline_src/.

`transformations/` is copied as-is, with no substitution. Glue job arguments don't
reach those files (SDP runs them through its own CLI wrapper), so bronze reads its
input path from the manifest's `configuration:` block via spark.conf.

pipeline_src/spark-pipeline.yml is the one file that carries deployment values. It's
static YAML the SDP runtime reads before any Python code executes. It's committed as a `{bucket}`/`{prefix}`/`{database}` template and rendered
here with str.format using the same PipelineStackProps fields the rest of the stack
uses, so bucket/prefix/database are defined in exactly one place (the stack's props)
and never hand-duplicated as literals elsewhere.

This script never mutates pipeline_src/ and is safe to re-run.
"""
from __future__ import annotations

import shutil
from pathlib import Path


def render_package(
    bucket: str,
    prefix: str,
    database: str,
    src_dir: Path,
    out_dir: Path,
) -> Path:
    """Build out_dir from src_dir's transformations/ plus a rendered manifest.

    Returns the output directory path.
    """
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)

    shutil.copytree(src_dir / "transformations", out_dir / "transformations")

    manifest_template = (src_dir / "spark-pipeline.yml").read_text()
    manifest = manifest_template.format(bucket=bucket, prefix=prefix, database=database)
    (out_dir / "spark-pipeline.yml").write_text(manifest)

    return out_dir


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--src", default="pipeline_src")
    parser.add_argument("--out", default="build/package")
    args = parser.parse_args()

    render_package(
        bucket=args.bucket,
        prefix=args.prefix,
        database=args.database,
        src_dir=Path(args.src),
        out_dir=Path(args.out),
    )
