from pathlib import Path

from scripts.package_pipeline import render_package

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PIPELINE_SRC_DIR = REPO_ROOT / "pipeline_src"


def test_render_package_generates_manifest_from_props(tmp_path):
    out_dir = tmp_path / "package"

    render_package(
        bucket="my-bucket",
        prefix="my-prefix",
        database="my_db",
        src_dir=PIPELINE_SRC_DIR,
        out_dir=out_dir,
    )

    manifest_text = (out_dir / "spark-pipeline.yml").read_text()
    assert "database: my_db" in manifest_text
    assert "catalog:" not in manifest_text
    assert "storage: s3://my-bucket/my-prefix/state/" in manifest_text


def test_render_package_root_layout(tmp_path):
    out_dir = tmp_path / "package"

    render_package(
        bucket="my-bucket",
        prefix="my-prefix",
        database="my_db",
        src_dir=PIPELINE_SRC_DIR,
        out_dir=out_dir,
    )

    assert (out_dir / "spark-pipeline.yml").is_file()
    assert (out_dir / "transformations").is_dir()
    assert (out_dir / "transformations" / "01_bronze.py").is_file()
    assert (out_dir / "transformations" / "02_silver.py").is_file()
    assert (out_dir / "transformations" / "03_gold.sql").is_file()


def test_render_package_manifest_carries_input_path(tmp_path):
    out_dir = tmp_path / "package"

    render_package(
        bucket="my-bucket",
        prefix="my-prefix",
        database="my_db",
        src_dir=PIPELINE_SRC_DIR,
        out_dir=out_dir,
    )

    # The input path reaches bronze through the manifest's `configuration:` block;
    # the copied transformation file stays free of bucket/prefix values. It must be a
    # directory, not a specific file: bronze_orders streams via spark.readStream, whose
    # file source requires `.load(path)` to point at a directory it can monitor.
    manifest_text = (out_dir / "spark-pipeline.yml").read_text()
    assert "orders.input.path: s3://my-bucket/my-prefix/input/" in manifest_text
    bronze_text = (out_dir / "transformations" / "01_bronze.py").read_text()
    assert "getResolvedOptions" not in bronze_text
    assert "my-bucket" not in bronze_text


def test_render_package_is_idempotent(tmp_path):
    out_dir = tmp_path / "package"

    render_package(
        bucket="my-bucket",
        prefix="my-prefix",
        database="my_db",
        src_dir=PIPELINE_SRC_DIR,
        out_dir=out_dir,
    )
    render_package(
        bucket="my-bucket",
        prefix="my-prefix",
        database="my_db",
        src_dir=PIPELINE_SRC_DIR,
        out_dir=out_dir,
    )

    manifest_text = (out_dir / "spark-pipeline.yml").read_text()
    # state path + input path: re-rendering must not duplicate or stack substitutions.
    assert manifest_text.count("my-bucket") == 2
