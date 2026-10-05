from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineStackProps:
    """Naming and sizing knobs for the Glue SDP stack.

    Kept as a dataclass (rather than literals in the stack) so a future
    dev/prod split can pass different values without rewriting the stack.
    """

    bucket_name: str
    prefix: str
    database_name: str
    job_name: str
    worker_type: str = "G.1X"
    num_workers: int = 2
    project_tag: str = "declarative-etl-pipeline-using-glue"
    environment_tag: str = "dev"
    owner_tag: str = "de-team"
