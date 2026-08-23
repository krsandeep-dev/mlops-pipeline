"""Smoke-test DAG: proves Airflow can reach the MLflow tracking server."""

from datetime import datetime, timezone

from airflow.sdk import dag, task


@dag(
    dag_id="hello_stack",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["smoke"],
)
def hello_stack():
    @task
    def check_mlflow() -> str:
        import mlflow

        experiments = mlflow.search_experiments()
        names = [e.name for e in experiments]
        print(f"MLflow reachable — experiments: {names}")
        return f"{len(names)} experiments"

    check_mlflow()


hello_stack()
