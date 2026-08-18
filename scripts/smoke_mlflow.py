import mlflow

mlflow.set_tracking_uri("http://localhost:5001")
mlflow.set_experiment("smoke-test")

with mlflow.start_run(run_name="wiring-check") as run:
    mlflow.log_param("phase", 1)
    mlflow.log_metric("answer", 42)
    with open("/tmp/hello.txt", "w") as f:
        f.write("artifact stored in MinIO\n")
    mlflow.log_artifact("/tmp/hello.txt")
    print(f"OK — run_id: {run.info.run_id}")
