from helpers import create_spark, get_feat_config_path, load_config
from predictions.runner import run_tip_prediction_training

def main() -> None:
    config = load_config(get_feat_config_path())
    spark = create_spark("tip-prediction-model")

    try:
        metrics = run_tip_prediction_training(spark, config)

        print(
            f"test: "
            f"MAE={metrics['mae']:.4f}, "
            f"RMSE={metrics['rmse']:.4f}"
        )
    finally:
        spark.stop()

if __name__ == "__main__":
    main()