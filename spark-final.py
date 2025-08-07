from pyspark.sql import SparkSession
import time
from datetime import datetime
from collections import Counter
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler, MinMaxScaler, PCA
from pyspark.ml.classification import RandomForestClassifier
from pyspark.mllib.evaluation import MulticlassMetrics


def load_and_clean_data(spark):
    print("=== Loading and Cleaning Data ===")

    data = spark.read.options(inferSchema=True, delimiter=",").csv(
        "hdfs:///user/vietomarc/Data/covtype.data"
    )

    col_names = (
        [
            "Elevation",
            "Aspect",
            "Slope",
            "Horizontal_Distance_To_Hydrology",
            "Vertical_Distance_To_Hydrology",
            "Horizontal_Distance_To_Roadways",
            "Hillshade_9am",
            "Hillshade_Noon",
            "Hillshade_3pm",
            "Horizontal_Distance_To_Fire_Points",
        ]
        + [f"Wilderness_Area{i}" for i in (1, 2, 3, 4)]
        + [f"Soil_Type{i}" for i in range(1, 41)]
        + ["Cover_Type"]
    )

    data_clean = data.toDF(*col_names)

    dtype_list = [dtype for (_, dtype) in data_clean.dtypes]
    counts_by_type = Counter(dtype_list)

    print("\nColumn counts by Spark type:")
    for spark_type, cnt in counts_by_type.items():
        print(f"  {spark_type}: {cnt} columns")

    print(f"\nRows before dropna: {data.count()}")
    data_clean = data_clean.dropna()
    total_rows_count = data_clean.count()
    print(f"Rows after dropna: {total_rows_count}")

    print("\nClass distribution:")
    data_clean.select("Cover_Type").groupBy("Cover_Type").count().show()

    counts = data_clean.groupBy("Cover_Type").count().collect()
    counts_map = {row["Cover_Type"]: row["count"] for row in counts}

    total = sum(counts_map.values())
    num_classes = len(counts_map)
    avg_size = float(total) / float(num_classes)

    print(
        f"\nTotal rows: {total}, Number of classes: {num_classes}, Average size: {avg_size:.2f}"
    )

    weight_map = {
        int(label): float(avg_size) / float(cnt) for label, cnt in counts_map.items()
    }

    def lookup_weight(label):
        return float(weight_map[int(label)])

    weight_udf = udf(lookup_weight, DoubleType())

    data_weighted = data_clean.withColumn("classWeight", weight_udf(col("Cover_Type")))

    print("\nClass weights assigned (average size divided by class size):")
    for label, cnt in counts_map.items():
        print(f"  Class {label}: count={cnt}, weight={weight_map[label]:.4f}")
    print()

    numeric_cols = [
        col_name
        for col_name, col_type in data_clean.dtypes
        if (col_type in ("int", "double")) and (col_name != "Cover_Type")
    ]

    spark.catalog.clearCache()

    train_data, test_data = data_weighted.randomSplit([0.8, 0.2], seed=30790)

    num_partitions = spark.sparkContext.defaultParallelism
    print(f"Repartitioning data into {num_partitions} partitions")
    print(
        f"Number of partitions before repartitioning: {train_data.rdd.getNumPartitions()}"
    )

    train_data = train_data.repartition(num_partitions)
    test_data = test_data.repartition(num_partitions)

    train_data = train_data.persist()
    train_count = train_data.count()

    test_data = test_data.persist()
    test_count = test_data.count()

    print(f"Training set size: {train_count} rows")
    print(f"Test set size:     {test_count} rows")

    return train_data, test_data, numeric_cols


def print_top_10_important_features(model, feature_names):
    print("Possible features:")
    for idx, feature in enumerate(feature_names, start=1):
        print(f"{idx}. {feature}")

    rf_model = None
    for stage in model.stages:
        if stage.__class__.__name__ == "RandomForestClassificationModel":
            rf_model = stage
            break

    if rf_model is None:
        print("Random Forest model not found in the pipeline.")
        return

    importances = rf_model.featureImportances.toArray()
    feature_importance_pairs = list(zip(feature_names, importances))
    sorted_importances = sorted(
        feature_importance_pairs, key=lambda x: x[1], reverse=True
    )
    top_10 = sorted_importances[:10]

    print("\n=== Top 10 Most Important Features in the Forest ===")
    for idx, (feature, importance) in enumerate(top_10, start=1):
        print(f"{idx}. {feature:<40} Importance: {importance:.5f}")


def build_pipeline(numeric_cols, use_scaling=False, use_pca=False, pca_k=None):
    stages = []

    assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
    stages.append(assembler)

    feature_input = "features"
    intermediate_col = "features"

    if use_scaling:
        scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
        stages.append(scaler)
        intermediate_col = "scaled_features"
        feature_input = "scaled_features"

    if use_pca:
        pca = PCA(k=pca_k, inputCol=intermediate_col, outputCol="pca_features")
        stages.append(pca)
        intermediate_col = "pca_features"
        feature_input = "pca_features"

    clf = RandomForestClassifier(
        featuresCol=feature_input,
        labelCol="Cover_Type",
        weightCol="classWeight",
        numTrees=50,
        maxDepth=20,
        seed=30790,
    )
    stages.append(clf)

    return Pipeline(stages=stages)


def print_metrics(train_predictions, test_predictions, model_name=""):
    train_predictions.cache()
    train_predictions.count()

    test_predictions.cache()
    test_predictions.count()

    preds_and_labels_train = train_predictions.select(
        "prediction", "Cover_Type"
    ).rdd.map(lambda row: (float(row["prediction"]), float(row["Cover_Type"])))

    metrics_train = MulticlassMetrics(preds_and_labels_train)
    train_accuracy = metrics_train.accuracy
    train_precision = metrics_train.weightedPrecision
    train_recall = metrics_train.weightedRecall
    train_f1 = metrics_train.weightedFMeasure()

    train_misclassified = train_predictions.filter("Cover_Type != prediction").count()

    print("\n=== Training Results ===")
    print(f"Accuracy:  {train_accuracy:.4f}")
    print(f"Precision: {train_precision:.4f}")
    print(f"Recall:    {train_recall:.4f}")
    print(f"F1‐score:  {train_f1:.4f}")
    print(f"Misclassified (train): {train_misclassified}")

    preds_and_labels_test = test_predictions.select("prediction", "Cover_Type").rdd.map(
        lambda row: (float(row["prediction"]), float(row["Cover_Type"]))
    )

    metrics_test = MulticlassMetrics(preds_and_labels_test)
    test_accuracy = metrics_test.accuracy
    test_precision = metrics_test.weightedPrecision
    test_recall = metrics_test.weightedRecall
    test_f1 = metrics_test.weightedFMeasure()

    test_misclassified = test_predictions.filter("Cover_Type != prediction").count()
    test_confusion = (
        test_predictions.groupBy("Cover_Type", "prediction").count().collect()
    )

    print("\n=== Test Results ===")
    print(f"Accuracy:  {test_accuracy:.4f}")
    print(f"Precision: {test_precision:.4f}")
    print(f"Recall:    {test_recall:.4f}")
    print(f"F1‐score:  {test_f1:.4f}")
    print(f"Misclassified (test): {test_misclassified}")

    train_predictions.unpersist()
    test_predictions.unpersist()


def train_predict(
    train_data,
    test_data,
    numeric_cols,
    use_scaling=False,
    use_pca=False,
    pca_k=None,
    model_name=None,
):
    pipeline = build_pipeline(
        numeric_cols, use_scaling=use_scaling, use_pca=use_pca, pca_k=pca_k
    )

    model = pipeline.fit(train_data)

    if use_pca:
        numeric_cols = [f"PC{i+1}" for i in range(pca_k)]

    print_top_10_important_features(model, numeric_cols)

    train_predictions = model.transform(train_data)
    test_predictions = model.transform(test_data)

    if use_pca:
        pca_stage = None
        for stage in model.stages:
            if hasattr(stage, "explainedVariance"):
                pca_stage = stage
                break

        explained_variance_array = pca_stage.explainedVariance.toArray()

        total_explained = float(sum(explained_variance_array))

        print(f"Explained variance by each of the {pca_k} components:")
        for idx, val in enumerate(explained_variance_array, start=1):
            print(f"  Component {idx}: {val:.4f}")
        print(
            f"Total variance captured by first {pca_k} components: {total_explained:.4f}\n"
        )

    print_metrics(train_predictions, test_predictions, model_name)


def run_model(train_data, test_data, numeric_cols):
    print("\n=== Started: Model ===")
    start_time = time.time()
    start_dt = datetime.now()
    print(f"Start time: {start_dt.strftime('%H:%M:%S')}")

    train_predict(train_data, test_data, numeric_cols, model_name="raw")

    end_time = time.time()
    end_dt = datetime.now()
    print(f"\nEnd time: {end_dt.strftime('%H:%M:%S')}")
    print(f"Total runtime: {end_time - start_time:.2f} seconds")
    print("=== Ended: Model ===")


def run_model_scaled(train_data, test_data, numeric_cols):
    print("\n=== Started: Model (Scaled) ===")
    start_time = time.time()
    start_dt = datetime.now()
    print(f"Start time: {start_dt.strftime('%H:%M:%S')}")

    train_predict(
        train_data, test_data, numeric_cols, use_scaling=True, model_name="scaled"
    )

    end_time = time.time()
    end_dt = datetime.now()
    print(f"\nEnd time: {end_dt.strftime('%H:%M:%S')}")
    print(f"Total runtime: {end_time - start_time:.2f} seconds")
    print("=== Ended: Model (Scaled) ===")


def run_model_pca(train_data, test_data, numeric_cols, k=None):
    print("\n=== Started: Model with PCA ===")
    start_time = time.time()
    start_dt = datetime.now()
    print(f"Start time: {start_dt.strftime('%H:%M:%S')}")

    train_predict(
        train_data,
        test_data,
        numeric_cols,
        use_scaling=True,
        use_pca=True,
        pca_k=k,
        model_name="pca",
    )

    end_time = time.time()
    end_dt = datetime.now()
    print(f"\nEnd time: {end_dt.strftime('%H:%M:%S')}")
    print(f"Total runtime: {end_time - start_time:.2f} seconds")
    print("=== Ended: Model with PCA ===")


def main():
    print("=== Starting Program ===")

    total_start_time = time.time()
    total_start_dt = datetime.now()
    print(f"General Start time: {total_start_dt.strftime('%H:%M:%S')}")

    spark = SparkSession.builder.appName(
        "Forest Cover Types Classification"
    ).getOrCreate()

    train_data, test_data, numeric_cols = load_and_clean_data(spark)

    run_model(train_data, test_data, numeric_cols)
    run_model_scaled(train_data, test_data, numeric_cols)
    run_model_pca(train_data, test_data, numeric_cols, k=20)

    total_end_time = time.time()
    print(f"\nGeneral runtime: {total_end_time - total_start_time:.2f} seconds")
    print("=== Program Finished ===")
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, flush=True)
        import traceback

        traceback.print_exc()
        raise
