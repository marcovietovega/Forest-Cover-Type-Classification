# Forest Cover Type Classification with Random Forest in Apache Spark

This project uses big data techniques to accurately predict forest cover types based on terrain and soil characteristics. By leveraging Apache Spark and Random Forest algorithms, it analyzes environmental patterns to classify forest types with high precision across a large-scale dataset.

### Key Features of This Project

- **Three Model Variants**: Started with raw data and built up to advanced preprocessing techniques.
- **Smart Feature Engineering**: Applied scaling and dimensionality reduction using PCA.
- **Distributed Computing**: Leveraged Apache Spark for handling large-scale data processing.
- **Class Balancing**: Implemented weighted classification to handle imbalanced data.
- **Comprehensive Evaluation**: Shows model performance with detailed metrics and feature importance analysis.

## Dataset

The Covertype Dataset from the UCI Machine Learning Repository contains 581,012 instances with 54 features describing terrain and soil characteristics. The dataset includes:

- **Environmental Features**: Elevation, slope, aspect, hydrology distances, hillshade values, soil types, and wilderness areas
- **Target Classes**: 7 forest cover types (tree species)
- **Size**: 75.2MB total dataset
- **Source**: UCI Machine Learning Repository

The data is automatically cleaned, balanced using class weights, and split into 80% training and 20% test sets.

## How the Model Works

The final model uses **Random Forest**, a powerful ensemble method that combines multiple decision trees. The implementation includes:

- Starts with 54 environmental and cartographic features.
- Uses weighted classification to handle class imbalance.
- Applies feature scaling and PCA for dimensionality reduction.
- Trains 50 decision trees with a maximum depth of 20.

### The Three Model Variants

1. **Non-Scaled Model**: Uses raw features without preprocessing.
2. **Scaled Model**: Applies MinMax scaling to normalize feature ranges.
3. **PCA Model**: Uses Principal Component Analysis to reduce dimensionality to 20 components.

## Tools and Techniques

### Libraries

- **Apache Spark**: Distributed computing framework for big data processing.
- **PySpark MLlib**: Machine learning library for scalable algorithms.
- **Random Forest**: Ensemble learning method for classification.
- **HDFS**: Distributed file system for data storage.

### Techniques

- **Class Weighting**: Handles imbalanced data by assigning weights inversely proportional to class frequency.
- **Feature Scaling**: MinMax normalization for better model performance.
- **Dimensionality Reduction**: PCA to capture variance in fewer components.
- **Distributed Processing**: Partitioning data across multiple nodes for parallel computation.

## Results

- **High Accuracy**: The PCA model achieves 90.36% accuracy, significantly outperforming other variants.
- **Excellent F1-Score**: PCA model reaches 0.9046 F1-score, showing balanced precision and recall.
- **Efficient Processing**: Models complete training and evaluation in under 10 minutes.
- **Feature Insights**: Analysis reveals the most important environmental factors for forest classification.

### Performance Comparison

| Model      | Accuracy   | F1-score   | Runtime (s) |
| ---------- | ---------- | ---------- | ----------- |
| Non-Scaled | 0.8262     | 0.8329     | 169.48      |
| Scaled     | 0.8196     | 0.8269     | 140.22      |
| **PCA**    | **0.9036** | **0.9046** | 524.53      |

## What This Project Shows

### Big Data Processing

- **Scalable data loading**: Efficiently loads and processes large datasets from HDFS.
- **Distributed computing**: Leverages Spark's parallel processing capabilities.
- **Memory optimization**: Smart caching and partitioning strategies.
- **Fault tolerance**: Robust error handling and resource management.

### Feature Engineering

- **Automated preprocessing**: Pipeline-based approach for consistent data transformation.
- **Dimensionality reduction**: PCA captures 90%+ variance in 20 components.
- **Feature importance**: Identifies the most predictive environmental characteristics.
- **Class balancing**: Weighted sampling addresses data imbalance.

### Model Development

- **Ensemble methods**: Random Forest provides robust predictions through voting.
- **Hyperparameter tuning**: Optimized tree count, depth, and other parameters.
- **Cross-validation ready**: Modular design supports multiple evaluation strategies.
- **Scalable architecture**: Handles datasets of varying sizes efficiently.

### Results and Analysis

- **Comprehensive metrics**: Reports accuracy, precision, recall, and F1-scores.
- **Feature analysis**: Ranks environmental factors by predictive importance.
- **Variance explanation**: PCA components show cumulative variance captured.
- **Performance monitoring**: Detailed timing and resource usage tracking.

## What I Learned

This project demonstrates important big data and machine learning skills:

- **Distributed Computing**: Using Spark for large-scale data processing.
- **Feature Engineering**: Applying scaling and dimensionality reduction techniques.
- **Ensemble Methods**: Leveraging Random Forest for robust classification.
- **Performance Optimization**: Balancing accuracy with computational efficiency.
- **Big Data Pipelines**: Building scalable machine learning workflows.

## Technical Implementation

### Environment Setup

- Apache Spark cluster configuration
- HDFS data storage and access
- PySpark MLlib pipeline development
- Resource management and optimization

### Data Pipeline

- Automated data loading from distributed storage
- Feature vector assembly and preprocessing
- Class weight calculation for imbalanced data
- Train/test split with stratification

### Model Pipeline

- Modular pipeline design with interchangeable components
- Feature scaling and PCA transformation stages
- Random Forest classifier with optimized parameters
- Comprehensive evaluation and metrics collection

## Contact

Marco A. Vieto V. - [vietomarc@myvuw.ac.nz](mailto:vietomarc@myvuw.ac.nz)

---

_This project was developed as part of AIML427 coursework at Victoria University of Wellington._
