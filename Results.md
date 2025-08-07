=== Starting Program ===
General Start time: 00:32:38
=== Loading and Cleaning Data ===

Column counts by Spark type:
int: 55 columns

Rows before dropna: 581012
Rows after dropna: 581012

Class distribution:
+----------+------+
|Cover_Type| count|
+----------+------+
| 1|211840|
| 6| 17367|
| 3| 35754|
| 5| 9493|
| 4| 2747|
| 7| 20510|
| 2|283301|
+----------+------+

Total rows: 581012, Number of classes: 7, Average size: 83001.71

Class weights assigned (average size divided by class size):
Class 1: count=211840, weight=0.3918
Class 6: count=17367, weight=4.7793
Class 3: count=35754, weight=2.3215
Class 5: count=9493, weight=8.7435
Class 4: count=2747, weight=30.2154
Class 7: count=20510, weight=4.0469
Class 2: count=283301, weight=0.2930

Repartitioning data into 16 partitions
Number of partitions before repartitioning: 16
+- Exchange RoundRobinPartitioning(16), REPARTITION_BY_NUM, [plan_id=256]
+- Sample 0.0, 0.8, false, 30790
+- Exchange RoundRobinPartitioning(16), REPARTITION_BY_NUM, [plan_id=256]
+- Sample 0.0, 0.8, false, 30790
+- Exchange RoundRobinPartitioning(16), REPARTITION_BY_NUM, [plan_id=366]
+- Sample 0.8, 1.0, false, 30790
+- Exchange RoundRobinPartitioning(16), REPARTITION_BY_NUM, [plan_id=366]
+- Sample 0.8, 1.0, false, 30790
Training set size: 464601 rows
Test set size: 116411 rows

=== Started: Model ===
Start time: 00:32:53
total: 109.407546835
findBestSplits: 108.836067934
chooseSplits: 108.541542938

=== Top 10 Most Important Features in the Forest ===

1. Elevation Importance: 0.29078
2. Horizontal_Distance_To_Roadways Importance: 0.08203
3. Horizontal_Distance_To_Fire_Points Importance: 0.06006
4. Wilderness_Area4 Importance: 0.05977
5. Horizontal_Distance_To_Hydrology Importance: 0.05015
6. Vertical_Distance_To_Hydrology Importance: 0.03842
7. Hillshade_9am Importance: 0.03687
8. Soil_Type10 Importance: 0.03320
9. Soil_Type38 Importance: 0.02872
10. Aspect Importance: 0.02725

=== Training Results ===
Accuracy: 0.8416
Precision: 0.8651
Recall: 0.8416
F1‐score: 0.8479
Misclassified (train): 73600

=== Test Results ===
Accuracy: 0.8262
Precision: 0.8505
Recall: 0.8262
F1‐score: 0.8329
Misclassified (test): 20238

End time: 00:35:43
Total runtime: 169.48 seconds
=== Ended: Model ===

=== Started: Model (Scaled) ===
Start time: 00:35:43
total: 98.328395927
findBestSplits: 97.836700943
chooseSplits: 97.592118654

=== Top 10 Most Important Features in the Forest ===

1. Elevation Importance: 0.29240
2. Horizontal_Distance_To_Roadways Importance: 0.08111
3. Horizontal_Distance_To_Fire_Points Importance: 0.06038
4. Wilderness_Area4 Importance: 0.05962
5. Horizontal_Distance_To_Hydrology Importance: 0.05026
6. Vertical_Distance_To_Hydrology Importance: 0.03880
7. Hillshade_9am Importance: 0.03553
8. Soil_Type10 Importance: 0.03433
9. Soil_Type38 Importance: 0.02871
10. Aspect Importance: 0.02824

=== Training Results ===
Accuracy: 0.8358
Precision: 0.8618
Recall: 0.8358
F1‐score: 0.8427
Misclassified (train): 76285

=== Test Results ===
Accuracy: 0.8196
Precision: 0.8464
Recall: 0.8196
F1‐score: 0.8269
Misclassified (test): 21006

End time: 00:38:03
Total runtime: 140.22 seconds
=== Ended: Model (Scaled) ===

=== Started: Model with PCA ===
Start time: 00:38:03
total: 241.578715542
findBestSplits: 240.530822309
chooseSplits: 239.767231119

=== Top 10 Most Important Features in the Forest ===

1. PC2 Importance: 0.08049
2. PC15 Importance: 0.07783
3. PC14 Importance: 0.07683
4. PC13 Importance: 0.07610
5. PC12 Importance: 0.07356
6. PC7 Importance: 0.05775
7. PC3 Importance: 0.04773
8. PC11 Importance: 0.04584
9. PC18 Importance: 0.04568
10. PC9 Importance: 0.04523
    Explained variance by each of the 20 components:
    Component 1: 0.3001
    Component 2: 0.0838
    Component 3: 0.0686
    Component 4: 0.0626
    Component 5: 0.0497
    Component 6: 0.0475
    Component 7: 0.0409
    Component 8: 0.0343
    Component 9: 0.0301
    Component 10: 0.0281
    Component 11: 0.0226
    Component 12: 0.0225
    Component 13: 0.0190
    Component 14: 0.0184
    Component 15: 0.0163
    Component 16: 0.0151
    Component 17: 0.0140
    Component 18: 0.0133
    Component 19: 0.0122
    Component 20: 0.0115
    Total variance captured by first 20 components: 0.9106

=== Training Results ===
Accuracy: 0.9404
Precision: 0.9439
Recall: 0.9404
F1‐score: 0.9412
Misclassified (train): 27684

=== Test Results ===
Accuracy: 0.9036
Precision: 0.9075
Recall: 0.9036
F1‐score: 0.9046
Misclassified (test): 11224

End time: 00:46:48
Total runtime: 524.53 seconds
=== Ended: Model with PCA ===

General runtime: 849.81 seconds
=== Program Finished ===
