# ClimateSpark

## Overreview
ClimateSpark is a Spark-based distributed computing framework to support big climate data management and analytics.
It has the following capabilities: <br />
1. Natively support HDF4 and NetCDF4 datasets stored in HDFS <br />
2. Spatiotemporal query of multi-dimensional array-based climate data with high efficiency, e.g. high data locality,
no redundant data reading<br />
3. ClimateRDD: a multi-dimensional array-based data model for Spark to organize climate data
4. Basic climate data analytics

## Tutorial
 https://docs.google.com/document/d/1JIMLhNzXA_Ay-0P6yxzGfvUhajMLfJFyyduillzpeSI/edit
 
 * Extract the metadata: `hadoop jar sia-core/target/sia-core-0.1.0.jar properties/sia_merra2_preprocessor.properties`
 * Build index: `hadoop jar sia-indexer/target/sia-indexer-0.1.0.jar properties/sia_merra2_indexer.properties`
 
 