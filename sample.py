from pyspark import SparkContext, SparkConf
import re

class DataPipeline:
    def __init__(self, sc):
        """
        Initialize the data processing pipeline
        
        Pipeline Stages:
        1. Data Ingestion
        2. Data Cleaning
        3. Data Transformation
        4. Data Filtering
        5. Data Aggregation
        """
        self.spark_context = sc
    
    def ingest_data(self, raw_data):
        """
        Stage 1: Data Ingestion
        Convert raw input into initial RDD
        """
        return self.spark_context.parallelize(raw_data)
    
    def clean_data(self, input_rdd):
        """
        Stage 2: Data Cleaning
        Remove special characters, convert to lowercase
        """
        return input_rdd.map(
            lambda x: re.sub(r'[^a-zA-Z0-9\s]', '', x).lower()
        )
    
    def transform_data(self, cleaned_rdd):
        """
        Stage 3: Data Transformation
        Split into words, create (word, 1) pairs
        """
        return cleaned_rdd.flatMap(
            lambda x: x.split()
        ).map(
            lambda word: (word, 1)
        )
    
    def filter_data(self, transformed_rdd):
        """
        Stage 4: Data Filtering
        Remove very short or very long words
        """
        return transformed_rdd.filter(
            lambda x: 2 < len(x[0]) < 10
        )
    
    def aggregate_data(self, filtered_rdd):
        """
        Stage 5: Data Aggregation
        Count word frequencies
        """
        return filtered_rdd.reduceByKey(
            lambda a, b: a + b
        ).sortBy(
            lambda x: x[1], ascending=False
        )
    
    def execute_pipeline(self, raw_data):
        """
        Execute the entire data processing pipeline
        Each stage depends on the output of the previous stage
        """
        # Pipeline execution
        ingested_data = self.ingest_data(raw_data)
        cleaned_data = self.clean_data(ingested_data)
        transformed_data = self.transform_data(cleaned_data)
        filtered_data = self.filter_data(transformed_data)
        final_result = self.aggregate_data(filtered_data)
        
        return final_result

def main():
    # Create Spark Context
    conf = SparkConf().setAppName("Data Processing Pipeline")
    sc = SparkContext(conf=conf)
    
    # Sample data
    sample_documents = [
        "Hello, world! This is a sample text.",
        "Another example of text processing pipeline.",
        "Data pipelines are powerful for big data processing."
    ]
    
    try:
        # Create pipeline
        pipeline = DataPipeline(sc)
        
        # Execute pipeline
        results = pipeline.execute_pipeline(sample_documents)
        
        # Print results
        print("Word Frequencies:")
        for word, count in results.take(5):
            print(f"{word}: {count}")
    
    finally:
        # Stop Spark Context
        sc.stop()

if __name__ == "__main__":
    main()