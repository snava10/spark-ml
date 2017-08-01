package com.smartsolutions.ml

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Clustering {

	def ingestcsv(spark: SparkSession, filename: String) = {
		val dataset = spark.read
			.format("csv")
			.option("header", "true") // Use first line of all files as header
			.option("inferSchema", "true") // Automatically infer data types
			.load(filename)

		//Convert all the non numeric columns to numbers

		//Convert to libsvm format

	}

	def kmeans(spark: SparkSession, df: DataFrame, clusters: Int) : (Double, KMeansModel) = {
		// Trains a k-means model.
		val kmeans = new KMeans().setK(clusters).setSeed(1L)
		val model = kmeans.fit(df)

		// Evaluate clustering by computing Within Set Sum of Squared Errors.
		val WSSSE = model.computeCost(df)

		(WSSSE, model)
	}


	def kmeans_libsvm(spark: SparkSession, filename: String) : (Double, KMeansModel) = {
		// Loads data.
		val dataset = spark.read.format("libsvm").load(filename)
		kmeans(spark, dataset, 2)
	}

	def kmeans_csv(spark: SparkSession, filename: String): (Double, KMeansModel) = {
		val dataset = spark.read
			.format("csv")
			.option("header", "true") // Use first line of all files as header
			.option("inferSchema", "true") // Automatically infer data types
			.load(filename)
		???
	}

	/*
	The returned data frame will have two columns, label of type Double and features of type SparseVector
	 */
	def to_mlib_dataset(spark: SparkSession, dataset: DataFrame): DataFrame = {

		//Convert all date time columns to int

		//Normalise

		//Create features vector

		???
	}

	def convertDatetimeToInt(spark: SparkSession, df: DataFrame): DataFrame = {

		???
	}

	def normalise(spark: SparkSession, df: DataFrame): DataFrame = {
		???
	}

	def toFeaturesVector(spark: SparkSession, df: DataFrame): DataFrame = {
		???
	}

}