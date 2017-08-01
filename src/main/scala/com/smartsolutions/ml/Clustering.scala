package com.smartsolutions.ml

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession

object Clustering {

	def kmeans(spark: SparkSession, filename: String) = {
		// Loads data.
		val dataset = spark.read.format("libsvm").load(filename)

		// Trains a k-means model.
		val kmeans = new KMeans().setK(2).setSeed(1L)
		val model = kmeans.fit(dataset)

		// Evaluate clustering by computing Within Set Sum of Squared Errors.
		val WSSSE = model.computeCost(dataset)
		println(s"Within Set Sum of Squared Errors = $WSSSE")

		// Shows the result.
		println("Cluster Centers: ")
		model.clusterCenters.foreach(println)
	}
}