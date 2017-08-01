package com.smartsolutions.ml

import org.apache.spark.sql.SparkSession
import org.junit.{After, Before, Test}

@Test
class ClusteringTest {

	var spark: SparkSession = _

	@Before
	def before() = {
		spark = SparkSession.builder
			.master("local[2]")
			.appName("testkmeans")
			.getOrCreate()
	}

	@After
	def after() = {
		spark.stop()
	}

	@Test
	def testClusteringKmeans() = {
		val testFile: String = getClass.getResource("/kmeans_data.txt").getPath()
		val res_tuple = Clustering.kmeans_libsvm(spark, testFile)
		println("Error %d".format(res_tuple._1))
	}
}
