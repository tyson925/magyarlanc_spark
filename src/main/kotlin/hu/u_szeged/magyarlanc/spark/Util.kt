package hu.u_szeged.magyarlanc.spark

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL

inline fun <reified U : Any> JavaRDD<U>.convertRDDToDF(sparkSession: SparkSession): Dataset<U> {
    val encoder = Encoders.bean(U::class.java)

    val dataset = sparkSession.sqlContext().createDataset(this.rdd(), encoder)

    this.unpersist()

    return dataset
}


fun readSoContentFromEs(sparkSession: SparkSession, indexName: String): Dataset<Row> {

    val res = JavaEsSparkSQL.esDF(sparkSession, indexName)

    println(res.schema())
    return res
}


fun getLocalSparkContext(appName: String, cores: Int = 6): JavaSparkContext {
    val sparkConf = SparkConf().setAppName(appName).setMaster("local[$cores]")
            //.set("spark.sql.shuffle.partitions", "1")
            .set("es.nodes", "localhost:9200")
            .set("es.nodes.discovery", "true")
            .set("es.nodes.wan.only", "false")
            .set("spark.default.parallelism", "8")
            .set("num-executors", "3")
            .set("executor-cores", "4")
            .set("executor-memory", "4G")


    val jsc = JavaSparkContext(sparkConf)
    return jsc
}

fun closeSpark(jsc: JavaSparkContext) {
    jsc.clearJobGroup()
    jsc.close()
    jsc.stop()
    println("exit spark job conf")
}

fun getLocalSparkSession(sessionName: String): SparkSession {
    return SparkSession.builder().master("local").appName(sessionName).orCreate
}