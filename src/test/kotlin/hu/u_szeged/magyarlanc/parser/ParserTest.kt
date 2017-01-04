package hu.u_szeged.magyarlanc.parser

import hu.u_szeged.magyarlanc.constructTestDataset
import hu.u_szeged.magyarlanc.spark.*
import hu.u_szeged.magyarlanc.spark.morphParse.HunMorphParser
import hu.u_szeged.magyarlanc.spark.morphParse.MorphParsedContent
import hu.u_szeged.magyarlanc.spark.morphParse.MorphParsedToken
import hu.u_szeged.magyarlanc.spark.morphParse.MorphSentence
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL
import scala.collection.JavaConversions
import scala.collection.mutable.WrappedArray
import java.io.Serializable


class ParserTest() : Serializable {
    companion object {
        const val HUN_PARSED_INDEX = "hun_parsed_content"
        @JvmStatic fun main(args: Array<String>) {
            val test = ParserTest()
            //test.writeTaggedContentToES()
            test.readTaggedContentFromES().show(false)
        }
    }

    fun writeTaggedContentToES() {
        val jsc = getLocalSparkContext("Test NLP parser", cores = 4)
        val sparkSession = getLocalSparkSession("Test NLP parser")


        val testCorpus = constructTestDataset(jsc, sparkSession)
        testCorpus?.let {
            //taggerTest(sparkSession, testCorpus)
        }
        closeSpark(jsc)
    }

    fun taggerTest(sparkSession: SparkSession, testCorpus: Dataset<Row>) {

        val tagger = HunMorphParser(sparkSession)
        val test = tagger.transform(testCorpus)
        test?.show(false)

        val taggedContent = tagger.transform(testCorpus)?.toJavaRDD()?.map { row ->
            println(row.schema())
            val parsedSentences = row.getList<WrappedArray<WrappedArray<String>>>(3)
            MorphParsedContent(parsedSentences.map { sentence ->

                MorphSentence(JavaConversions.asJavaCollection(sentence).map { token ->
                    MorphParsedToken(token.apply(0), token.apply(1), token.apply(2), token.apply(3))
                })
            })
        }

        val taggedDataset = taggedContent?.convertRDDToDF(sparkSession)
        JavaEsSparkSQL.saveToEs(taggedDataset, "$HUN_PARSED_INDEX/taggedContent")
    }

    fun readTaggedContentFromES(): Dataset<Row> {
        val sparkConf = SparkConf().setAppName("appName").setMaster("local[6]")
                //.set("spark.sql.shuffle.partitions", "1")
                .set("es.nodes", "localhost:9200")
                .set("es.nodes.discovery", "true")
                .set("es.nodes.wan.only", "false")
                .set("spark.default.parallelism", "8")
                .set("num-executors", "3")
                .set("executor-cores", "4")
                .set("executor-memory", "4G")
                .set("es.read.field.as.array.include", "morphParsedContent, morphParsedContent.morphSentence")

        val jsc = JavaSparkContext(sparkConf)

        jsc.appName()

        val sparkSession = getLocalSparkSession("ES test")
        val documents = readSoContentFromEs(sparkSession, HUN_PARSED_INDEX)


        val res = documents.toJavaRDD().map { row ->
            //println(row.schema())
            val data = row.getSeq<Row>(0)

            //val data = row.getAs<WrappedArray<WrappedArray<*>>>(0)
            println(data.javaClass.kotlin)
            val res = JavaConversions.asJavaCollection(data)
            MorphParsedContent(res.map { row1 ->
                val sentence = JavaConversions.asJavaCollection(row1.getSeq<Row>(0))
                MorphSentence(
                        sentence.map { parsedToken ->
                            val token = parsedToken.getAs<String>("token")
                            val lemma = parsedToken.getAs<String>("lemma")
                            val purePos = parsedToken.getAs<String>("purePos")
                            val msd = parsedToken.getAs<String>("msd")
                            MorphParsedToken(token, lemma, purePos, msd)
                        })
            })
        }

        //documents.show(10, false)
        return res.convertRDDToDF(sparkSession).toDF()
    }
}

