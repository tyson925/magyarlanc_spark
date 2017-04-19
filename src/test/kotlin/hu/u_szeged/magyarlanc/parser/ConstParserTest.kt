package hu.u_szeged.magyarlanc.parser

import hu.u_szeged.magyarlanc.constructTestDataset
import hu.u_szeged.magyarlanc.spark.*
import hu.u_szeged.magyarlanc.spark.constituensParse.ConstParsedContent
import hu.u_szeged.magyarlanc.spark.constituensParse.ConstParsedSentence
import hu.u_szeged.magyarlanc.spark.constituensParse.ConstParsedToken
import hu.u_szeged.magyarlanc.spark.constituensParse.HunConstParser
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL
import scala.collection.JavaConversions
import scala.collection.mutable.WrappedArray
import java.io.Serializable


class ConstParserTest : Serializable {
    companion object {
        const val HUN_CONSTPARSED_INDEX = "hun_constparsed_content"
        @JvmStatic fun main(args: Array<String>) {
            val test = ConstParserTest()
            test.writeDepParsedContentToES()
            test.readTaggedContentFromES()
        }

    }

    fun writeDepParsedContentToES() {
        val jsc = getLocalSparkContext("Test NLP parser", cores = 1)
        val sparkSession = getLocalSparkSession("Test NLP parser")


        val testCorpus = constructTestDataset(jsc, sparkSession)
        testCorpus?.let {
            parserTest(sparkSession, testCorpus)
        }
        closeSpark(jsc)
    }

    fun parserTest(sparkSession: SparkSession, testCorpus: Dataset<Row>) {

        val parser = HunConstParser(sparkSession)
        val test = parser.transform(testCorpus)
        test?.show(false)

        val parsedContent = parser.transform(testCorpus)?.toJavaRDD()?.map { row ->
            println(row.schema())
            val parsedSentences = row.getList<WrappedArray<WrappedArray<String>>>(3)
            ConstParsedContent(parsedSentences.map { sentence ->

                ConstParsedSentence(JavaConversions.asJavaCollection(sentence).map { token ->
                    ConstParsedToken(token.apply(0), token.apply(1), token.apply(2), token.apply(3), token.apply(4))
                })
            })
        }

        val depParsedDataset = parsedContent?.convertRDDToDF(sparkSession)
        JavaEsSparkSQL.saveToEs(depParsedDataset, "$HUN_CONSTPARSED_INDEX/parsedContent")
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
                .set("es.read.field.as.array.include", "constParsedContent, constParsedContent.constSentence")

        val jsc = JavaSparkContext(sparkConf)

        jsc.appName()

        val sparkSession = getLocalSparkSession("ES test")
        val documents = readSoContentFromEs(sparkSession, ParserTest.HUN_PARSED_INDEX)


        val res = documents.toJavaRDD().map { row ->
            //println(row.schema())
            val data = row.getSeq<Row>(0)

            //val data = row.getAs<WrappedArray<WrappedArray<*>>>(0)
            println(data.javaClass.kotlin)
            val res = JavaConversions.asJavaCollection(data)


            ConstParsedContent(res.map { row1 ->
                val sentence = JavaConversions.asJavaCollection(row1.getSeq<Row>(0))
                ConstParsedSentence(
                        sentence.map { parsedToken ->
                            val token = parsedToken.getAs<String>(ConstParsedToken::token.name)
                            val lemma = parsedToken.getAs<String>(ConstParsedToken::lemma.name)
                            val purePos = parsedToken.getAs<String>(ConstParsedToken::purePos.name)
                            val msd = parsedToken.getAs<String>(ConstParsedToken::msd.name)
                            val const = parsedToken.getAs<String>(ConstParsedToken::const.name)
                            ConstParsedToken(token, lemma, purePos, msd, const)
                        })
            })
        }


        //documents.show(10, false)
        return res.convertRDDToDF(sparkSession).toDF()
    }


}

