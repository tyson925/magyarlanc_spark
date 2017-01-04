package hu.u_szeged.magyarlanc.spark.morphParse

import hu.u_szeged.magyarlanc.spark.tokenizer.tokenizedContent
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConversions
import scala.collection.mutable.WrappedArray
import java.io.Serializable

data class MorphParsedToken(var token : String, var lemma : String, var purePos : String, var msd : String) : Serializable
data class MorphSentence(var morphSentence: List<MorphParsedToken>) : Serializable
data class MorphParsedContent(var morphParsedContent : List<MorphSentence>) : Serializable

const val morhpOutputColName = "morphContent"

class HunMorphParser : Transformer {

    val morphTagger: MorphParseWrapper
    val udfName = "tokenizer"
    val sparkSession: SparkSession
    var inputColName: String
    var outputColName: String

    constructor(sparkSession: SparkSession, inputColName: String = tokenizedContent) {
        this.sparkSession = sparkSession
        this.inputColName = inputColName
        this.outputColName = morhpOutputColName
        this.morphTagger = MorphParseWrapper()

        val tagger = UDF1 { sentences: WrappedArray<WrappedArray<String>> ->
            val morphTagger = this.morphTagger.get()

            val sentencesJava = JavaConversions.asJavaCollection(sentences)

            val results = sentencesJava.map { sentence ->
                morphTagger.morphParseSentence(JavaConversions.seqAsJavaList(sentence))
            }

            results.toTypedArray()
        }

        this.sparkSession.udf().register(udfName, tagger, DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.StringType))))

    }

    fun setInputColName(inputColName: String): HunMorphParser {
        this.inputColName = inputColName
        return this
    }

    fun setOutputColName(outputColName: String): HunMorphParser {
        this.outputColName = outputColName
        return this
    }


    override fun uid(): String {
        return "uid1111111"
    }

    override fun copy(p0: ParamMap?): Transformer {
        return HunMorphParser(this.sparkSession)
    }

    override fun transform(dataset: Dataset<*>?): Dataset<Row>? {
        return dataset?.select(dataset.col("*"),
                functions.callUDF(udfName, JavaConversions.asScalaBuffer(listOf(dataset.col(inputColName)))).`as`(outputColName))
    }

    override fun transformSchema(schema: StructType?): StructType {
        val inputType = schema?.apply(schema.fieldIndex(inputColName))
        val inputTypeMetaData = inputType?.metadata()
        //val refType = DataTypes.createArrayType(DataTypes.StringType).javaClass

        if (inputTypeMetaData is DataTypes) {
            println("Input type must be StringType but got $inputTypeMetaData.")
        }
        val nullable = inputType?.nullable() ?: false
        return SchemaUtils.appendColumn(schema, outputColName, DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.StringType))), nullable)
    }

}