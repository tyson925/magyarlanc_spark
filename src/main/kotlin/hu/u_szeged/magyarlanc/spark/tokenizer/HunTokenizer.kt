package hu.u_szeged.magyarlanc.spark.tokenizer

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
import java.io.Serializable

const val tokenizedContent = "tokenizedContent"

class HunTokenizer : Transformer, Serializable {

    val tokenizerWrapper: HunTokenizerWrapper
    val udfName = "tokenizer"
    val sparkSession: SparkSession
    var inputColName: String
    var outputColName: String

    constructor(sparkSession: SparkSession, inputColName: String = "content") {
        this.tokenizerWrapper = HunTokenizerWrapper()
        this.sparkSession = sparkSession
        this.inputColName = inputColName
        this.outputColName = tokenizedContent

        val tokenizer = UDF1 { content: String ->
            tokenizerWrapper.get().splitToArray(content)
        }

        sparkSession.udf().register(udfName, tokenizer, DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.StringType)))
    }

    fun setInputColName(inputColName: String): HunTokenizer {
        this.inputColName = inputColName
        return this
    }

    fun setOutputColName(outputColName : String) : HunTokenizer {
        this.outputColName = outputColName
        return this
    }


    override fun uid(): String {
        return "uid1111111"
    }

    override fun copy(p0: ParamMap?): Transformer {
        return HunTokenizer(sparkSession)
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
        return SchemaUtils.appendColumn(schema, outputColName, DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.StringType)), inputType?.nullable() ?: false)
    }

}