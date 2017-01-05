package hu.u_szeged.magyarlanc.spark.depParse

import hu.u_szeged2.dep.parser.MyMateParser
import java.io.Serializable

class DepParserWrapper() : Serializable {
    @Transient private var depParser: MyMateParser? = null

    fun get() : MyMateParser {

        return if (depParser == null) {
            depParser = MyMateParser.getInstance()
            depParser!!
        } else {
            depParser!!
        }
    }
}