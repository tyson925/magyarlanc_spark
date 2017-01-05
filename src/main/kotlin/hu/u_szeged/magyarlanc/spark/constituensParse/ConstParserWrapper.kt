package hu.u_szeged.magyarlanc.spark.constituensParse

import hu.u_szeged.cons.parser.MyBerkeleyParser
import java.io.Serializable

class ConstParserWrapper() : Serializable {

    @Transient private var constParser: MyBerkeleyParser? = null

    fun get() : MyBerkeleyParser {

        return if (constParser == null) {
            constParser = MyBerkeleyParser.getInstance()
            constParser!!
        } else {
            constParser!!
        }
    }
}