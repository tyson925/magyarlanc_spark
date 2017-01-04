package hu.u_szeged.magyarlanc.spark.tokenizer

import splitter.MySplitter
import java.io.Serializable

class HunTokenizerWrapper() : Serializable {
    @Transient private var tokenizer: MySplitter? = null

    fun get() : MySplitter {

        return if (tokenizer == null) {
            tokenizer = MySplitter.getInstance()
            tokenizer!!
        } else {
            tokenizer!!
        }
    }
}

