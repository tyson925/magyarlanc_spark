package hu.u_szeged.magyarlanc.spark.morphParse

import hu.u_szeged2.pos.purepos.MyPurePos
import java.io.Serializable


class MorphParseWrapper() : Serializable {
    @Transient private var purePosTagger: MyPurePos? = null

    fun get() : MyPurePos {

        return if (purePosTagger == null) {
            purePosTagger = MyPurePos.getInstance()
            purePosTagger!!
        } else {
            purePosTagger!!
        }
    }
}