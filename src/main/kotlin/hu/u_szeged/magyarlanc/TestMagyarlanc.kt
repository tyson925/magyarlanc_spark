package hu.u_szeged.magyarlanc

import hu.u_szeged2.magyarlanc.Magyarlanc

class TestMagyarlanc() {

    companion object {
        @JvmStatic fun main(args: Array<String>) {
            //println(MySplitter.getInstance().split("Menko az egy fasz"))
            Magyarlanc.depInit()
            println(Magyarlanc.depParse("Menko az egy fasz").map { token ->
                token.map { data -> data.toList() }
            }.joinToString(" "))
        }
    }


}

