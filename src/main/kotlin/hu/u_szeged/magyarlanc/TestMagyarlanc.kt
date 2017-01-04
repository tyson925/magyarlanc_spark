package hu.u_szeged.magyarlanc

import splitter.MySplitter

class TestMagyarlanc(){

    companion object{
        @JvmStatic fun main(args: Array<String>) {
            println(MySplitter.getInstance().split("Menko az egy fasz"))

        }
    }


}

