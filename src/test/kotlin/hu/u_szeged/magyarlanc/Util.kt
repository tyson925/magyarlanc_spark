package hu.u_szeged.magyarlanc

import hu.u_szeged.magyarlanc.spark.tokenizer.HunTokenizer
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import scala.Tuple2
import java.io.Serializable
import java.util.*


data class TestData(val id: Int, val text: String) : Serializable


fun constructTestDataset(jsc: JavaSparkContext, sparkSession: SparkSession): Dataset<Row>? {
    val test = LinkedList<Tuple2<Int, String>>()
    test.add(Tuple2(1, "A magyarlanc nem thread safe."))
    test.add(Tuple2(2, "Az előrejelzés szerint az országos havazás a Duna-Tisza közén csütörtökön is megmaradhat, ott gyenge hóesésre kell számítani, míg máshol csak elszórt hózáporok fordulhatnak elő, és lesz, ahol hosszabb időre még a nap is kisüt. Csütörtök hajnalban mínusz 7 - mínusz 1, napközben mínusz 3 - plusz 3 fokra kell számítani."))
    test.add(Tuple2(3, "Ennél jóval zordabb lesz az idő péntek hajnalban, amikor akár mínusz 15 fokig is süllyedhet a hőmérséklet, és napközben sem lesz melegebb mínusz 10 - mínusz 3 foknál - Északkeleten lesz a leghidegebb."))
    test.add(Tuple2(4, "Pénteken az ország nagy részén derült, csapadékmentes idő várható, bár nyugaton néhol előfordulhat még futó hózápor. Az északi szél több helyen megerősödik, és viharossá fokozódik."))
    test.add(Tuple2(5, "A leghidegebb szombat hajnalban lesz: általában mínusz 10 - mínusz 15 fok között alakul, de elsősorban északkeleten, keleten, a hóval borított, szélvédett kisebb körzetekben mínusz 16 - mínusz 20 fok várható."))
    test.add(Tuple2(6, "Napközben is északkeleten lesz a hidegebb: mínusz 8 - mínusz 11, máshol mínusz 4 - mínusz 8 fokot mérhetünk. Az OMSZ előrejelzése szerint szombat délutántól kisebb havazás már kialakulhat, de igazán csak vasárnap fog rákapcsolni, akkor többfelé kell havazásra, hószállingózásra kell számítani."))
    test.add(Tuple2(7, "Vasárnap hajnalban mínusz 15 - mínusz 6, nappal mínusz 9 - plusz 2 fok várható."))
    test.add(Tuple2(8, "A mostani előrejelezések szerint az igazán csípős idő szerencsére csak három napig tart majd, jövő héttől melegszik a levegő, és visszatérnek a 4-5 fokos nappali maximumok."))

    val testRdd = jsc.parallelizePairs(test).map { item ->
        TestData(item._1, item._2)
    }

    val input = sparkSession.createDataFrame(testRdd, TestData::class.java).toDF("id", "content")
    //val sparkSession = SparkSession.builder().master("local").appName("test").orCreate
    val tokenizer = HunTokenizer(sparkSession, inputColName = "content")

    val tokenized = tokenizer.transform(input)

    tokenized?.show(false)

    return tokenized
}
