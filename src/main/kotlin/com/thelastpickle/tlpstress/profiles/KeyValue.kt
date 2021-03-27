package com.thelastpickle.tlpstress.profiles

import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.thelastpickle.tlpstress.PartitionKey
import com.thelastpickle.tlpstress.StressContext
import com.thelastpickle.tlpstress.WorkloadParameter
import com.thelastpickle.tlpstress.generators.FieldGenerator
import com.thelastpickle.tlpstress.generators.Field
import com.thelastpickle.tlpstress.generators.FieldFactory
import com.thelastpickle.tlpstress.generators.functions.Random


class KeyValue : IStressProfile {

    lateinit var insert: PreparedStatement
    lateinit var select: PreparedStatement
    lateinit var delete: PreparedStatement

    @WorkloadParameter("Limit select to N rows.")
    var ncols = 1

    override fun prepare(session: Session) {
        var cols = (1..ncols).joinToString(",") { i ->
            "value$i"
        }
        var args = (1..ncols).joinToString(",") { _ -> "?"}
        insert = session.prepare("INSERT INTO keyvalue (key, $cols) VALUES (?, $args)")
        select = session.prepare("SELECT $cols from keyvalue WHERE key = ?")
        delete = session.prepare("DELETE from keyvalue WHERE key = ?")
    }

    override fun schema(): List<String> {
        var cols = (1..ncols).joinToString(",") { i ->
            "value$i text"
        }
        val table = """CREATE TABLE IF NOT EXISTS keyvalue (
                        key text PRIMARY KEY,
                        $cols
                        )""".trimIndent()
        return listOf(table)
    }

    override fun getDefaultReadRate(): Double {
        return 0.5
    }

    override fun getRunner(context: StressContext): IStressRunner {

        val value = context.registry.getGenerator("keyvalue", "value")

        return object : IStressRunner {

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                val bound = select.bind(partitionKey.getText())
                return Operation.SelectStatement(bound)
            }

            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                //val data = value.getText()
                //val bound = insert.bind(partitionKey.getText(),  data)
                val bound = insert.bind().setString("key", partitionKey.getText())
                (1..ncols).forEach(){i -> bound.setString("value$i", value.getText())}

                return Operation.Mutation(bound)
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                val bound = delete.bind(partitionKey.getText())
                return Operation.Deletion(bound)
            }
        }
    }

    override fun getFieldGenerators(): Map<Field, FieldGenerator> {
        val kv = FieldFactory("keyvalue")
        return mapOf(kv.getField("value") to Random().apply{min=100; max=200})
    }
}