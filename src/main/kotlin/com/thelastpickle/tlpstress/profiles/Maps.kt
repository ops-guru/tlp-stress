package com.thelastpickle.tlpstress.profiles

import com.beust.jcommander.Parameter
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.thelastpickle.tlpstress.PartitionKey
import com.thelastpickle.tlpstress.StressContext
import com.thelastpickle.tlpstress.WorkloadParameter
import com.thelastpickle.tlpstress.generators.Field
import com.thelastpickle.tlpstress.generators.FieldFactory
import com.thelastpickle.tlpstress.generators.FieldGenerator
import com.thelastpickle.tlpstress.generators.functions.Random
import com.thelastpickle.tlpstress.profiles.IStressProfile
import com.thelastpickle.tlpstress.profiles.IStressRunner
import com.thelastpickle.tlpstress.profiles.Operation


class Maps : IStressProfile {

    lateinit var insert : PreparedStatement
    lateinit var select : PreparedStatement
    lateinit var delete : PreparedStatement

    @WorkloadParameter("Use frozen type.")
    var frozen = "false"

    @WorkloadParameter("Number of map<text, text> columns to create")
    var ncols = 1

    @WorkloadParameter("Number of map<text, text> keys")
    var nkeys = 1

    val entries = mutableMapOf<String, String>()

    override fun prepare(session: Session) {
        val update_args = (1..ncols).joinToString(",") { i ->
            if (frozen == "true") {
                "data$i = ?"
            } else {
                (1..nkeys).joinToString(",") { j ->
                    "data$i['key$j'] = ?"
                }
            }
        }

        insert = session.prepare("UPDATE map_stress SET $update_args WHERE id = ?")
        select = session.prepare("SELECT * from map_stress WHERE id = ?")
        delete = session.prepare("DELETE from map_stress WHERE id = ?")
        (1..nkeys).forEach() { i->
            entries["key$i"] = "value"
        }
    }

    override fun schema(): List<String> {
        val cols = (1..ncols).joinToString(", ") { i ->
            if (frozen == "true") {
                "data$i frozen<map<text, text>>"
            } else {
                "data$i map<text, text>"
            }
        }
        val query =  """ CREATE TABLE IF NOT EXISTS map_stress (id text, $cols, primary key (id)) """
        return listOf(query)
    }


    override fun getRunner(context: StressContext): IStressRunner {

        val value = context.registry.getGenerator("maps", "value")

        return object : IStressRunner {
            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                val bound = insert.bind()
                return if (frozen == "true") {
                    (1..nkeys).forEach() { i->
                        entries["key$i"] = value.getText()
                    }

                    (1..ncols).forEach(){i -> bound.setMap("data$i", entries)}
                    Operation.Mutation(bound.setString("id", partitionKey.getText()))
                } else {
                    (1..ncols).forEach() { i ->
                        (1..nkeys).forEach() { j ->
                            bound.setString(nkeys * (i - 1) + j - 1, value.getText())
                        }
                    }
                    Operation.Mutation(bound.setString("id", partitionKey.getText()))
                }
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                val b = select.bind(partitionKey.getText())
                return Operation.SelectStatement(b)
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                val b = delete.bind(partitionKey.getText())
                return Operation.Deletion(b)
            }
        }
    }

    override fun getFieldGenerators(): Map<Field, FieldGenerator> {
        val kv = FieldFactory("maps")
        return mapOf(kv.getField("value") to Random().apply{min=100; max=200})
    }
}