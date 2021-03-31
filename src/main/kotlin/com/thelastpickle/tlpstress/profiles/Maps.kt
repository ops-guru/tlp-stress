package com.thelastpickle.tlpstress.profiles

import com.beust.jcommander.Parameter
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.thelastpickle.tlpstress.PartitionKey
import com.thelastpickle.tlpstress.StressContext
import com.thelastpickle.tlpstress.WorkloadParameter
import com.thelastpickle.tlpstress.profiles.IStressProfile
import com.thelastpickle.tlpstress.profiles.IStressRunner
import com.thelastpickle.tlpstress.profiles.Operation


class Maps : IStressProfile {

    lateinit var insert : PreparedStatement
    lateinit var select : PreparedStatement
    lateinit var delete : PreparedStatement

    @WorkloadParameter("Use frozen type.")
    var frozen = "false"

    override fun prepare(session: Session) {
        insert = session.prepare(
            if (frozen == "true") {
                "UPDATE map_stress SET data = ? WHERE id = ?"
            } else
            {
                "UPDATE map_stress SET data[?] = ? WHERE id = ?"
            }
        )
        select = session.prepare("SELECT * from map_stress WHERE id = ?")
        delete = session.prepare("DELETE from map_stress WHERE id = ?")
    }

    override fun schema(): List<String> {
        val query = if (frozen == "true") {
            """ CREATE TABLE IF NOT EXISTS map_stress (id text, data frozen<map<text, text>>, primary key (id)) """}
        else {
            """ CREATE TABLE IF NOT EXISTS map_stress (id text, data map<text, text>, primary key (id)) """
        }
        return listOf(query)
    }


    override fun getRunner(context: StressContext): IStressRunner {
        return object : IStressRunner {
            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                return if (frozen == "true") {
                    val map = mapOf("key" to "value")
                    Operation.Mutation(insert.bind(map, partitionKey.getText()))
                } else {
                    Operation.Mutation(insert.bind("key", "value", partitionKey.getText()))
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
}