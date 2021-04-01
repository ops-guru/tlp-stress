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

    @WorkloadParameter("Limit select to N rows.")
    var ncols = 1

    override fun prepare(session: Session) {
        val update_args = (1..ncols).joinToString(",") { i ->
            if (frozen == "true") {
                "data$i = ?"
            } else {
                "data$i[?] = ?"
            }
        }
        insert = session.prepare("UPDATE map_stress SET $update_args WHERE id = ?")
        select = session.prepare("SELECT * from map_stress WHERE id = ?")
        delete = session.prepare("DELETE from map_stress WHERE id = ?")
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
        return object : IStressRunner {
            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                val map = mapOf("key" to "value")
                val bound = insert.bind()
                return if (frozen == "true") {
                    (1..ncols).forEach(){i -> bound.setMap("data$i", map)}
                    Operation.Mutation(bound.setString("id", partitionKey.getText()))
                } else {
                    (1..ncols).forEach(){i -> bound.setString((i-1)*2, "key")}
                    (1..ncols).forEach(){i -> bound.setString((i-1)*2+1, "value")}
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
}