package com.kafka.lab3

import java.util

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

import scala.collection.mutable.Map

class MonitoringPartitioner extends Partitioner {

  private var countryToMonitoringPartitionMap: Map[String,Int] = Map.empty[String,Int]

  override def configure(configs: util.Map[String, _]): Unit = {
    configs.entrySet().forEach(entry => {
      if(entry.getKey.startsWith("partition.")){
        val paritionId = Integer.parseInt(entry.getKey.substring(10))
        countryToMonitoringPartitionMap += (entry.getValue.toString -> paritionId)

      }
    })

  }

  override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    val partitions = cluster.availablePartitionsForTopic(topic)
    val authID: String = value.toString.split(",").last.trim

    if(authID.startsWith("AuthID_US"))
      return countryToMonitoringPartitionMap("US")
    else if(authID.startsWith("AuthID_UK"))
      return countryToMonitoringPartitionMap("UK")
    else
      return countryToMonitoringPartitionMap("OTHERS")
  }

  override def close(): Unit = { }


}



