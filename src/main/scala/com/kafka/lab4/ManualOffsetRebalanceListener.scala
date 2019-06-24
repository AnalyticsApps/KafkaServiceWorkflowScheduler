package com.kafka.lab4

import java.util


import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition



class ManualOffsetRebalanceListener(kafkaConsumer: KafkaConsumer[String, String], offset: Long) extends ConsumerRebalanceListener{

  override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {}

  override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {

    val iter = partitions.iterator()
    while(iter.hasNext){
      val topicPartition = iter.next()
      println(s"Current offset: ${kafkaConsumer.position(topicPartition)} Committed offset: ${kafkaConsumer.committed(topicPartition)}")

      if(offset == 0){
        println("Reading offset from Beginning")
        kafkaConsumer.seekToBeginning(partitions)
      } else if(offset == -1){
        println("Reading offset from End")
        kafkaConsumer.seekToEnd(partitions)
      } else {
        println(s"Reading offset from $offset")
        kafkaConsumer.seek(topicPartition, offset)
      }

    }

  }
}


