package com.kafka.lab5

import java.util

import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable.Map
import scala.jdk.CollectionConverters._

class ManualOffsetCommitRebalanceListener(kafkaConsumer: KafkaConsumer[String, String]) extends ConsumerRebalanceListener{

  private var currentOffsets: Map[TopicPartition, OffsetAndMetadata] = Map.empty[TopicPartition, OffsetAndMetadata]

  def addOffset(topic: String, partition: Int, offset: Long): Unit = {
    currentOffsets += (new TopicPartition(topic, partition) -> new OffsetAndMetadata(offset, "Commit"))
  }

  def getCurrentOffsets: java.util.Map[TopicPartition, OffsetAndMetadata] = currentOffsets.asJava

  override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
      kafkaConsumer.commitSync(currentOffsets.asJava)
      currentOffsets.clear()
  }



  override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {}
}


