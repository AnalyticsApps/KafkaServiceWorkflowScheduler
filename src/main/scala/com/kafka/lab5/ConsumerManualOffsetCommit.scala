package com.kafka.lab5

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.io.Source

class MonitoringConsumerManualOffset {

  var properties : Properties = null

  def readProperties(): Unit ={
    val url = getClass.getResource("/lab5/monitoringConsumer.properties")
    if(url != null){
      val source = Source.fromURL(url)
      properties = new Properties()
      properties.load(source.bufferedReader())
      println(properties)
    }
  }


  def consume() = {


    val kafkaProp = new Properties()
    kafkaProp.setProperty("bootstrap.servers",
      properties.getProperty("monitoring.consumer.bootstrap.servers") + ":" + properties.getProperty("monitoring.consumer.bootstrap.servers.port"))
    kafkaProp.setProperty(ConsumerConfig.GROUP_ID_CONFIG, properties.getProperty("monitoring.consumer.group.id"))
    kafkaProp.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, properties.getProperty("enable.auto.commit"))
    kafkaProp.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, properties.getProperty("monitoring.consumer.key.deserializer"))
    kafkaProp.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, properties.getProperty("monitoring.consumer.value.deserializer"))

    val topic = properties.getProperty("monitoring.consumer.service.topic")



    val kafkaConsumer = new KafkaConsumer[String, String](kafkaProp)

    val rebalanceListner = new ManualOffsetCommitRebalanceListener(kafkaConsumer)

    kafkaConsumer.subscribe(Collections.singletonList(topic), rebalanceListner)

    while (true) {
      val records = kafkaConsumer.poll(100)
      val it = records.iterator()
      while (it.hasNext) {
        val rec = it.next()
        println(s"Process the record: ${rec}")

        rebalanceListner.addOffset(rec.topic, rec.partition, rec.offset)

      }
      // kafkaConsumer.commitSync() to commit the offset to the topic
      kafkaConsumer.commitSync(rebalanceListner.getCurrentOffsets)

    }
  }

}


object MonitoringConsumerManualOffset extends App{
  val consumer = new MonitoringConsumerManualOffset()
  consumer.readProperties()
  consumer.consume()
}

