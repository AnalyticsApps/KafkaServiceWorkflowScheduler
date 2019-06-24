package com.kafka.lab3


import java.util.concurrent.Executors
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, Partitioner, ProducerConfig, ProducerRecord, RecordMetadata}

import scala.io.Source


/**
  * Topic-to-Topic: Reading from Service Topic and based on some condition put to Monitoring topic
  *
  * Example for sending the messages to a partition.
  */
class MonitoringProducerService {

  var properties : Properties = null

  def readProperties(): Unit ={
    val url = getClass.getResource("/lab3/monitoringService.properties")
    if(url != null){
      val source = Source.fromURL(url)
      properties = new Properties()
      properties.load(source.bufferedReader())
      println(properties)
    }
  }

  def consumeServiceTopic() = {
    val kafkaProp = new Properties()
    kafkaProp.setProperty("bootstrap.servers",
      properties.getProperty("service.consumer.bootstrap.servers") + ":" + properties.getProperty("service.consumer.bootstrap.servers.port"))
    kafkaProp.setProperty(ConsumerConfig.GROUP_ID_CONFIG, properties.getProperty("service.consumer.group.id"))
    kafkaProp.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getProperty("service.consumer.auto.offset.reset"))
    kafkaProp.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, properties.getProperty("service.consumer.auto.commit.enable"))
    kafkaProp.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, properties.getProperty("service.consumer.auto.commit.interval.ms"))
    kafkaProp.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, properties.getProperty("service.consumer.session.timeout.ms"))
    kafkaProp.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, properties.getProperty("service.consumer.key.deserializer"))
    kafkaProp.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, properties.getProperty("service.consumer.value.deserializer"))


    val topic = properties.getProperty("service.consumer.service.topic")

    val kafkaConsumer = new KafkaConsumer[String, String](kafkaProp)

    kafkaConsumer.subscribe(Collections.singletonList(topic))


    Executors.newSingleThreadExecutor.execute(    new Runnable {
      override def run(): Unit = {
        while (true) {
          val records = kafkaConsumer.poll(1000)
          records.iterator().forEachRemaining(record => {
            val data = record.value().split(",")
            val action = data(2).trim
            val authId = data(3).trim
            if(data.length == 4 && ( action.startsWith("start") || action.startsWith("stop")) ){
              println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
              monitoringProducer(authId, record.value())

            }
          })
        }
      }
    })

  }


  def monitoringProducer(authId: String, msg: String): Unit ={

    val kafkaProp = new Properties()
    kafkaProp.setProperty("bootstrap.servers",
      properties.getProperty("monitoring.producer.bootstrap.servers") + ":" + properties.getProperty("monitoring.producer.bootstrap.servers.port"))
    kafkaProp.setProperty("client.id", properties.getProperty("monitoring.producer.client.id"))
    kafkaProp.setProperty("acks", properties.getProperty("monitoring.producer.acks"))
    kafkaProp.setProperty("retries", properties.getProperty("monitoring.producer.retries"))
    kafkaProp.setProperty("batch.size", properties.getProperty("monitoring.producer.batch.size"))
    kafkaProp.setProperty("linger.ms", properties.getProperty("monitoring.producer.linger.ms"))
    kafkaProp.setProperty("buffer.memory", properties.getProperty("monitoring.producer.buffer.memory"))
    kafkaProp.setProperty("key.serializer", properties.getProperty("monitoring.producer.key.serializer"))
    kafkaProp.setProperty("value.serializer", properties.getProperty("monitoring.producer.value.serializer"))

    kafkaProp.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG,new MonitoringPartitioner().getClass.getCanonicalName)
    kafkaProp.setProperty("partition.0","US")
    kafkaProp.setProperty("partition.1","UK")
    kafkaProp.setProperty("partition.2","OTHERS")


    val monitoringTopic = properties.getProperty("monitoring.producer.monitoring.topic")

    val producer = new KafkaProducer[String, String](kafkaProp)
    val producerRecord = new ProducerRecord[String,String](monitoringTopic,msg)

    try {
      val rec = producer.send(producerRecord, new MonitoringServiceCallback(producerRecord))

    }catch {
      case e : Exception => {
        println("Error in processing")
      }

    }
    producer.flush()
    producer.close()

  }




}

class MonitoringServiceCallback(producerRecord: ProducerRecord[String, String]) extends Callback {

  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (metadata != null) {
      println(s"Message of offset: ${metadata.offset()} is send to partition: ${metadata.partition()}")
    }

    if(exception != null){
      println(s"Error : ${exception.getMessage}")
    }

  }
}

object MonitoringProducerService extends App{
  val monitoringProducer = new MonitoringProducerService()
  monitoringProducer.readProperties()
  monitoringProducer.consumeServiceTopic()
}


