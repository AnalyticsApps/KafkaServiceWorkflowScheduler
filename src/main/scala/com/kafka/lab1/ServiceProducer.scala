package com.kafka.lab1

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.io.Source

/**
  * Simple Producer example.
  *
  * Read the Kafka Connection properties from a property file and send to Kafka Topic.
  *
  * Example for Sync / Async Calls and Call back example.
  */

class ServiceProducer{

  var properties : Properties = null

  def readProperties(): Unit ={
    val url = getClass.getResource("/lab1/serviceProducer.properties")
    if(url != null){
      val source = Source.fromURL(url)
      properties = new Properties()
      properties.load(source.bufferedReader())
    }
  }

  def produce() = {

    val kafkaProp = new Properties()
    kafkaProp.setProperty("bootstrap.servers",
      properties.getProperty("bootstrap.servers") + ":" + properties.getProperty("bootstrap.servers.port"))
    kafkaProp.setProperty("client.id", properties.getProperty("client.id"))
    kafkaProp.setProperty("acks", properties.getProperty("acks"))
    kafkaProp.setProperty("retries", properties.getProperty("retries"))
    kafkaProp.setProperty("batch.size", properties.getProperty("batch.size"))
    kafkaProp.setProperty("linger.ms", properties.getProperty("linger.ms"))
    kafkaProp.setProperty("buffer.memory", properties.getProperty("buffer.memory"))
    kafkaProp.setProperty("key.serializer", properties.getProperty("key.serializer"))
    kafkaProp.setProperty("value.serializer", properties.getProperty("value.serializer"))

    val serviceTopic = properties.getProperty("service.topic")
    val serviceFile = properties.getProperty("servicefile")

    val producer = new KafkaProducer[String, String](kafkaProp)


    Source.fromFile(serviceFile).getLines.toList.map( line => {
      val items = line.split(",")
      val service = items(1).trim
      val authid = items(3).trim

      val producerRecord = new ProducerRecord[String,String](serviceTopic, authid, line)
      try {

        if ("SERVICE_COST_CALCULATOR".equals(service)) {
          println("callback")
          // Callback Function
          producer.send(producerRecord, new ServiceCostCallback(producerRecord))

        } else {
          // Send the record to topic async call
          producer.send(producerRecord)

          // Send the record to topic sync call
          // val recordMetadata: RecordMetadata = producer.send(producerRecord).get()
        }

      }catch {
        case e : Exception => {
          println(s"Error in processing line: $line")
        }

      }

    })

    producer.flush()
    producer.close()

  }

}


class ServiceCostCallback(producerRecord: ProducerRecord[String, String]) extends Callback {

  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (metadata != null) {
       println(s"Processing the cost. Offset is ${metadata.offset()}")
    }
    
  }
}



object ServiceProducer extends App{
  val serviceProducer = new ServiceProducer()
  serviceProducer.readProperties()
  serviceProducer.produce()
}