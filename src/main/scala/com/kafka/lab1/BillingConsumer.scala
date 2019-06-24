package com.kafka.lab1

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.io.Source

/**
  * Simple  Consumer example.
  *
  * Read the Kafka Connection properties from a property file and consume from Kafka Topic.
  *
  * Example for poll the topic in a loop.
  */

class BillingConsumer {

  var properties : Properties = null

  def readProperties(): Unit ={
    val url = getClass.getResource("/lab1/billingConsumer.properties")
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
      properties.getProperty("bootstrap.servers") + ":" + properties.getProperty("bootstrap.servers.port"))
    kafkaProp.setProperty(ConsumerConfig.GROUP_ID_CONFIG, properties.getProperty("group.id"))
    kafkaProp.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getProperty("auto.offset.reset"))
    kafkaProp.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, properties.getProperty("auto.commit.enable"))
    kafkaProp.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, properties.getProperty("auto.commit.interval.ms"))
    kafkaProp.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, properties.getProperty("session.timeout.ms"))
    kafkaProp.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, properties.getProperty("key.deserializer"))
    kafkaProp.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, properties.getProperty("value.deserializer"))

    val topic = properties.getProperty("service.topic")

    val kafkaConsumer = new KafkaConsumer[String, String](kafkaProp)

    kafkaConsumer.subscribe(Collections.singletonList(topic))

    while(true){
      val records = kafkaConsumer.poll(100)
      val it = records.iterator()
      while(it.hasNext){
        val rec = it.next()
        val data = rec.value().split(",")
        val action = data(2).trim
        if(action.startsWith("Calculate_cost"))
          println(s"Process the cost for User Id: ${data(3)} action: ${action}")
      }
    }

  }

}

object BillingConsumer extends App{
  val billingConsumer = new BillingConsumer()
  billingConsumer.readProperties()
  billingConsumer.consume()
}