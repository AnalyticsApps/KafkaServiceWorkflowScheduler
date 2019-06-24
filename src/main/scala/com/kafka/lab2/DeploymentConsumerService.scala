package com.kafka.lab2

import java.util.{Collections, Properties}
import java.util.concurrent.Executors

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.io.Source

/**
  * Reading from deployment topic for deplyment action of a service
  *
  * Example for creating Kafka Custom Deserializer for case class ServiceDataModel.
  */

class DeploymentConsumerService {

  var properties : Properties = null

  def readProperties(): Unit ={
    val url = getClass.getResource("/lab2/deploymentService.properties")
    if(url != null){
      val source = Source.fromURL(url)
      properties = new Properties()
      properties.load(source.bufferedReader())
      println(properties)
    }
  }


  def consumeDeploymentTopic() = {
    val kafkaProp = new Properties()
    kafkaProp.setProperty("bootstrap.servers",
      properties.getProperty("deployment.consumer.bootstrap.servers") + ":" + properties.getProperty("deployment.consumer.bootstrap.servers.port"))
    kafkaProp.setProperty(ConsumerConfig.GROUP_ID_CONFIG, properties.getProperty("deployment.consumer.group.id"))
    kafkaProp.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getProperty("deployment.consumer.auto.offset.reset"))
    kafkaProp.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, properties.getProperty("deployment.consumer.auto.commit.enable"))
    kafkaProp.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, properties.getProperty("deployment.consumer.auto.commit.interval.ms"))
    kafkaProp.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, properties.getProperty("deployment.consumer.session.timeout.ms"))
    kafkaProp.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, properties.getProperty("deployment.consumer.key.deserializer"))
    kafkaProp.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, properties.getProperty("deployment.consumer.value.customDeserializer"))

    val topic = properties.getProperty("deployment.consumer.service.topic")

    val kafkaConsumer = new KafkaConsumer[String, ServiceDataModel](kafkaProp)

    kafkaConsumer.subscribe(Collections.singletonList(topic))

    Executors.newSingleThreadExecutor.execute(    new Runnable {
      override def run(): Unit = {
        while (true) {
          val records = kafkaConsumer.poll(1000)
          records.iterator().forEachRemaining(record => {
            val key : String = record.key()
            val value : ServiceDataModel = record.value()
            println(s"Received message - serviceName: ${value.serviceName} action: ${value.action}")

          })
        }
      }
    })

  }
}

object DeploymentConsumerService extends App{
  val deploymentService = new DeploymentConsumerService()
  deploymentService.readProperties()
  deploymentService.consumeDeploymentTopic()
}

