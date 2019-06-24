package com.kafka.lab2

import java.util.concurrent.Executors
import java.util.{Collections, Properties}

import com.kafka.lab1.ServiceCostCallback
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

/**
  * Topic-to-Topic: Reading from Service Topic and based on some condition put to deployment topic
  *
  * Example for creating Kafka Custom Serializer for case class ServiceDataModel.
  */
class DeploymentProducerService {

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
            if(data.length == 4 && ( action.startsWith("start") || action.startsWith("stop")) ){
              println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
              val serviceDataModel = new ServiceDataModel(data(0).trim, data(1).trim, action, data(3).trim)
              deploymentProducer(serviceDataModel)

            }
          })
        }
      }
    })

  }


  def deploymentProducer(serviceDataModel: ServiceDataModel): Unit ={

    val kafkaProp = new Properties()
    kafkaProp.setProperty("bootstrap.servers",
      properties.getProperty("deployment.producer.bootstrap.servers") + ":" + properties.getProperty("deployment.producer.bootstrap.servers.port"))
    kafkaProp.setProperty("client.id", properties.getProperty("deployment.producer.client.id"))
    kafkaProp.setProperty("acks", properties.getProperty("deployment.producer.acks"))
    kafkaProp.setProperty("retries", properties.getProperty("deployment.producer.retries"))
    kafkaProp.setProperty("batch.size", properties.getProperty("deployment.producer.batch.size"))
    kafkaProp.setProperty("linger.ms", properties.getProperty("deployment.producer.linger.ms"))
    kafkaProp.setProperty("buffer.memory", properties.getProperty("deployment.producer.buffer.memory"))
    kafkaProp.setProperty("key.serializer", properties.getProperty("deployment.producer.key.serializer"))
    kafkaProp.setProperty("value.serializer", properties.getProperty("deployment.producer.value.customSerializer"))

    val deploymentTopic = properties.getProperty("deployment.producer.deployment.topic")

    val producer = new KafkaProducer[String, ServiceDataModel](kafkaProp)
    val producerRecord = new ProducerRecord[String,ServiceDataModel](deploymentTopic, serviceDataModel.authID, serviceDataModel)

    try {
      val recordMetadata = producer.send(producerRecord).get()
      println(s"Added to the topic: ${deploymentTopic} Offset: ${recordMetadata.offset()}")
    }catch {
      case e : Exception => {
        println("Error in processing")
      }

    }
    producer.flush()
    producer.close()

  }




}

object DeploymentProducerService extends App{
  val deploymentService = new DeploymentProducerService()
  deploymentService.readProperties()
  deploymentService.consumeServiceTopic()
}
