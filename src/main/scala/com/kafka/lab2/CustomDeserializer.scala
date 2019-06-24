package com.kafka.lab2

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util

import org.apache.kafka.common.serialization.Deserializer

class CustomDeserializer extends Deserializer[ServiceDataModel] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): ServiceDataModel = {
    val byteIn = new ByteArrayInputStream(data)
    val objIn = new ObjectInputStream(byteIn)
    val obj = objIn.readObject().asInstanceOf[ServiceDataModel]
    byteIn.close()
    objIn.close()
    obj
  }

  override def close(): Unit = {}
}
