package org.deep.fear

import java.util
import java.io._

import org.apache.kafka.clients.consumer._

import scala.collection.JavaConverters._

/**
 * @author deependra
 */
object KafkaConsumer {

  def main(args: Array[String]): Unit = {

    import java.util.Properties

    val TOPIC = "test"

    val props = new Properties()
    props.put("bootstrap.servers", "192.168.4.195:9092")

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "something")

    val consumer = new KafkaConsumer[String, String](props)

    consumer.subscribe(util.Collections.singletonList(TOPIC))

    while (true) {
      val records = consumer.poll(100)
      for (record <- records.asScala) {

        // val pw = new PrintWriter(new File("consumes.txt" ))
        // pw.write(record.toString())
        println(record)

      }
    }

  }

}
  