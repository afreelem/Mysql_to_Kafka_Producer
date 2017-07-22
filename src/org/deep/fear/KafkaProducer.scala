package org.deep.fear

import java.util.Properties
import org.apache.kafka.clients.producer._

/**
 *
 * @author deependra
 *
 */

object KafkaProducer {

  def main(args: Array[String]): Unit = {

 val  props = new Properties()
 
 props.put("bootstrap.servers", "192.168.4.195:9092") // IP AND PORT OF KAFKA
  
 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

 val producer = new KafkaProducer[String, String](props)
 
 val TOPIC="test"
    
    // Checking whether config file is passed as Argument or not. If not, exiting from program.

    val db = new Dbconnection()
    val row = db.getData("employee_id,first_name,last_name,hire_date,department,termination_date,manager_employee_id")
    
    
   // db.InsertData(""" "12", "San" , "kum" , "2016-02-24 00:00:00" ,"technology" , "2016-03-25 00:00:00" , "25" """)
    
    
    val record:Array[String] = for( line <- row) yield(line.toList.map(x => x._2).mkString(","))
    

    
    for (line <- record){
      

       println(line)

     val records = new ProducerRecord(TOPIC, "key", s"hello $line")
     producer.send(records)
      
    }
    

  
  }
}
  
  