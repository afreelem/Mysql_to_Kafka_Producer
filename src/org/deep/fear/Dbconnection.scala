package org.deep.fear
import java.io.File
import com.typesafe.config.ConfigFactory

import java.sql.{ Connection, DriverManager }

/**
 *
 * @author deependra
 *
 */
class Dbconnection {

   /* Different Parameters which can also be reaad by config file */
  
  val url = "jdbc:mysql://localhost:3306"  
  val driver = "com.mysql.jdbc.Driver"
  val database = "deep"
  val userName = "root"
  val password = "redhat"
  val table = "t_employee"
  
  
  /* Columns is the required column you want in your query */

  def getData(columns: String): Array[Map[String, String]] = {
    
    
    
    var connection: Connection = null
        var rows: Array[Map[String, String]] = Array()

    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url + "/" + database, userName, password)
      val statement = connection.createStatement
      val query = "select " + columns + " from " + database + "." + table
      val rs = statement.executeQuery(query)
      while (rs.next) {
        var row: Array[String] = Array()
        for (col <- columns.split(",")) {
          row = row :+ rs.getString(col).toString()
        }
        val rowMap = columns.split(",").zip(row).toMap
        rows = rows :+ rowMap
      }
    } catch {
      case e: Exception => e.printStackTrace
    }
    connection.close
    rows
  }

}