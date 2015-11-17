package my.spark.import_

import org.scalatest.FunSuite
import java.sql.ResultSet

class ImporterSuite extends FunSuite {
  test("Test importFromRDB[U](selectSQL: String, convertFunc: (ResultSet) => U): Array[U]") {
    val sql = """
      select distinct(roomid) from db_live_history 
      where 0 = 0 and dt between from_unixtime(1433088000, "%Y%m%d") and from_unixtime(1433174400,"%Y%m%d")
    """
    val convertFunc = (rSet: ResultSet) => { rSet.getInt(1) }

    val array = Importer.importFromRDB(sql, convertFunc)

    println(array.toSet)
  }

  test("Test importOneFromRDB[U](selectSQL: String, convertFunc: (ResultSet) => U): U") {
    val sql = """
      select distinct(roomid) from db_live_history 
      where 0 = 0 and dt between from_unixtime(1433088000, "%Y%m%d") and from_unixtime(1433174400,"%Y%m%d")
      limit 1
    """
    val convertFunc = (rSet: ResultSet) => { rSet.getInt(1) }

    val roomid = Importer.importOneFromRDB(sql, convertFunc)

    println(roomid)
  }

  test("Test importOneFromRDB with no result") {
    val sql = """
      select distinct(roomid) from db_live_history 
      where 0 != 0
    """

    val convertFunc = (rSet: ResultSet) => { rSet.getInt(1) }

    val roomid = Importer.importOneFromRDB(sql, convertFunc)

    println(roomid)
  }
}