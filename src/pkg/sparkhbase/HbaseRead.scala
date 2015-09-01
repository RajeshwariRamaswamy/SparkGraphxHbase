
package pkg.sparkhbase

import org.apache.spark._
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
/**
 * @author biadmin
 */
class HbaseRead {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBaseRead").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    val tableName = "SparkTest"

    conf.set("hbase.master", "localhost:8080")
    conf.setInt("timeout", 120000)
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(tableName)
      admin.createTable(tableDesc)
    }

     val hBaseRDD = sc.newAPIHadoopRDD(conf, 
      classOf[TableInputFormat], 
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //get the row count
      //hbaseConfig.set(TableInputFormat.SCAN_COLUMNS, "o:fname o:email");
    println("Number of Records found : " + hBaseRDD.count())
    sc.stop()
  }
}