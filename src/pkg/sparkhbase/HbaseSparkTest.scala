package pkg.sparkhbase

import org.apache.spark._
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
/**
 * @author biadmin
 */
object HbaseSparkTest {
   def main(args: Array[String]) {
    //Initiate spark context with spark master URL. You can modify the URL per your environment. 
    val sc = new SparkContext("spark://INBLR-BIGDATA2:7077", "biadmin")
     sc.addJar("/usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.1.jar")
     sc.addJar("/usr/local/hbase/lib/hadoop-mapreduce-client-core-2.5.1.jar")
     sc.addJar("/usr/local/hbase/lib/hbase-annotations-1.0.1.1.jar")
     sc.addJar("/usr/local/hbase/lib/hbase-client-1.0.1.1.jar")
     sc.addJar("/usr/local/hbase/lib/hbase-common-1.0.1.1.jar")
     sc.addJar("/usr/local/hbase/lib/hbase-server-1.0.1.1.jar")
     sc.addJar("/usr/local/hbase/lib/hbase-hadoop2-compat-1.0.1.1.jar")
     sc.addJar("/usr/local/Projects/lib/com.google.protobuf-2.4.0.jar")

    
    val tableName = "SparkTest"
    
    val conf = HBaseConfiguration.create()
    // Add local HBase conf
    conf.addResource(new Path("file:///usr/local/hbase/conf/hbase-site.xml"))
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    // create m7 table with column family
    val admin = new HBaseAdmin(conf)
    if(!admin.isTableAvailable(tableName)) {
      print("Creating TestSpark Table")
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor("cf1"
                  .getBytes()));
      admin.createTable(tableDesc)
    }else{
      print("Table already exists!!")
      val columnDesc = new HColumnDescriptor("cf1");
    admin.disableTable(Bytes.toBytes(tableName));
      admin.addColumn(tableName, columnDesc);
    admin.enableTable(Bytes.toBytes(tableName));
    }

    //put data into table
    val myTable = new HTable(conf, tableName);
    for (i <- 0 to 5) {
    var  p = new Put(new String("row" + i).getBytes());
    p.add("cf1".getBytes(), "column-1".getBytes(), new String(
            "value " + i).getBytes());
    myTable.put(p);
  }
  myTable.flushCommits();
    
  //create rdd
    val hBaseRDD = sc.newAPIHadoopRDD(conf, 
      classOf[TableInputFormat], 
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //get the row count
    val count = hBaseRDD.count()
    print("HBase RDD count:"+count)
    System.exit(0)
  }
}