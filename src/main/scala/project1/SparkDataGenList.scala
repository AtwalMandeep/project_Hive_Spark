package project1
import project1.DataConnection
import org.apache.spark.sql.SparkSession
class SparkDataGenList  {
  //def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
 val spark1 = new DataConnection()
 val spark=spark1.dataCon()
  /*  System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession.builder()
      .appName("problemAssign")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    spark.sparkContext.setLogLevel("WARN")*/

     // spark.sql(sqlText = "drop table BranchAndBeverageTable");
    //spark.sql(sqlText = "drop table BeverageAndConsumptionTable");

  // create table for BranchAndBeverageTable(A,B,C)
   /* spark.sql("create table IF NOT EXISTS BranchAndBeverageTable(beverageName String,branchName String) row format delimited fields terminated by ',' stored as textfile");
     spark.sql("create table IF NOT EXISTS Branch_BevDataSetA(beverageName String,branchName String) row format delimited fields terminated by ',' stored as textfile");
     spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE BranchAndBeverageTable")
     spark.sql("create table IF NOT EXISTS Branch_BevDataSetB(beverageName String,branchName String) row format delimited fields terminated by ',' stored as textfile");
     spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE BranchAndBeverageTable")
     spark.sql("create table IF NOT EXISTS Branch_BevDataSetC(beverageName String,branchName String) row format delimited fields terminated by ',' stored as textfile");
     spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE BranchAndBeverageTable")*/

    // create table for BeverageAndConsumptionTable(A,B,C)
     /*spark.sql("create table IF NOT EXISTS BeverageAndConsumptionTable(beverageName String,consumers Int) row format delimited fields terminated by ',' stored as textfile");
     spark.sql("create table IF NOT EXISTS BevConsCountDataSetA(beverageName String,consumers Int) row format delimited fields terminated by ','stored as textfile");
     spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE BeverageAndConsumptionTable")
     spark.sql("create table IF NOT EXISTS BevConsCountDataSetB(beverageName String,consumers Int) row format delimited fields terminated by ','stored as textfile");
     spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE BeverageAndConsumptionTable")
     spark.sql("create table IF NOT EXISTS BevConsCountDataSetC(beverageName String,consumers Int) row format delimited fields terminated by ','stored as textfile");
     spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE BeverageAndConsumptionTable")*/

    // show Main Table Data Content
    // spark.sql("SELECT * FROM BranchAndBeverageTable").show()
    //spark.sql("SELECT * FROM BeverageAndConsumptionTable").show()

    def getConsumedCount():Unit={
        println("What is the total number of consumers for Branch1? ")
        spark.sql("SELECT SUM(BeverageAndConsumptionTable.consumers)as total_consumer FROM " +
               "BeverageAndConsumptionTable INNER JOIN BranchAndBeverageTable ON " +
               "BeverageAndConsumptionTable.beverageName = BranchAndBeverageTable .beverageName WHERE " +
                "BranchAndBeverageTable.branchName = 'Branch1'").show()
    }
   def getConsumedCountBranch2():Unit= {
       println("What is the number of consumers for Branch2? ")
       spark.sql("SELECT SUM(ca.consumers) FROM BeverageAndConsumptionTable ca INNER JOIN " +
      "BranchAndBeverageTable bb ON ca.beverageName = bb.beverageName WHERE bb.branchName = 'Branch2'").show()
    }
   def getMostConsumedBev():Unit= {
       println("What is the most consumed beverage on Branch1?")
       spark.sql("select BranchAndBeverageTable.beverageName,sum(BeverageAndConsumptionTable.consumers) " +
      "from BranchAndBeverageTable join BeverageAndConsumptionTable ON " +
      "BranchAndBeverageTable.beverageName= BeverageAndConsumptionTable.beverageName " +
      "Where branchName='Branch1' " +
      "group by BranchAndBeverageTable.beverageName " + "order by sum(BeverageAndConsumptionTable.consumers)DESC limit 1" ).show()
    }
   def getAvgConsumedBev():Unit= {
       println("What is the Average consumed beverage on Branch1?")
       spark.sql("select BranchAndBeverageTable.beverageName,AVG(BeverageAndConsumptionTable.consumers) " +
      "from BranchAndBeverageTable join BeverageAndConsumptionTable ON " +
      "BranchAndBeverageTable.beverageName= BeverageAndConsumptionTable.beverageName " +
      "Where branchName='Branch1' " +
      "group by BranchAndBeverageTable.beverageName " + "order by AVG(BeverageAndConsumptionTable.consumers)DESC limit 1" ).show()
    }
    def geLeastConsumedBev():Unit= {
        println("What is the most least beverage on Branch1?")
        spark.sql("select BranchAndBeverageTable.beverageName,sum(BeverageAndConsumptionTable.consumers) " +
               "from BranchAndBeverageTable join BeverageAndConsumptionTable ON " +
               "BranchAndBeverageTable.beverageName= BeverageAndConsumptionTable.beverageName " +
               "Where branchName='Branch2' " +
               "group by BranchAndBeverageTable.beverageName " + "order by sum(BeverageAndConsumptionTable.consumers)asc limit 1" ).show()
    }
    def getAllBev():Unit= {
        println("What are the beverage available on Branch10,Branch8,and Branch1?")
        spark.sql("select DISTINCT beverageName,branchName from BranchAndBeverageTable " +
                "where branchName = 'Branch1' or branchName = 'Branch8' or branchName='Branch10' order by branchName").show(100)
    }
    def getCommonBev():Unit= {
        println("What are the common bevrages available in Branch4,Branch7 ?")
        spark.sql("SELECT DISTINCT bb.beverageName,bb.branchName FROM BranchAndBeverageTable bb " +
               "INNER JOIN BeverageAndConsumptionTable cc ON bb.beverageName = cc.beverageName " +
               "WHERE branchName='Branch4' OR branchName='Branch7' order by branchName").show(200)
    }
    def getTableView():Unit= {
        println("Create a View for the scenario3.")
        spark.sql("create view if not exists Bev_List1 as select distinct beverageName, branchName " +
              "from BranchAndBeverageTable where branchName = 'Branch1' or branchName = 'Branch8'")
        spark.sql("select distinct beverageName,branchName from " +
              "Bev_List1 group by branchName,beverageName order by branchName").show(100)
  }
    //new table and make new partition on it
    /* spark.sql("create table IF NOT EXISTS branches_table_nonpartitioned( beverages String, branches String) row format delimited fields terminated by ',' stored as textfile");
     spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' OVERWRITE INTO TABLE branches_table_nonpartitioned")
     spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE branches_table_nonpartitioned")
     spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE branches_table_nonpartitioned")

     spark.sql("CREATE TABLE IF NOT EXISTS partitioned_branches_table(beverages STRING) PARTITIONED BY(branches STRING)")
     spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
     spark.sql("INSERT OVERWRITE TABLE partitioned_branches_table PARTITION(branches) SELECT beverages,branches from branches_table_nonpartitioned")
     spark.sql("SELECT * from partitioned_branches_table").show(200)*/
    def getTablePartition():Unit= {
    // delete branchName giving issue
        println("Create a partition for the scenario3.")
       //spark.sql("drop table branch_part")
       spark.sql("CREATE TABLE IF NOT EXISTS branch_part(beveragename STRING) PARTITIONED BY(branchname STRING)")
       spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
       spark.sql("INSERT OVERWRITE TABLE branch_part PARTITION(branchname) SELECT beverageName,branchName from BranchAndBeverageTable")
       spark.sql("SELECT * from branch_part").show(200)
  }
   /*def AddComment():Unit= {
     println("Problem Scenario 5 - Alter the table properties to add 'note','comment'")
     spark.sql("create table if not exists example2(firstName String COMMENT 'THIS IS MY FIRST NAME',lastName String) row format delimited fields terminated by ','");
     spark.sql("LOAD DATA LOCAL INPATH 'inputdemo/kvi.txt' INTO TABLE example2")

    spark.sql("ALTER TABLE example2 SET TBLPROPERTIES('notes' = '$note')")
    spark.sql("SHOW TBLPROPERTIES example2").show()
    spark.sql("select firstName,lastName from example2").show()
    spark.sql("describe formatted example2").show()
    }*/
     def getDeleteRow() {
       println("Remove the row 5 from any of Scenario and add 'note','comment'")
       //spark.sql("create table IF NOT EXISTS BranchAndBeverageTable(beverageName String COMMENT 'Special_Lite ROW IS DELETED',branchName String) " +
           // "row format delimited fields terminated by ',' stored as textfile");
         spark.sql("select * from BranchAndBeverageTable").show()
         spark.sql("ALTER TABLE BranchAndBeverageTable SET TBLPROPERTIES('note' = 'Special_Lite DELETED')")
         spark.sql("SHOW TBLPROPERTIES BranchAndBeverageTable").show()
          spark.sql("Select *from BranchAndBeverageTable LEFT JOIN " +
                "(select *FROM BranchAndBeverageTable Where beverageName='Special_Lite'ORDER BY beverageName DESC LIMIT 1) " +
                 "B ON BranchAndBeverageTable.beverageName=B.beverageName WHERE B.beverageName IS NULL").show();
     }
      def AddFutureQuery() {
       //spark.sql(("drop table branches_table_future_query"))
       println("Problem Scenario- Add a Future query")
        spark.sql("create table IF NOT EXISTS branches_table_future_query(day Int,consumption Int) " +
                  "row format delimited fields terminated by ',' stored as textfile");
        spark.sql("LOAD DATA LOCAL INPATH 'input/prediction.txt' OVERWRITE INTO TABLE branches_table_future_query")
        spark.sql("select * from branches_table_future_query").show()
      }
      def exit(): Unit ={
        println("Thank you")
         System.exit(0)
      }

}
//}

