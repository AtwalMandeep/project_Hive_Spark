package project1
import org.apache.spark.sql.SparkSession
class ScenariosData {
  val spark1 = new DataConnection()
  val spark=spark1.dataCon()

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

  // spark.sql(sqlText = "drop table BranchAndBeverageTable");

  def scenarioOne() : Unit = {
    /*Problem Scenario 1
    What is the total number of consumers for Branch1?
    What is the number of consumers for the Branch2?*/
    println("What is the total number of consumers for Branch 1?")
    spark.sql("SELECT SUM(BeverageAndConsumptionTable.consumers)as total_consumer FROM " +
      "BeverageAndConsumptionTable INNER JOIN BranchAndBeverageTable ON " +
      "BeverageAndConsumptionTable.beverageName = BranchAndBeverageTable .beverageName WHERE " +
      "BranchAndBeverageTable.branchName = 'Branch1'").show()
    println("What is the total number of consumers for Branch 2?")
    spark.sql("SELECT SUM(ca.consumers) FROM BeverageAndConsumptionTable ca INNER JOIN " +
      "BranchAndBeverageTable bb ON ca.beverageName = bb.beverageName WHERE bb.branchName = 'Branch2'").show()

  }
  def scenarioTwo() : Unit = {
    //What is the most consumed beverage on Branch1
    //What is the Average consumed beverage on Branch2
    //What is the Least consumed beverage of  Branch2
    println("What is the most consumed beverage for Branch 1?")
    spark.sql("select BranchAndBeverageTable.beverageName,sum(BeverageAndConsumptionTable.consumers) " +
      "from BranchAndBeverageTable join BeverageAndConsumptionTable ON " +
      "BranchAndBeverageTable.beverageName= BeverageAndConsumptionTable.beverageName " +
      "Where branchName='Branch1' " +
      "group by BranchAndBeverageTable.beverageName " + "order by sum(BeverageAndConsumptionTable.consumers)DESC limit 1" ).show()

    println("What is the least consumed beverage for Branch 2?")
    spark.sql("select BranchAndBeverageTable.beverageName,sum(BeverageAndConsumptionTable.consumers) " +
      "from BranchAndBeverageTable join BeverageAndConsumptionTable ON " +
      "BranchAndBeverageTable.beverageName= BeverageAndConsumptionTable.beverageName " +
      "Where branchName='Branch2' " +
      "group by BranchAndBeverageTable.beverageName " + "order by sum(BeverageAndConsumptionTable.consumers)asc limit 1" ).show()

    println("What is the Average consumed beverage of Branch 2?")
    spark.sql("select BranchAndBeverageTable.beverageName,AVG(BeverageAndConsumptionTable.consumers) " +
      "from BranchAndBeverageTable join BeverageAndConsumptionTable ON " +
      "BranchAndBeverageTable.beverageName= BeverageAndConsumptionTable.beverageName " +
      "Where branchName='Branch1' " +
      "group by BranchAndBeverageTable.beverageName " + "order by AVG(BeverageAndConsumptionTable.consumers)DESC limit 1" ).show()

  }

  def scenarioThree() : Unit = {
    /*Problem Scenario 3
    What are the beverages available on Branch10, Branch8, and Branch1?
    what are the comman beverages available in Branch4,Branch7?*/
    println("What are the beverage available on Branch10,Branch8,and Branch1?")
    spark.sql("select DISTINCT beverageName,branchName from BranchAndBeverageTable " +
      "where branchName = 'Branch1' or branchName = 'Branch8' or branchName='Branch10' order by branchName").show(100)

    println("What are the common beverages available between Branch 4 and Branch 7?")
    spark.sql("SELECT DISTINCT bb.beverageName,bb.branchName FROM BranchAndBeverageTable bb " +
      "INNER JOIN BeverageAndConsumptionTable cc ON bb.beverageName = cc.beverageName " +
      "WHERE branchName='Branch4' OR branchName='Branch7' order by branchName").show(200)
  }
  def scenarioFour() : Unit = {
    /*Problem Scenario 4
   create a partition,View for the scenario3.*/
    println("Create a partitioned table for Scenario 3: ")
    spark.sql("CREATE TABLE IF NOT EXISTS branch_part(beveragename STRING) PARTITIONED BY(branchname STRING)")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("INSERT OVERWRITE TABLE branch_part PARTITION(branchname) SELECT beverageName,branchName from BranchAndBeverageTable")
    spark.sql("SELECT * from branch_part").show(200)

    println("Create a View for the scenario3.")
    spark.sql("create view if not exists Bev_List1 as select distinct beverageName, branchName " +
      "from BranchAndBeverageTable where branchName = 'Branch1' or branchName = 'Branch8'")
    spark.sql("select distinct beverageName,branchName from " +
      "Bev_List1 group by branchName,beverageName order by branchName").show(100)

  }
  def scenarioFive() : Unit = {
    /*Problem Scenario 5
    Alter the table properties to add "note","comment"
    Remove a row from the any Senario.*/
    println("Alter a table to add comments")
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
  def scenarioSix() : Unit = {
    //println("Problem Scenario- Add a Future query")
    spark.sql("create table IF NOT EXISTS branches_table_future_query(day Int,consumption Int) " +
      "row format delimited fields terminated by ',' stored as textfile");
    spark.sql("LOAD DATA LOCAL INPATH 'input/prediction.txt' OVERWRITE INTO TABLE branches_table_future_query")
    spark.sql("select * from branches_table_future_query").show()

  }

}
