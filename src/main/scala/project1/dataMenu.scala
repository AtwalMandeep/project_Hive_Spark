package project1
import scala.io.StdIn
object dataMenu extends SparkDataGenList
{
  //var s = new SparkDataGenList()
  def main(args: Array[String]) {
    while (true) {
      val option =
        """
          |SELECT YOUR OPTION
          |To view the total consumer counts, Enter 1
          |To view the consumer of Branch2 ,  Enter 2
          |To view the most consumed beverages,Enter 3
          |To view the averaged consumed beverages, Enter 4
          |To view the least consumed beverages, Enter 5
          |To view all available beverages, Enter 6
          |To view common available beverages, Enter 7
          |To view the table partition, Enter 8
          |To view the table view, Enter 9
          |To delete a row from record or add a "note","comment", Enter 10
          |To see future trends, Enter 11
          |12.Exit
          |""".stripMargin
      println(option)
      var lineInput = scala.io.StdIn.readInt()
      actionMatch(lineInput)
    }
  }
      def actionMatch(lineInput: Int) {
        lineInput match {
          case 1 => getConsumedCount()
          case 2 => getConsumedCountBranch2()
          case 3 => getMostConsumedBev()
          case 4 => getAvgConsumedBev()
          case 5 => geLeastConsumedBev()
          case 6 => getAllBev()
          case 7 => getCommonBev()
          case 8 => getTablePartition()
          case 9 => getTableView()
          case 10 => getDeleteRow()
          case 11 => AddFutureQuery()
          case 12 => exit()
            System.exit(0);
          case _ => println("Thank you")
        }

      }

    }

