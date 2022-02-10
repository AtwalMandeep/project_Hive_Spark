object hello  {

  def main(args: Array[String]) {
  }
  while (true) {
    val option="""
                 |SELECT YOUR OPTION
                 |To view the total consumer counts, enter 1
                 |To view the most consumed beverages, enter 2
                 |To view the least consumed beverages, enter 3
                 |To view the averaged consumed beverages, enter 4
                 |To view all available beverages, enter 5
                 |To view common available beverages, enter 6
                 |To view the table partition,enter 7
                 |To view the table view,enter 8
                 |To delete a row from record or add a "note","comment", enter 9
                 |To see future trends, enter 10
                 |8.Exit
                 |""".stripMargin

    println(option)
    var lineInput = scala.io.StdIn.readInt()
    actionMatch(lineInput)

    def actionMatch(lineInput: Int) {
     // lineInput match {
       /* case 1 => s.getConsumedCount()
        case 2 => s.getMostConsumedBev()
        case 3 => s.getLeastConsumedBev()
        case 4 => s.geAvgConsumedBev()
        case 5 => s.getAllBev()
        case 6 => s.getCommonBev()
        case 7=> s.getTablePartition()
        case 8=> s.getTableView()
        case 9=> s.getDeleteRow()
        case 10=> s.AddComment()
        case 8 => s.exit()
          System.exit(0);
        case _ => println("Thank you")*/
      }

    }
  }
//}