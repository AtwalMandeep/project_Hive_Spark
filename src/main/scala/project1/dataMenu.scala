package project1
import scala.io.StdIn
object dataMenu extends ScenariosData
{
  //var s = new SparkDataGenList()
  def main(args: Array[String]) {
    while (true) {
      println(
        """
          |************************
          |Enter a number to select
          |Scenario List.************
          |************************
          |1. Scenario 1 **********
          |2. Scenario 2 **********
          |3. Scenario 3 **********
          |4. Scenario 4 **********
          |5. Scenario 5 **********
          |6. Scenario 6 *****
          |7. Quit ****************
          |************************
          |""".stripMargin)
      var lineInput = scala.io.StdIn.readInt()
      actionMatch(lineInput)
    }
  }
      def actionMatch(lineInput: Int) {
        lineInput match {
          case 1 => {
            println("Results for Scenario 1 : ")
            scenarioOne()
            //selectionMenu()

          }
          case 2 => {
            println("Results for Scenario 2 : ")
            scenarioTwo()
          }
          case 3 => {
            println("Results for Scenario 3 : ")
            scenarioThree()
          }
          case 4 => {
            println("Results for Scenario 4 : ")
            scenarioFour()
          }
          case 5 => {
            println("Results for Scenario 5 : ")
            scenarioFive()
          }
          case 6 => {
            println("Results for Scenario 6 : ")
            scenarioSix()
          }
          case 7 => {
            sys.exit(0)
          }
        }

      }

    }

