package dv.sparkbench.bmc
//import dv.sparkbench.LinearRegression._


abstract class bmCommon {
	type T
	var paramList : List[T]
	var runResults : List[T]
	var runLabel = "na"

	// Takes in a type of parameter, and returns the partameter with runTime included
	def  run(s : T ) : T

	def addRun(r : T) = {
        this.paramList = this.paramList ::: List(r)
    }

    def  loop = {
        //var ind = 1
        this.paramList.foreach(s => {
            //println("**** Iteration " + ind + " ****")
            var tmp = s
            tmp = this.run(s)
            runResults = runResults ::: List(tmp)
            //ind += 1
        })
    }

    def setRunLabel(s : String) = {
    	runLabel = s
    }

    def printResults

}
