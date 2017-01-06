package dv.sparkbench.bmc
//import dv.sparkbench.LinearRegression._


abstract class bmCommon {
	type T
	var paramList : List[T]

	// Takes in a type of parameter, and returns the partameter with runTime included
	def  run(s : T ) : T
	def  addRun(t : T)
    def  loop


}
