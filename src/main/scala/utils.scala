package dv.sparkbench.utils

object funcs {

  // This function returns whatever the original function returns, plus the time in a Tuple
  def time[R](block: => R): (R,Double) = {
      val t0 = System.nanoTime()
      val result = block    // call-by-name
      val t1 = System.nanoTime()
      val delta  = (t1 - t0).toDouble / scala.math.pow(10,9)
      //println("Elapsed time: " + delta + "secs")
      (result, delta)
  }

  // This just returns the time for functions that dont return anything
  def timeonly[R](block: => R): Double = {
      val t0 = System.nanoTime()
      val result = block    // call-by-name
      val t1 = System.nanoTime()
      val delta  = (t1 - t0).toDouble / scala.math.pow(10,9)
      //println("Elapsed time: " + delta + "secs")
      delta
  }

  def vprintln(s : String) (implicit verbose : Boolean) = {
  	if(verbose) { 
  		println(s)
  	}
  }


}
  //def vb[R](block: => R): R = {
  //    val t0 = System.nanoTime()
  //    val result = block    // call-by-name
  //    val t1 = System.nanoTime()
  //    val delta  = (t1 - t0).toDouble / scala.math.pow(10,9)
  //    //println("Elapsed time: " + delta + "secs")
  //    delta
  //}
//

  
