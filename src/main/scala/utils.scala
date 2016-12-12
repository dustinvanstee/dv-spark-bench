package dv.sparkbench.utils

object utils {

  def time[R](block: => R): (R,Double) = {
      val t0 = System.nanoTime()
      val result = block    // call-by-name
      val t1 = System.nanoTime()
      val delta  = (t1 - t0).toDouble / scala.math.pow(10,9)
      //println("Elapsed time: " + delta + "secs")
      (result, delta)
  }

  case class linearSettings(numExamples:Integer, numFeatures : Integer, epsilon : Double, numPartitions : Integer, intercept : Double, iterations : Integer)


}