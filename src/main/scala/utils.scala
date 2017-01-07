package dv.sparkbench.utils

import org.apache.spark.SparkContext

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


  def swiftConnection(sc : SparkContext,  credentials : scala.collection.mutable.HashMap[String, String]) : Unit = {
    val pfx = "fs.swift.service." + credentials("name") 
    
    val conf = sc.hadoopConfiguration
    conf.set(pfx + ".auth.url", credentials("auth_url") + "/v3/auth/tokens" )
    conf.set(pfx + ".auth.endpoint.prefix", "endpoints")
    conf.set(pfx + ".tenant",   credentials("project_id"))
    conf.set(pfx + ".username", credentials("user_id"))
    conf.set(pfx + ".password", credentials("password"))
    conf.setInt(pfx + ".http.port", 8080)
    conf.set(pfx + ".region", credentials("region"))
    conf.setBoolean(pfx + ".public", true)
    
    // conf.set(pfx + ".apikey",   credentials("password"))
  
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

  
