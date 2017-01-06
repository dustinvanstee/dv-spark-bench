package dv.sparkbench.LinearRegression
// some sbt commands
// inspect scalaVersion
// inspect libraryDependencies

//import sqlContext.implicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.mllib.util.LinearDataGenerator._
import org.apache.spark.ml.regression.LinearRegression
import dv.sparkbench.utils._
import dv.sparkbench.bmc._

case class linearSettings(numExamples:Integer, numFeatures : Integer, epsilon : Double, numPartitions : Integer, intercept : Double, iterations : Integer, runtime : Double = -999999)


class linearRegressionBenchmark(sc : SparkContext) extends bmCommon  {
    type T = linearSettings
    implicit var verbose = false


    var paramList  = List[linearSettings]()
    var runResults = List[linearSettings]()
    
    def run(s : linearSettings) = {
        // For now generate data /// perform test are in the same function.  Probably could refactor, but ok for now ...
        val sqlContext = new SQLContext(sc) 
        import sqlContext.implicits._

        val (data, loadTime) = funcs.time { generateLinearRDD(sc, s.numExamples,s.numFeatures,s.epsilon,s.numPartitions,s.intercept).toDF() }
    
        val lr = new LinearRegression().setMaxIter(s.iterations).setRegParam(0.1).setElasticNetParam(0.1).setTol(0.00000000000000001)
   
        // Train the Model
        val (lrModel,fitTime) = funcs.time { lr.fit(data) }
        // Predictions
        val (predicitons, predTime) = funcs.time {lrModel.transform(data)}
        funcs.vprintln(f"loadTime =$loadTime%2.2f secs")

        funcs.vprintln(f"loadTime = $loadTime%2.2f secs")
        funcs.vprintln(f"fitTime  = $fitTime%2.2f secs")
        funcs.vprintln(f"predTime = $predTime%2.2f secs")

        //(loadTime,fitTime,predTime)
        val runtime = loadTime + fitTime + predTime
        val t = s.copy(runtime = runtime)
        t
    }

    //def loop = {
    //    var ind = 1
    //    this.paramList.foreach(s => {
    //        println("**** Iteration " + ind + " ****")
    //        var tmp = s
    //        tmp = this.run(s)
    //        runResults = runResults ::: List(tmp)
    //        ind += 1
    //    })
    //}
//
    //def addRun(r : linearSettings) = {
    //    this.paramList = this.paramList ::: List(r)
    //}
     // bc i cant override toString in linearSettings...
   def setHeadString() = {println("numExamples,numFeatures,numPartitions,iterations,runtime") }
   def setToString(a:linearSettings) = {println(a.numExamples + "," + a.numFeatures + "," + a.numPartitions + "," + a.iterations + "," + a.runtime) }


  def printResults = {
    this.setHeadString
    runResults.foreach(li => this.setToString(li)) 
  }

}

object linearRegressionBenchmark {
    def main(args: Array[String]) {
        val sc = new SparkContext
        val test = new linearRegressionBenchmark(sc)
        val ts = new linearSettings(numExamples=200, numFeatures=200, epsilon=0.5, numPartitions=10, intercept=1, iterations=15)
      
        println("Test run =" + test.run(ts))
    }


}
 
