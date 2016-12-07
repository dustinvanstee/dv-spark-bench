package dv.sparkbench.main
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

class sparkbenchmark(sc : SparkContext) {

    //val s = new linearSettings(200,200,0.5,5,0.0)
    
    def benchmarkLinearRegression(s : utils.linearSettings) = {
        // Create Fake Data
        val sqlContext = new SQLContext(sc) 
        import sqlContext.implicits._

        val (data, loadTime) = utils.time { generateLinearRDD(sc, s.numExamples,s.numFeatures,s.epsilon,s.numPartitions,s.intercept).toDF() }
    
        val lr = new LinearRegression().setMaxIter(50).setRegParam(0.3).setElasticNetParam(0.8)
    
        // Train the Model
        val (lrModel,fitTime) = utils.time { lr.fit(data) }
        // Predictions
        val (predicitons, predTime) = utils.time {lrModel.transform(data)}
        println(f"loadTime =$loadTime%2.2f secs")
        println(f"fitTime  =$fitTime%2.2f secs")
        println(f"predTime =$predTime%2.2f secs")

        (loadTime,fitTime,predTime)
    }

  override def toString : String = {
  	"add something here ..."
  }

}

object sparkbenchmark {
    def main(args: Array[String]) {
        val sc = new SparkContext
        val test = new sparkbenchmark(sc)
        val ts = new utils.linearSettings(200,200,0.5,5,0.0)
      
        println("Test run =" + test.benchmarkLinearRegression(ts))
    }
} 
