package dv.sparkbench.linearregression
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

class BenchmarkLinearRegression(sc : SparkContext) {

    //val s = new linearSettings(200,200,0.5,5,0.0)
    
    def run(s : utils.linearSettings) = {
        // Create Fake Data
        val sqlContext = new SQLContext(sc) 
        import sqlContext.implicits._

        val (data, loadTime) = utils.time { generateLinearRDD(sc, s.numExamples,s.numFeatures,s.epsilon,s.numPartitions,s.intercept).toDF() }
    
        val lr = new LinearRegression().setMaxIter(s.iterations).setRegParam(0.1).setElasticNetParam(0.1).setTol(0.00000000000000001)
   
        // Train the Model
        val (lrModel,fitTime) = utils.time { lr.fit(data) }
        // Predictions
        val (predicitons, predTime) = utils.time {lrModel.transform(data)}
        println(f"loadTime =$loadTime%2.2f secs")

        println(f"loadTime = $loadTime%2.2f secs")
        println(f"fitTime  = $fitTime%2.2f secs")
        println(f"predTime = $predTime%2.2f secs")

        (loadTime,fitTime,predTime)
    }

  override def toString : String = {
  	"Creating spark benchmark object.  Linear Gression and Terasort supported."
  }

}

object linearRegression {
    def main(args: Array[String]) {
        val sc = new SparkContext
        val test = new BenchmarkLinearRegression(sc)
        val ts = new utils.linearSettings(numExamples=200, numFeatures=200, epsilon=0.5, numPartitions=10, intercept=1, iterations=15)
      
        println("Test run =" + test.run(ts))
    }
} 
