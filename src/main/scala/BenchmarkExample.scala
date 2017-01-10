/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is modified from  https://github.com/SparkTC/spark-bench
*/

package dv.sparkbench.ExampleBenchmark

// Imports are required for timer functions and 
// Benchmark interface
import dv.sparkbench.utils._
import dv.sparkbench.bmc._

// Define a case class to hold benchmark parameters
// number of parameters can be as long as you need
// add runtime at the end, and default is to some non sensical value (will be used for recording run results)
case class exampleBenchmarkSettings(a:Integer, b : String, c : Double,  runtime : Double = -999999)

// Define your class here.  Make sure you extend it with bmcCommon
// For spark applications, include sc : SparkContext
class exampleBenchmark() extends bmCommon  {
    // Define T to be exampleBenchmarkSettings case class type.  
    type T = exampleBenchmarkSettings
    // Setting for verbose/quiet print mode
    implicit var verbose = false

    // These two lists are required.
    // paramList is the list or run settings prior to running benchmark
    // runResults is a copy of this list with the runtime recorded ..
    var paramList  = List[exampleBenchmarkSettings]()
    var runResults = List[exampleBenchmarkSettings]()
    
    // This 
    def run(s : exampleBenchmarkSettings) = {

        // Generate some random data ...
        // here i will use the a member of the exampleBenchmarkSettings to construct a random Vector
        // note : funcs.time will return both the original value of whatever is enclosed in braces
        // as well as the time taken
        val (data, gentime) = funcs.time { Range(0,s.a).map(a => scala.math.random) }
    
        // now, just record the time to add the number in our contrived example
        val (sumval,runtime) = funcs.time { data.sum }
   
        funcs.vprintln(f"example runtime  = $runtime%2.2f secs")

        // Copy our settings instance and update the runtime
        // and return.  Thats it!
        val t = s.copy(runtime = runtime)
        t
    }
    // method loop defined in bmCommon

   // Some print statements required for having a nice csv final output  
   def setHeadString() = {println("label,a,b,c,runtime") }
   def setToString(a:exampleBenchmarkSettings) = {println(this.runLabel + "," + a.a + "," + a.b + "," + a.c + "," + a.runtime) }

    // Final Results.  Prints the header column names and then each example run with runtime
    def printResults = {
        this.setHeadString
        runResults.foreach(li => this.setToString(li)) 
    }

}
