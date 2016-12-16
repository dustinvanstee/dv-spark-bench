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

package dv.sparkbench.terasort
import org.apache.log4j.Logger
import org.apache.log4j.Level

import com.google.common.primitives.UnsignedBytes
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}

import src.main.java.Unsigned16
import src.main.java.Random16
import src.main.scala._
import dv.sparkbench.utils._

/**
 * This file is modified from  com.github.ehiggs.spark.terasort.*
 */
object terasort {

    implicit val caseInsensitiveOrdering = UnsignedBytes.lexicographicalComparator

    def sort(sc : SparkContext, inputFile: String, outputFile : String) : Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);

        // Process command line arguments
        println("## Loading Data ## ")
        val (dataset, loadtime) = utils.time { sc.newAPIHadoopFile[Array[Byte], Array[Byte], TeraInputFormat](inputFile) }

        println("## Sorting Data ## ")
        val (sorted,  sorttime) = utils.time { dataset.partitionBy(new TeraSortPartitioner(dataset.partitions.size)).sortByKey() }

        println("## Writing Data ## ")
        val (rc, writetime) = utils.time { sorted.saveAsNewAPIHadoopFile[TeraOutputFormat](outputFile) }
        println("===================== TeraSort Summary ====================================")
        println("===========================================================================")
        println("sorttime  = " + sorttime)
        println("loadtime  = " + loadtime)
        println("writetime = " + writetime)
        println("===========================================================================")
        println("===========================================================================")




    }
}