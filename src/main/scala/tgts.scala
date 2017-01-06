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
 * This file is modified from  com.github.ehiggs.spark.terasort.*
 * This file is modified from  https://github.com/SparkTC/spark-bench
*/

package dv.sparkbench.terasort

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import sys.process._
//import org.apache.spark.{SparkConf, SparkContext}

// Custom framework
import com.google.common.primitives.UnsignedBytes
import dv.sparkbench.utils.{funcs}
import dv.sparkbench.bmc._

// Custom 
import src.main.java.Unsigned16
import src.main.java.Random16
import src.main.scala._

//runsizes, datadir, sortdir, partitions
case class teraSettings(nrows :String, inputDir : String, outputDir : String, numPartitions : Integer,  runtime : Double = -999999)


class tgts( sc : SparkContext ) extends bmCommon {
    // made this implicit so i dont have to pass to vprint all the time
    implicit var verbose = false
    type T = teraSettings

    var paramList  = List[teraSettings]()
    var runResults = List[teraSettings]()

    //, verbose : Boolean

    def run( s : teraSettings) : teraSettings = {

        val outputSizeInBytes = tgts.sizeStrToBytes(s.nrows)
        val size = tgts.sizeToSizeStr(outputSizeInBytes)


        val datadir = s.inputDir + "." + s.nrows
        val sortdir = s.outputDir  + "." + s.nrows
        val tgentime  = funcs.timeonly { tgts.teragen(sc,  s.nrows , s.inputDir, s.numPartitions) }
        val tsorttime = funcs.timeonly { tgts.terasort(sc, s.inputDir, s.outputDir) }
        
        funcs.vprintln("Tgen time =  " + tgentime)
        funcs.vprintln("Tsort time = " + tsorttime)
        //(loadTime,fitTime,predTime)
        val runtime = tgentime + tsorttime
        val t = s.copy(runtime = runtime)
        t
       
      }

    def setHeadString() = {println("nrows,size,numPartitions,runtime") }
    def setToString(a:teraSettings) = {println(a.nrows + "," + tgts.sizeToSizeStr(tgts.sizeStrToBytes(a.nrows)) + "," + a.numPartitions + "," + a.runtime) }

    def printResults = {
        this.setHeadString
        runResults.foreach(li => this.setToString(li)) 
    }

}

object tgts {
    implicit val caseInsensitiveOrdering = UnsignedBytes.lexicographicalComparator
    implicit var verbose = false

    def teragen(sc : SparkContext, numberOfRecords : String,  outputFilePath : String, numPartitions : Integer )  = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);

        // Process command line arguments
        val outputSizeInBytes = sizeStrToBytes(numberOfRecords)
        val size = sizeToSizeStr(outputSizeInBytes)

        //val conf = new SparkConf().setAppName(s"TeraGen ($size)")
        //val sc = new SparkContext(conf)

        val parts = numPartitions
        val recordsPerPartition = outputSizeInBytes / 100 / parts
        
        //val numRecords= outputSizeInBytes * parts
        funcs.vprintln("===========================================================================")
        funcs.vprintln("===========================================================================")
        funcs.vprintln(s"Input size: $size")
        funcs.vprintln(f"Total number of records: $numberOfRecords%s")
        funcs.vprintln(s"Number of output partitions: $parts")
        funcs.vprintln(s"Number of records/ partition:   $recordsPerPartition")
        funcs.vprintln("===========================================================================")
        funcs.vprintln("===========================================================================")

        assert(recordsPerPartition < Int.MaxValue, s"records per partition > ${Int.MaxValue}")


        val dataset = sc.parallelize(1 to parts, parts).mapPartitionsWithIndex { case (index, _) =>
            val one = new Unsigned16(1)
            val firstRecordNumber = new Unsigned16(index.toLong * recordsPerPartition.toLong)
            val recordsToGenerate = new Unsigned16(recordsPerPartition)
            val recordNumber = new Unsigned16(firstRecordNumber)
            val lastRecordNumber = new Unsigned16(firstRecordNumber)
            lastRecordNumber.add(recordsToGenerate)
            val rand = Random16.skipAhead(firstRecordNumber)
            val rowBytes: Array[Byte] = new Array[Byte](TeraInputFormat.RECORD_LEN)
            val key = new Array[Byte](TeraInputFormat.KEY_LEN)
            val value = new Array[Byte](TeraInputFormat.VALUE_LEN)
            //val a = scala.util.Random

            Iterator.tabulate(recordsPerPartition.toInt) { offset =>

                Random16.nextRand(rand)
                //val rand = new Unsigned16(recs, recs)
                generateRecord(rowBytes, rand, recordNumber)
                //generateRecordDv(rowBytes, rand, new Unsigned16(recs))
                //rowBytes(0) = recs.toByte
                recordNumber.add(one)
                rowBytes.copyToArray(key, 0, TeraInputFormat.KEY_LEN)
                rowBytes.takeRight(TeraInputFormat.VALUE_LEN).copyToArray(value, 0, TeraInputFormat.VALUE_LEN)
                //var keyr = key.map("%02X" format _).mkString("","","")
                //val valuer = value.map { a => a.asInstanceOf[Long].toHexString }.mkString("","","")
                (key ,value)
            }
        }

        val firstrecord = dataset.take(1).mkString("","","\n")
        //println(f"Dataset Generation Completed.  First Records = $firstrecord%s")
        dataset.saveAsNewAPIHadoopFile[TeraOutputFormat](outputFilePath)
        //println("Number of records written: " + dataset.count())
    }

    def terasort(sc : SparkContext, inputFile: String, outputFile : String) : Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);

        // Process command line arguments
        funcs.vprintln("## Loading Data ## ")
        val (dataset, loadtime) = funcs.time { sc.newAPIHadoopFile[Array[Byte], Array[Byte], TeraInputFormat](inputFile) }
        //val loadtime = 0
        //val dataset  = sc.newAPIHadoopFile[Array[Byte], Array[Byte], TeraInputFormat](inputFile) 

        funcs.vprintln("## Sorting Data ## ")
        val (sorted,  sorttime) = funcs.time { dataset.partitionBy(new TeraSortPartitioner(dataset.partitions.size)).sortByKey() }
        //val sorttime = 0
        //val sorted = dataset.partitionBy(new TeraSortPartitioner(dataset.partitions.size)).sortByKey() 

        funcs.vprintln("## Writing Data ## ")
        val (rc, writetime) = funcs.time { sorted.saveAsNewAPIHadoopFile[TeraOutputFormat](outputFile) }
        funcs.vprintln("===================== TeraSort Summary ====================================")
        funcs.vprintln("===========================================================================")
        funcs.vprintln("sorttime  = " + sorttime)
        funcs.vprintln("loadtime  = " + loadtime)
        funcs.vprintln("writetime = " + writetime)
        funcs.vprintln("===========================================================================")
        funcs.vprintln("===========================================================================")

    }

    
    // NumRecord * 100 Bytes/Record
    def sizeStrToBytes(str: String): Long = {
        val lower = str.toLowerCase
            if (lower.endsWith("k")) {
                lower.substring(0, lower.length - 1).toLong * 1000 * 100
            } else if (lower.endsWith("m")) {
                lower.substring(0, lower.length - 1).toLong * 1000 * 1000 * 100
            } else if (lower.endsWith("g")) {
                lower.substring(0, lower.length - 1).toLong * 1000 * 1000 * 1000 * 100
            } else if (lower.endsWith("t")) {
                lower.substring(0, lower.length - 1).toLong * 1000 * 1000 * 1000 * 1000 * 100
            } else {
                // no suffix, so it's just a number in bytes
                lower.toLong * 100
            }
    }

    def sizeToSizeStr(size: Long): String = {
        val kbScale: Long = 1000
            val mbScale: Long = 1000 * kbScale
            val gbScale: Long = 1000 * mbScale
            val tbScale: Long = 1000 * gbScale
            if (size > tbScale) {
                size / tbScale + "TB"
            } else if (size > gbScale) {
                size / gbScale  + "GB"
            } else if (size > mbScale) {
                size / mbScale + "MB"
            } else if (size > kbScale) {
                size / kbScale + "KB"
            } else { 
                size + "B"
            }
    }

    /**
     * Generate a binary record suitable for all sort benchmarks except PennySort.
     *
     * @param recBuf record to return
     */
    def generateRecord(recBuf: Array[Byte], rand: Unsigned16, recordNumber: Unsigned16): Unit = {
        // Generate the 10-byte key using the high 10 bytes of the 128-bit random number
        
        // grab the low order bytes first ...
        var i = 0
        while (i < 10) {
            recBuf(i) = rand.getByte(15-i)
                i += 1
        }

        // Add 2 bytes of "break"
        recBuf(10) = 0x00.toByte
            recBuf(11) = 0x11.toByte

            // Convert the 128-bit record number to 32 bits of ascii hexadecimal
            // as the next 32 bytes of the record.
            i = 0
            while (i < 32) {
                recBuf(12 + i) = recordNumber.getHexDigit(i).toByte
                    i += 1
            }

        // Add 4 bytes of "break" data
        recBuf(44) = 0x88.toByte
            recBuf(45) = 0x99.toByte
            recBuf(46) = 0xAA.toByte
            recBuf(47) = 0xBB.toByte

            // Add 48 bytes of filler based on low 48 bits of random number
            i = 0
            while (i < 12) {
                val v = rand.getHexDigit(20 + i).toByte
                    recBuf(48 + i * 4) = v
                    recBuf(49 + i * 4) = v
                    recBuf(50 + i * 4) = v
                    recBuf(51 + i * 4) = v
                    i += 1
            }

        // Add 4 bytes of "break" data
        recBuf(96) = 0xCC.toByte
            recBuf(97) = 0xDD.toByte
            recBuf(98) = 0xEE.toByte
            recBuf(99) = 0xFF.toByte
    
    }

    def generateRecordDv(ary_in: Array[Byte], rand: Unsigned16, recordNumber: Unsigned16) : Unit = {
       for(i <- 0 to 99) {
          ary_in(i) = rand.getByte(0)
       }
    }
    
}
