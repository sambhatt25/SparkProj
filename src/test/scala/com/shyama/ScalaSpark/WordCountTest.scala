package com.shyama.ScalaSpark

//import com.shyama.ScalaSpark.WordCount
// Java
import java.io.{FileWriter, File}

// Scala
import scala.io.Source

// Google Guava
import com.google.common.io.Files

// Specs2
//import org.specs2.mutable.Specification
import org.scalatest.FunSuite
class WordCountTest extends FunSuite {

  test("WordCount_test"){
      val tempDir = Files.createTempDir()
      val inputFile = new File(tempDir, "input").getAbsolutePath
      val inWriter = new FileWriter(inputFile)
      inWriter.write("hack hack hack and hack")
      inWriter.close
      val outputDir = new File(tempDir, "output").getAbsolutePath

      WordCount.execute(
        master = "local",
        args   = List(inputFile, outputDir)
      )

      val outputFile = new File(outputDir, "part-00000")
      val actual = Source.fromFile(outputFile).mkString
      assert(actual=== "(hack,4)\n(and,1)\n")
    }
  
}
