package org.hari.Montecarlo

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{ StructType, DateType, StringType, StructField, DoubleType, TimestampType, IntegerType }
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Montecarlo {

  def main(args: Array[String]): Unit = {

    if (args.size < 6) {
      println("Usage: <MasterFile><instrumentTrial><ratingMatrix><spCapitalMatrix><Delimiter><TgtFileName>")
      sys.exit(1)
    }
    val masterFile = args(0)
    val instrumentTrialFile = args(1)
    val ratingMatrixFile = args(2)
    val spCapitalMatrixFile = args(3)
    val delimiter = args(4)
    val tgtFile = args(5)

    val sc = new SparkConf().setAppName("MonteCarlo").setMaster("local[*]")
    val sparkCont = new SparkContext(sc)
    val sqlCont = new HiveContext(sparkCont)
    val enterpriseIDMasterBaseLambda = createDF(masterFile, sqlCont, delimiter)
    val instrumentTrial = createDF(instrumentTrialFile, sqlCont,delimiter)
    val ratingMatrix = createDF(ratingMatrixFile, sqlCont, delimiter)
    val spCapitalMatrix = createDF(spCapitalMatrixFile, sqlCont, delimiter
        )

    val walGroup = when(enterpriseIDMasterBaseLambda.col("WAL") < 1, 1)
      .otherwise(when(enterpriseIDMasterBaseLambda.col("WAL") < 5, 2)
        .otherwise(when(enterpriseIDMasterBaseLambda.col("WAL") < 10, 3)
          .otherwise(when(enterpriseIDMasterBaseLambda.col("WAL") < 20, 4)
            .otherwise(5))))

    val concatenate = udf((first: Int, second: String) => { first.toString() + second })

    val joinInstrumentMaster = enterpriseIDMasterBaseLambda
      .join(instrumentTrial, enterpriseIDMasterBaseLambda.col("CUR_LOCAL_ID_IGI") === instrumentTrial.col("InstrumentID"))
      .withColumn("defaultLossCalculation", when(instrumentTrial.col("NoDefaultFlag") === 1, 0).otherwise(instrumentTrial.col("InstrumentValue")))
      .withColumn("trueUpMarketValue", instrumentTrial.col("InstrumentValue") * enterpriseIDMasterBaseLambda.col("TrueupRatio"))
      .withColumn("walGroup", walGroup)
      .withColumn("spGroup", concatenate(walGroup, enterpriseIDMasterBaseLambda.col("WaterFallRating")))

    val joinInstrumentMasterspCapital = joinInstrumentMaster.join(spCapitalMatrix, joinInstrumentMaster.col("spGroup") === spCapitalMatrix.col("TenorType"))
      .join(ratingMatrix, joinInstrumentMaster.col("WaterFallRating") === ratingMatrix.col("FromTo"))

    joinInstrumentMasterspCapital.show()
    joinInstrumentMasterspCapital.write.mode("overwrite").format("com.databricks.spark.csv").save(tgtFile)
    

  }

  def createDF(fullFilePath: String, hc: HiveContext, delimiter: String): DataFrame = {

    val myDF = hc.read.format("com.databricks.spark.csv")
      .option("Delimiter", delimiter)
      .option("inferSchema", "true")
      .option("header", "true").load(fullFilePath)
    myDF

  }

}