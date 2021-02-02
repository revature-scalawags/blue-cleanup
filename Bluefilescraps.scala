package blue
package purple.Q1
package purple.Q2
package runner

import org.apache.spark.sql.{
  AnalysisException,
  DataFrame,
  SparkSession,
  functions,
  Row,
  SaveMode
}
import org.apache.spark.sql.functions.{explode, when, max, sum, avg, explode}
import org.apache.spark.sql.types.DoubleType
import java.io.PrintWriter
import java.nio.file.{Files, Paths}
import java.util.{Calendar, Date, Scanner}
import java.text.SimpleDateFormat
import breeze.linalg.{DenseVector, linspace}
import breeze.plot.{Figure, plot}
import breeze.numerics.{round, sqrt}
import scala.collection.mutable.ArrayBuffer
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, TimeoutException}
import scala.concurrent.duration.DurationInt
import scala.concurrent.Future._
import scala.io.StdIn
import scala.sys.exit
import purple.FileUtil.FileWriter.writeDataFrameToFile
import purple.Q1.HashtagsByRegion
import purple.Q2.HashtagsWithCovid
import blue.Question8
import green.Q4.Question4
import green.Q5.Question5

object BlueRunner {
  val spark = SparkSession
    .builder()
    .master("local[4]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  //Type path here

  //  val casepath2="C:\\Users\\liamh\\V2_Project_3\\201005-reston-bigdata\\pj3-bigdata\\daily_stats.tsv"
  //  val econpath2="C:\\Users\\liamh\\V2_Project_3\\201005-reston-bigdata\\economic_data_2018-2021.tsv"
  //
  //  val casepath1="C:\\Users\\river\\IdeaProjects\\201005-reston-bigdata\\pj3-bigdata\\daily_stats.tsv"
  //  val econpath1="C:\\Users\\river\\IdeaProjects\\201005-reston-bigdata\\economic_data_2018-2021.tsv"
  //
  //  val casepath4="C:\\Users\\issac\\IdeaProjects\\project 3\\daily_stats.tsv"
  //  val econpath4="C:\\Users\\issac\\IdeaProjects\\project 3\\economic_data_2018-2021.tsv"



  // Path must be changed to satisfy the path to our s3

  val casepath3 = "s3a://adam-king-848/data/daily_stats.tsv"
  val econpath3 = "s3a://adam-king-848/data/economic_data_2018-2021.tsv"
  val econpath = econpath3
  val casepath = casepath3
//  def main(args: Array[String]): Unit = {
//
//    if (args.length <= 2) {
//      System.err.println("EXPECTED 2 ARGUMENTS: AWS Access Key, then AWS Secret Key, then S3 Bucket")
//      System.exit(1)
//    }
//    val accessKey = args(0)
//    val secretKey = args(1)
//    val filePath = args(2)
//
//    val spark = SparkSession.builder().appName("sample").getOrCreate()
//
//    spark.sparkContext.addJar("https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.4/hadoop-aws-2.7.4.jar")
//    spark.sparkContext.addJar("https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar")
//    spark.sparkContext.addJar("https://repo1.maven.org/maven2/org/apache/spark/spark-sql_2.12/3.0.0/spark-sql_2.12-3.0.0.jar")
//    spark.sparkContext.addJar("https://repo1.maven.org/maven2/org/scalanlp/breeze_2.13/1.1/breeze_2.13-1.1.jar")
//    spark.sparkContext.addJar("https://repo1.maven.org/maven2/org/scalanlp/breeze-natives_2.13/1.1/breeze-natives_2.13-1.1.jar")
//    spark.sparkContext.addJar("https://repo1.maven.org/maven2/org/scalanlp/breeze-viz_2.13/1.1/breeze-viz_2.13-1.1.jar")
//    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
//    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
//    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
//    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
//    spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
//    spark.sparkContext.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
//    spark.sparkContext.hadoopConfiguration.set("fs.s3.awsAccessKeyId", accessKey)
//    spark.sparkContext.hadoopConfiguration.set("fs.s3.awsSecretAccessKey", secretKey)
//
//    println("Start")
//    Q1(spark)
//    Q8_1(spark)
//    Q8_2(spark)
//
//  }

  def df(spark: SparkSession, econpath: String, casepath: String): DataFrame = {
    val regionDF = spark.read.json("s3a://adam-king-848/data/regionDict.json")
    //val regionDF = spark.read.json("regionDict")

    val econRawDF =
      spark.read.option("delimiter", "\t").option("header", true).csv(econpath)
    val caseRawDF = spark.read
      .option("delimiter", "\t")
      .option("header", true)
      .csv(casepath)
    val caseRegionDF = DataFrameManipulator.caseJoin(spark, regionDF, caseRawDF)
    val econRegionDF = DataFrameManipulator.econJoin(spark, regionDF, econRawDF)
    val fullDF =
      DataFrameManipulator.joinCaseEcon(spark, caseRegionDF, econRegionDF)
    fullDF
  }

  def Q1(
      spark: SparkSession,
      fullDF: DataFrame = df(spark, econpath, casepath)
  ): Unit = {
    Question1.initialSolution(
      spark,
      fullDF,
      resultpath = "s3a://adam-king-848/results/blue/"
    )
    //    Question1.initialSolution(spark, fullDF, resultpath = "results")

  }

  def Q8_1(
      spark: SparkSession,
      fullDF: DataFrame = df(spark, econpath, casepath)
  ): Unit = {
    Question8.regionCorrelation(spark, fullDF)
  }

  def Q8_2(
      spark: SparkSession,
      fullDF: DataFrame = df(spark, econpath, casepath)
  ): Unit = {
    Question8.regionFirstPeak(
      spark,
      fullDF,
      "s3a://adam-king-848/results/blue/"
    )
    //    Question8.regionFirstPeak(spark, fullDF, "results")
  }

}
//EOF: Bluerunner.scala

object DataFrameManipulator {
  def caseJoin(
      spark: SparkSession,
      regionDF: DataFrame,
      caseDF: DataFrame
  ): DataFrame = {
    import spark.implicits._

    val regionDict = regionDF
      .select($"name", explode($"countries") as "country2")
//      .select($"name", $"agg_population", $"country")

    caseDF
      .select(
        $"date",
        $"country",
        $"total_cases",
        $"total_cases_per_million",
        $"new_cases",
        $"new_cases_per_million"
      )
      .join(regionDict, $"country" === $"country2")
//      .where($"date" =!= null)
      .drop($"country2")
      .withColumn(
        "new_cases",
        when($"new_cases" === "NULL", 0).otherwise($"new_cases")
      )
      .withColumn(
        "total_cases",
        when($"total_cases" === "NULL", 0).otherwise($"total_cases")
      )
      .withColumn(
        "new_cases_per_million",
        when($"new_cases_per_million" === "NULL", 0)
          .otherwise($"new_cases_per_million")
      )
      .withColumn(
        "total_cases_per_million",
        when($"total_cases_per_million" === "NULL", 0)
          .otherwise($"total_cases_per_million")
      )
      .filter($"date" =!= "null")
      .sort($"date" desc_nulls_first)
  }
  def econJoin(
      spark: SparkSession,
      regionDF: DataFrame,
      econDF: DataFrame
  ): DataFrame = {
    import spark.implicits._
    val regionDict = regionDF
      .select($"name", explode($"countries") as "country")
      .select($"name" as "region", $"country" as "country2")
    econDF
      .join(regionDict, $"name" === $"country2")
      .select(
        $"year",
        $"region",
        $"name" as "country",
        $"gdp_currentPrices" as "current_prices_gdp",
        $"gdp_perCap_currentPrices" as "gdp_per_capita"
      )
      .drop($"country2")
  }
  def joinCaseEcon(
      spark: SparkSession,
      caseDF: DataFrame,
      econDF: DataFrame
  ): DataFrame = {
    import spark.implicits._
    econDF.createOrReplaceTempView("econDFTemp")
    caseDF.createOrReplaceTempView("caseDFTemp")
    val caseEconDF = spark.sql(
      "SELECT e.year, e.region, c.country, e.current_prices_gdp, e.gdp_per_capita, c.total_cases, c.new_cases, c.new_cases_per_million, c.total_cases_per_million,c.date " +
        " FROM econDFTemp e JOIN caseDFTemp c " +
        "ON e.country == c.country " +
        "ORDER BY region, gdp_per_capita"
    )
    caseEconDF
  }
}
// EOF: DataFrameManipulator.scala

object Question1 {
  def initialSolution(
      spark: SparkSession,
      origdata: DataFrame,
      resultpath: String
  ): Unit = {
    import spark.implicits._
    val pop_col = origdata
      .withColumn("population", $"total_cases" / $"total_cases_per_million")
      .groupBy("region", "country")
      .agg(max($"population") as "population")
      .groupBy("region")
      .agg(sum($"population") as "population")
    val data = origdata.join(pop_col, "region")
    println("")
    println("Average New Cases per Day in Each Region")
    RankRegions.rankByMetricLow(spark, data, "new_cases", "avg").show()
    println("")
    println(
      "Average New Cases per Million People per Day in Each Region (normalized before region grouping)"
    )
    RankRegions
      .rankByMetricLow(spark, data, "new_cases_per_million", "avg")
      .show()
    println("")
    println("Average New Cases per Million People per Day in Each Region")
    RankRegions.rankByMetricLow(spark, data, "new_cases", "pop").show()
    println("")
    println("Total Cases in Each Region")
    RankRegions.rankByMetricLow(spark, data, "total_cases", "max").show()
    println("")
    println(
      "Total Cases per Million People in Each Region (normalized before region grouping)"
    )
    RankRegions
      .rankByMetricLow(spark, data, "total_cases_per_million", "max")
      .show()
    println("")
    println("Total Cases per Million People in Each Region")
    RankRegions.rankByMetricLow(spark, data, "total_cases", "maxpop").show()

    //    RankRegions.plotMetrics(spark, data, "new_cases", true, s"${resultpath}/plot_infection_rate_per_million")
    //    RankRegions.plotMetrics(spark, data, "new_cases", false,s"${resultpath}/plot_infection_rate")
    //    RankRegions.plotMetrics(spark, data, "total_cases", false, s"${resultpath}/plot_total_infections")
    //    RankRegions.plotMetrics(spark, data, "total_cases", true, s"${resultpath}/plot_total_infections_per_million")

    println("")
    println("Average GDP Percent Change in Each Region")
    RankRegions.changeGDP(spark, data, "current_prices_gdp", false).show()
    println("")
    println("Average GDP per Capita Percent Change in Each Region")
    RankRegions.changeGDP(spark, data, "gdp_per_capita", false).show()
  }
}
//EOF: Question1.scala

object Question8 {
  //TODO change this to GDP vs Value of First Infection Rate Spike
  def regionCorrelation(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._
    val now = Calendar.getInstance()
    val time = now.getTimeInMillis()
    val tableName = s"dfOptimize$time"
    df.write
      .mode("overwrite")
      .partitionBy("region")
      .bucketBy(40, "country")
      .saveAsTable(tableName)

    val regionNames = spark
      .sql(s"SELECT DISTINCT region FROM $tableName ORDER BY region")
      .rdd
      .map(_.get(0).toString)
      .collect()

    //val regionNames = df.select("region").sort("region").distinct().rdd.map(_.get(0).toString).collect()

    for (region <- (0 to regionNames.length - 1)) {
      var gdp = ArrayBuffer[Double]()
      var peak = ArrayBuffer[Double]()
      val specificRegion = regionNames(region)

      val regionCountries = spark
        .sql(
          s"SELECT DISTINCT country, region FROM $tableName WHERE region = '$specificRegion' "
        )
        .rdd
        .map(_.get(0).toString)
        .collect()
      //       val regionCountries = df.select("country").filter($"region" === regionNames(region)).distinct().rdd.map(_.get(0).toString).collect()
      // Get the first peak for each country in region and gdp
      for (country <- (0 to regionCountries.length - 1)) {
        val regionCountry = regionCountries(country)
        val countryDF = spark
          .sql(
            s"SELECT DISTINCT date, new_cases_per_million, gdp_per_capita FROM $tableName WHERE country = '$regionCountry'" +
              s" AND date != 'NULL' " +
              s" AND year = '2020'" +
              s" AND gdp_per_capita != 'NULL'" +
              s" ORDER BY date"
          )
          .cache()
        //        val countryDF = df.select($"date",$"gdp_per_capita",$"new_cases_per_million")
        //        .where($"country" === regionCountries(country))
        //        .filter($"date" =!= "NULL" && $"year" === "2020" && $"gdp_per_capita" =!= "NULL").sort("date").distinct()

        val tempCases = countryDF
          .select($"new_cases_per_million")
          .collect()
          .map(_.get(0).toString.toDouble)
        val tempDates = countryDF
          .select($"date")
          .collect()
          .map(_.get(0).toString)
          .map(DateFunc.dayInYear(_).toDouble)
        if (tempDates.length > 0 && tempCases.length > 0) {
          peak += (StatFunc.firstMajorPeak(tempDates, tempCases, 7, 10, 5)._2)
          val tempGDP = countryDF.select($"gdp_per_capita")
          val avgGDP = tempGDP
            .select(avg($"gdp_per_capita"))
            .collect()
            .map(_.get(0).toString.toDouble)
          gdp += avgGDP(0)
        }
      }
      // Give correlation for each region
      println(
        s"Region ${regionNames(region)}'s GDP - First Major Peak New Cases Value Correlation: ${StatFunc
          .correlation(gdp.toArray, peak.toArray)}"
      )
    }
    //spark.sql(s"DROP TABLE IF EXISTS $tableName")
    //Graph here
    //GraphFunc.graphSeries(regionGDP.toArray,regionPeaks.toArray, style='-', name = regionNames, legend=true)

    spark.sql(s"DROP TABLE IF EXISTS $tableName")
  }

  def regionFirstPeak(
      spark: SparkSession,
      df: DataFrame,
      resultpath: String
  ): Unit = {
    import spark.implicits._
    val now = Calendar.getInstance()
    val time = now.getTimeInMillis()
    val tableName = s"dfOptimize$time"
    df.write
      .partitionBy("region")
      .bucketBy(40, "country")
      .saveAsTable(tableName)
    val regionList = spark
      .sql(s"SELECT DISTINCT region FROM $tableName ORDER BY region")
      .rdd
      .map(_.get(0).toString)
      .collect()
    var tempDates: Array[Double] = null
    var tempCases: Array[Double] = null
    var tempFrame: DataFrame = null
    val firstPeakTimeAvg: ArrayBuffer[Double] = ArrayBuffer()
    val firstPeakForCountry: ArrayBuffer[Double] = ArrayBuffer()
    var countryList: Array[String] = Array()
    var peakTime: Double = 0
    for (region <- regionList) {
      countryList = df
        .select($"country")
        .where($"region" === region)
        .distinct()
        .collect()
        .map(_.get(0).asInstanceOf[String])
      for (country <- countryList) {
        tempFrame = spark
          .sql(
            s"SELECT DISTINCT country, date, new_cases FROM $tableName WHERE country = '$country' AND date != 'NULL' "
          )
          .sort($"date")
          .cache()
        tempCases = tempFrame
          .select($"new_cases")
          .collect()
          .map(_.get(0).toString.toDouble)
        tempDates = tempFrame
          .select($"date")
          .collect()
          .map(_.get(0).toString)
          .map(DateFunc.dayInYear(_).toDouble)
        peakTime = StatFunc.firstMajorPeak(tempDates, tempCases, 7, 10, 5)._1
        if (peakTime != -1) {
          firstPeakForCountry.append(peakTime)
          //          println(s"${country}, ${firstPeakForCountry.last}")
        }
      }
      firstPeakTimeAvg.append(
        firstPeakForCountry.sum / firstPeakForCountry.length
      )
      println(
        s"${region} Average Days to First Major Peak: ${firstPeakTimeAvg.last}"
      )
      firstPeakForCountry.clear()
    }

    //          val firstPeakTable: ArrayBuffer[(String, Double)] = ArrayBuffer()
    //          for (ii <- 0 to regionList.length-1){
    //            firstPeakTable.append((regionList(ii), firstPeakTimeAvg(ii)))
    //          }
    //          println("")
    //          for (ii <- 0 to regionList.length-1){
    //            println(s"${firstPeakTable(ii)._1}, ${firstPeakTable(ii)._2}}" )
    //          }
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
  }

}
//EOF: Question8.scala

object RankRegions {
  def rankByMetric(
      spark: SparkSession,
      fullDS: DataFrame,
      metric: String,
      op: String = "avg"
  ): DataFrame = {
    import spark.implicits._
    var oneTimeMetric: DataFrame = spark.emptyDataFrame
    op match {
      case "avg" => {
        oneTimeMetric = fullDS
          .groupBy("region")
          .agg(functions.avg(s"$metric") as s"$metric")
          .sort(functions.col(s"$metric") desc)
      }
      case "latest" => {
        val latestDate =
          fullDS.select(functions.max("date")).collect().map(_.getString(0))
        oneTimeMetric = fullDS
          .select("region", metric)
          .where($"date" === latestDate(0))
          .sort(functions.col(s"$metric") desc)
      }
      case "max" => {
        oneTimeMetric = fullDS
          .groupBy("region")
          .agg(functions.max(s"$metric") as s"$metric")
          .sort(functions.col(s"$metric") desc)
      }
      case "sum" => {
        oneTimeMetric = fullDS
          .groupBy("region")
          .agg(functions.max(s"$metric") as s"$metric")
          .sort(functions.col(s"$metric") desc)
      }
      case _ => {
        oneTimeMetric = fullDS
          .groupBy("region")
          .agg(functions.avg(s"$metric") as s"$metric")
          .sort(functions.col(s"$metric") desc)
      }
    }
    oneTimeMetric
  }
  def rankByMetricLow(
      spark: SparkSession,
      fullDS: DataFrame,
      metric: String,
      op: String = "avg"
  ): DataFrame = {
    import spark.implicits._
    var oneTimeMetric: DataFrame = spark.emptyDataFrame
    op match {
      case "avg" => {
        oneTimeMetric = fullDS
          .select(
            $"region",
            $"date",
            $"country",
            functions.round(functions.col(metric)) as metric
          )
          .distinct()
          .where($"$metric".isNotNull)
          .where($"$metric" =!= 0)
          .groupBy("region", "country")
          .agg(functions.avg(s"$metric") as s"$metric")
          .groupBy("region")
          .agg(functions.sum(s"$metric") as s"$metric")
          .sort(functions.col(s"$metric"))
//        oneTimeMetric.show()
      }
      case "max" => {
        oneTimeMetric = fullDS
          .select($"region", $"date", $"country", functions.col(metric))
          .distinct()
          .groupBy("region", "country")
          .agg(functions.max(s"$metric") as metric)
          .groupBy("region")
          .agg(functions.sum(s"$metric") as s"$metric")
          .sort(functions.col(metric))
      }
      case "pop" => {
        fullDS.select($"region", $"population").distinct().show()
        oneTimeMetric = fullDS
          .select(
            $"region",
            $"date",
            $"country",
            $"population",
            functions.round(functions.col(metric)) as metric
          )
          .distinct()
          .where($"$metric".isNotNull)
          .where($"$metric" =!= 0)
          .groupBy($"region", $"country", $"population")
          .agg((functions.avg(s"$metric")) as s"${metric}")
        oneTimeMetric = oneTimeMetric
          .groupBy($"region", $"population")
          .agg(
            functions.sum(
              s"${metric}"
            ) / $"population" as s"${metric}_per_million"
          )
          .drop("population")
          .sort(functions.col(s"${metric}_per_million"))
      }
      case "maxpop" => {
        oneTimeMetric = fullDS
          .select(
            $"region",
            $"date",
            $"country",
            $"population",
            functions.round(functions.col(metric)) as metric
          )
          .distinct()
          .where($"$metric".isNotNull)
          .where($"$metric" =!= 0)
          .groupBy($"region", $"country", $"population")
          .agg((functions.max(s"$metric")) as s"${metric}")
          .groupBy($"region", $"population")
          .agg(
            functions.sum(
              s"${metric}"
            ) / $"population" as s"${metric}_per_million"
          )
          .drop("population")
          .sort(functions.col(s"${metric}_per_million"))
      }
    }
    oneTimeMetric
  }
  def calculateMetric(
      spark: SparkSession,
      fullDS: DataFrame,
      metric: String,
      normalizer: String,
      numNormalizer: Double,
      newName: String
  ): DataFrame = {
    var importantData = spark.emptyDataFrame
    if (normalizer != "none") {
      importantData = fullDS
        .select("region", "date", s"$metric", s"$normalizer")
        .groupBy("region", "date")
        .agg(
          functions.round(
            functions.sum(
              functions.col(s"$metric") / (functions
                .col(s"$normalizer") / numNormalizer)
            ),
            2
          ) as newName
        )
        .sort(newName)
    } else {
      importantData = fullDS
        .select("region", "date", s"$metric")
        .groupBy("region", "date")
        .agg(
          functions.round(
            functions.sum(functions.col(s"$metric") / numNormalizer),
            2
          ) as newName
        )
        .sort(newName)
    }
    importantData
  }

  def plotMetrics(
      spark: SparkSession,
      data: DataFrame,
      metric: String,
      pop: Boolean,
      filename: String
  ): Unit = {
    import spark.implicits._

    val regionList =
      data.select("region").distinct().collect().map(_.getString(0))
    val datePlottable: ArrayBuffer[Array[Double]] = ArrayBuffer()
    val metricPlottable: ArrayBuffer[Array[Double]] = ArrayBuffer()
    val dataGrouped = if (pop) {
      data
        .where($"date" < "2020-11-20")
        .select($"region", $"population", $"date", $"$metric")
        .groupBy($"region", $"date", $"population")
        .agg(functions.sum($"$metric") / $"population" as metric)
        .drop($"population")
        .cache()
    } else {
      data
        .where($"date" < "2020-11-20")
        .select($"region", $"date", $"$metric")
        .groupBy($"region", $"date")
        .agg(functions.sum($"$metric") as metric)
        .cache()
    }
    for (region <- regionList) {
      metricPlottable.append(
        dataGrouped
          .select(metric)
          .where($"region" === region)
          .sort($"date")
          .rdd
          .collect
          .map(_.get(0).asInstanceOf[Double])
      )
      datePlottable.append(
        dataGrouped
          .select("date")
          .where($"region" === region)
          .sort($"date")
          .rdd
          .collect()
          .map(date =>
            DateFunc.dayInYear(date(0).asInstanceOf[String]).toDouble
          )
      )
    }
    val f = Figure()
    val p = f.subplot(0)
    for (ii <- 0 to regionList.length - 1) {
      p += plot(
        DenseVector(datePlottable(ii)),
        DenseVector(metricPlottable(ii)),
        name = regionList(ii)
      )
    }
    p.legend = true
    p.xlabel = "Days since 1st of January, 2020"
    p.ylabel = if (pop) {
      s"${metric}_per_million"
    } else {
      metric
    }
//    f.refresh()
    f.saveas(s"${filename}.png")

  }

  def changeGDP(
      spark: SparkSession,
      fullDS: DataFrame,
      metric: String,
      percapita: Boolean
  ): DataFrame = {
    import spark.implicits._

//    val temp = fullDS
//      .withColumn("delta_gdp", (($"2020_GDP"-$"2019_GDP")/$"2019_GDP")*100)
//    val gdp_tot = fullDS
//      .select($"country", $"region", $"current_prices_gdp", $"year")
    val gdp_temp = fullDS
      .select(
        $"country",
        $"region",
        $"population",
        $"$metric" as "gdp",
        $"year"
      )

    val gdp_2020 = gdp_temp
      .where($"year" === "2020")
      .where($"gdp" =!= "NULL")
      .drop("year")
      .groupBy($"region", $"population")
      .agg(functions.sum($"gdp") as "gdp_20")
    val gdp_2019 = gdp_temp
      .where($"year" === "2019")
      .where($"gdp" =!= "NULL")
      .drop("year")
      .groupBy($"region", $"population")
      .agg(functions.sum($"gdp") as "gdp_19")
    val gdp = gdp_2019
      .join(gdp_2020, "region")
      .withColumn("delta_gdp", (($"gdp_20" - $"gdp_19") / $"gdp_20") * 100)
      .drop("gdp_19", "gdp_20")
    rankByMetric(spark, gdp, "delta_gdp", "avg")
  }
}
//EOF: RankRegions.scala

object StatFunc {

  /** Gives the first peak that satisfies the inputs and allows for ignoring noise
    * @param xArray the independent data series
    * @param yArray the dependent data series
    * @param neighbors number of data points to evaluate after a potential peak
    * @param minDifference number to filter out noise where noise is difference between peak and avg value of neighbors
    * @param percentDifference percentage to filter out noise where noise is difference between peak and avg value of neighbors
    * @param minCasePercent floor to start evaluating values with respect to maximum value percentage
    * @return a first peak coordinate that satisfies conditions
    */
  def firstPeak(
      xArray: Array[Double],
      yArray: Array[Double],
      neighbors: Int,
      minDifference: Double
  ): (Double, Double) = {

    for (i <- (0 to (xArray.length - 1))) {
      if (i == 0 && yArray(i) > yArray(i + 1)) {
        var sum: Double = 0.0
        for (neighbor <- (1 to neighbors)) {
          sum += yArray(i + neighbor)
        }
        val avgSum = sum / neighbors
        if (yArray(i) - avgSum >= minDifference) {
          return (xArray(i), yArray(i))
        }
      } else if (i == xArray.length - 1 && yArray(i) > yArray(i - 1)) {
        var sum: Double = 0.0
        for (neighbor <- (1 to neighbors)) {
          sum += yArray(i - neighbor)
        }
        val avgSum = sum / neighbors
        if (yArray(i) - avgSum >= minDifference) {
          return (xArray(i), yArray(i))
        }
      } else if (
        i != 0 && i != xArray.length - 1 && yArray(i) > yArray(i - 1) && yArray(
          i
        ) > yArray(i + 1)
      ) {
        var sum: Double = 0.0
        for (neighbor <- (1 to neighbors)) {
          sum += yArray(i + neighbor)
        }
        val avgSum = sum / neighbors
        if (yArray(i) - avgSum >= minDifference) {
          return (xArray(i), yArray(i))
        }
      }
    }
    (xArray(0), yArray(0))
  }

  def firstMajorPeak(
      xArray: Array[Double],
      yArray: Array[Double],
      neighbors: Int,
      percentDifference: Double,
      minCasePercent: Double
  ): (Double, Double) = {
    var avgSum: Double = 0.0
    var sum: Double = 0.0
    var minDifference: Double = 0.0
    val minCase = minCasePercent * .01 * yArray.max
    val start = yArray.indexWhere(_ > minCase)
    if (start != -1) {
      for (i <- start to (xArray.length - neighbors - 1)) {
        sum = 0.0
        for (neighbor <- (1 to neighbors)) {
          sum += yArray(i + neighbor)
        }
        avgSum = sum / neighbors
        minDifference = .01 * percentDifference * yArray(i)
        if (yArray(i) - avgSum > minDifference) {
          return (xArray(i), yArray(i))
        }
      }
    }
    (-1, yArray(0))
  }

  /** The correlation between two series of data ~1 = positive correlation,
    * ~0 = no correlation, ~-1 = -negative correlation
    * @param xArray the independent data series
    * @param yArray the dependent data series
    * @return the correlation number as a double
    */
  def correlation(xArray: Array[Double], yArray: Array[Double]): Double = {
    var r = 0.0
    var x = 0.0
    var y = 0.0
    var x_2 = 0.0
    var y_2 = 0.0
    var xy = 0.0
    val n = xArray.length
    for (i <- (0 to (xArray.length - 1))) {
      x += xArray(i)
      y += yArray(i)
      x_2 += (xArray(i) * xArray(i))
      y_2 += (yArray(i) * yArray(i))
      xy += (xArray(i) * yArray(i))
    }
    r = (n * xy - (x * y)) / (sqrt(n * x_2 - (x * x)) * sqrt(n * y_2 - (y * y)))
    r
  }
}
//EOF: StatFunc.scala

object FileWriter {

  def writeDataFrameToFile(
      dataFrame: DataFrame,
      outputFilename: String,
      maxRecords: Int = 100
  ) = {
    dataFrame
      .limit(maxRecords)
      .write
      .format("csv")
      .save(s"s3a://adam-king-848/results/purple/$outputFilename")
  }
}
//EOF: Filewriter.scala

object HashtagsByRegion {

  def getHashtagsByRegion(spark: SparkSession, region: String = null): Unit = {
    //What are the hashtags used to describe COVID-19 by Region (e.g. #covid, #COVID-19, #Coronavirus, #NovelCoronavirus)?
    val staticDf = spark.read.json("s3a://adam-king-848/data/twitter_data.json")
    question1(spark, staticDf, region)
  }

  def getHashtagsByRegionAll(spark: SparkSession): Unit = {
    //What are the hashtags used to describe COVID-19 by Region (e.g. #covid, #COVID-19, #Coronavirus, #NovelCoronavirus)?
    val staticDf = spark.read.json("s3a://adam-king-848/data/twitter_data.json")
    question1all(spark, staticDf)
  }

  private def question1(
      spark: SparkSession,
      df: DataFrame,
      region: String
  ): Unit = {
    import spark.implicits._
    val startTime = System.currentTimeMillis()
    var outputFilename: String = null

    val newdf = df
      .filter(!functions.isnull($"place"))
      .select($"entities.hashtags.text", $"place.country")
      //map to Row(List(Hashtags),Region)
      .map(tweet => {
        (
          tweet.getList[String](0).toList.map(_.toLowerCase()),
          RegionDictionary.reverseMapSearch(tweet.getString(1))
        )
      })
      .withColumnRenamed("_1", "Hashtags")
      .withColumnRenamed("_2", "Region")

    // If we are passed a region, we need to filter by it
    // Otherwise we present all of the information
    if (region != null) {
      val sorteddf = newdf
        .filter($"region" === region)
        //explode List(Hashtags),Region to own rows resulting in Row(Hashtag,Region)
        .select(functions.explode($"Hashtags").as("Hashtag"), $"Region")
        //group by the same Region and Hashtag
        .groupBy("Region", "Hashtag")
        //count total of each Region/Hashtag appearance
        .count()
        .orderBy(functions.desc("Count"))

      outputFilename = s"hbr-${region.replaceAll("\\s+", "")}-$startTime"
      writeDataFrameToFile(sorteddf, outputFilename)
    } else {
      val sorteddf = newdf
        //explode List(Hashtags),Region to own rows resulting in Row(Hashtag,Region)
        .select(functions.explode($"Hashtags").as("Hashtag"), $"Region")
        //group by the same Region and Hashtag
        .groupBy("Region", "Hashtag")
        //count total of each Region/Hashtag appearance
        .count()
        .orderBy(functions.desc("Count"))

      outputFilename = s"hbr-AllRegions-$startTime"
      writeDataFrameToFile(sorteddf, outputFilename)
    }
  }

  private def question1all(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._
    val startTime = System.currentTimeMillis()
    var outputFilename: String = null

    val newdf = df
      .filter(!functions.isnull($"place"))
      .select($"entities.hashtags.text", $"place.country")
      //map to Row(List(Hashtags),Region)
      .map(tweet => {
        (
          tweet.getList[String](0).toList.map(_.toLowerCase()),
          RegionDictionary.reverseMapSearch(tweet.getString(1))
        )
      })
      .withColumnRenamed("_1", "Hashtags")
      .withColumnRenamed("_2", "Region")
      //explode List(Hashtags),Region to own rows resulting in Row(Hashtag,Region)
      .select(functions.explode($"Hashtags").as("Hashtag"), $"Region")
      //group by the same Region and Hashtag
      .groupBy("Region", "Hashtag")
      //count total of each Region/Hashtag appearance
      .count()

    val greatdf = newdf
      .orderBy(functions.desc("Count"))

    outputFilename = s"hbr-all-$startTime"
    writeDataFrameToFile(greatdf, outputFilename)

    RegionDictionary.getRegionList.foreach(region => {
      val bestdf = newdf
        .filter($"Region" === region)
        .orderBy(functions.desc("Count"))

      outputFilename = s"hbr-${region.replaceAll("\\s+", "")}-$startTime"
      writeDataFrameToFile(bestdf, outputFilename)
    })
  }
}
//EOF: HashtagsByRegion.scala

object RegionDictionary {
  private val regionMap = Map(
    "Africa" ->
      List(
        "Algeria",
        "Angola",
        "Benin",
        "Botswana",
        "Burkina Faso",
        "Burundi",
        "Cameroon",
        "Cape Verde",
        "Central African Republic",
        "Chad",
        "Comoros",
        "Cote d'Ivoire",
        "Democratic Republic of the Congo",
        "Djibouti",
        "Egypt",
        "Equatorial Guinea",
        "Eritrea",
        "Ethiopia",
        "Gabon",
        "Gambia",
        "Ghana",
        "Guinea",
        "Guinea-Bissau",
        "Kenya",
        "Lesotho",
        "Liberia",
        "Libya",
        "Madagascar",
        "Malawi",
        "Mali",
        "Mauritania",
        "Mauritius",
        "Morocco",
        "Mozambique",
        "Namibia",
        "Niger",
        "Nigeria",
        "Republic of the Congo",
        "Reunion",
        "Rwanda",
        "Saint Helena",
        "Sao Tome and Principe",
        "Senegal",
        "Seychelles",
        "Sierra Leone",
        "Somalia",
        "South Africa",
        "South Sudan",
        "Sudan",
        "Swaziland",
        "Tanzania",
        "Togo",
        "Tunisia",
        "Uganda",
        "Western Sahara",
        "Zambia",
        "Zimbabwe"
      ),
    "Asia" ->
      List(
        "Afghanistan",
        "Armenia",
        "Azerbaijan",
        "Bahrain",
        "Bangladesh",
        "Bhutan",
        "Brunei",
        "Burma",
        "Cambodia",
        "China",
        "Cyprus",
        "East Timor",
        "Georgia",
        "Hong Kong",
        "India",
        "Indonesia",
        "Iran",
        "Iraq",
        "Israel",
        "Japan",
        "Jordan",
        "Kazakhstan",
        "Kuwait",
        "Kyrgyzstan",
        "Laos",
        "Lebanon",
        "Macau",
        "Malaysia",
        "Maldives",
        "Mongolia",
        "Nepal",
        "North Korea",
        "Oman",
        "Pakistan",
        "Philippines",
        "Qatar",
        "Saudi Arabia",
        "Singapore",
        "South Korea",
        "Sri Lanka",
        "Syria",
        "Taiwan",
        "Tajikistan",
        "Thailand",
        "Turkey",
        "Turkmenistan",
        "United Arab Emirates",
        "Uzbekistan",
        "Vietnam",
        "Yemen"
      ),
    "Caribbean" ->
      List(
        "Anguilla",
        "Antigua and Barbuda",
        "Aruba",
        "The Bahamas",
        "Barbados",
        "Bermuda",
        "British Virgin Islands",
        "Cayman Islands",
        "Cuba",
        "Dominica",
        "Dominican Republic",
        "Grenada",
        "Guadeloupe",
        "Haiti",
        "Jamaica",
        "Martinique",
        "Montserrat",
        "Netherlands Antilles",
        "Puerto Rico",
        "Saint Kitts and Nevis",
        "Saint Lucia",
        "Saint Vincent and the Grenadines",
        "Trinidad and Tobago",
        "Turks and Caicos Islands",
        "U.S. Virgin Islands"
      ),
    "Central America" ->
      List(
        "Belize",
        "Costa Rica",
        "El Salvador",
        "Guatemala",
        "Honduras",
        "Nicaragua",
        "Panama"
      ),
    "Europe" ->
      List(
        "Albania",
        "Andorra",
        "Austria",
        "Belarus",
        "Belgium",
        "Bosnia and Herzegovina",
        "Bulgaria",
        "Croatia",
        "Czech Republic",
        "Denmark",
        "Estonia",
        "Finland",
        "France",
        "Germany",
        "Gibraltar",
        "Greece",
        "Holy See",
        "Hungary",
        "Iceland",
        "Ireland",
        "Italy",
        "Kosovo",
        "Latvia",
        "Liechtenstein",
        "Lithuania",
        "Luxembourg",
        "Macedonia",
        "Malta",
        "Moldova",
        "Monaco",
        "Montenegro",
        "Netherlands",
        "Norway",
        "Poland",
        "Portugal",
        "Romania",
        "Russia",
        "San Marino",
        "Slovak Republic",
        "Slovenia",
        "Spain",
        "Serbia",
        "Serbia and Montenegro",
        "Sweden",
        "Switzerland",
        "Ukraine",
        "United Kingdom"
      ),
    "North America" ->
      List(
        "Canada",
        "Greenland",
        "Mexico",
        "Saint Pierre and Miquelon",
        "United States"
      ),
    "Oceania" ->
      List(
        "American Samoa",
        "Australia",
        "Christmas Island",
        "Cocos (Keeling) Islands",
        "Cook Islands",
        "Federated States of Micronesia",
        "Fiji",
        "French Polynesia",
        "Guam",
        "Kiribati",
        "Marshall Islands",
        "Nauru",
        "New Caledonia",
        "New Zealand",
        "Niue",
        "Northern Mariana Islands",
        "Palau",
        "Papua New Guinea",
        "Pitcairn Islands",
        "Samoa",
        "Solomon Islands",
        "Tokelau",
        "Tonga",
        "Tuvalu",
        "Vanuatu",
        "Wallis and Futuna Islands"
      ),
    "South America" ->
      List(
        "Argentina",
        "Bolivia",
        "Brazil",
        "Chile",
        "Colombia",
        "Ecuador",
        "Falkland Islands",
        "French Guiana",
        "Guyana",
        "Paraguay",
        "Peru",
        "Suriname",
        "Uruguay",
        "Venezuela"
      )
  )

  def reverseMapSearch(country: String): String = {
    regionMap.foreach(region =>
      if (region._2.contains(country)) { return region._1 }
    )
    "Country Not Found"
  }

  def getRegionList: List[String] = {
    regionMap.keys.toList
  }
}
//EOF: RegionDictionary.scala

object HashtagsWithCovid {

  def getHashtagsWithCovid(spark: SparkSession): Unit = {
    //What are the top 10 commonly used hashtags used alongside COVID hashtags?
    val staticDf = spark.read.json("s3a://adam-king-848/data/twitter_data.json")
    question2(spark, staticDf)
  }

  private def question2(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._
    val startTime = System.currentTimeMillis()
    val covidRelatedWordsList = CovidTermsList.getTermsList
    val newDf = df
      .select($"entities.hashtags.text")
      //map to Row(List(Hashtags))
      .map(tweet => {
        tweet.getList[String](0).toList.map(_.toLowerCase())
      })
      .withColumnRenamed("value", "Hashtags")
      //filter to only lists with greater than 1 hashtags (2+)
      .filter(functions.size($"Hashtags").gt(1))
      //filter to only lists that contain a word from our filter list
      .filter(hashtags => {
        val hashtagList = hashtags.getList[String](0)
        //this filter statement seems inefficient
        hashtagList.exists(hashtag =>
          covidRelatedWordsList.exists(covidHashtag =>
            hashtag.contains(covidHashtag.toLowerCase())
          )
        )
      })
      //explode out all remaining List(Hashtags)
      .select(functions.explode($"Hashtags").as("Hashtag"))
      //remove all items that are on the our filter list
      .filter(hashtag => {
        val hashtagStr = hashtag.getString(0)
        !covidRelatedWordsList.exists(covidHashtag =>
          hashtagStr.toLowerCase().contains(covidHashtag.toLowerCase())
        )
      })
      .groupBy($"Hashtag")
      .count()
      .orderBy(functions.desc("Count"))

    val outputFilename: String = s"hwc-full-$startTime"
    writeDataFrameToFile(newDf, outputFilename)
  }
}
//EOF: HashtagsWithCovid.scala

object Runner {
  def main(args: Array[String]): Unit = {
    // Command Line Interface format that accepts params
    // Configs that William so kindly provided us with.
    if (args.length <= 2) {
      System.err.println(
        "EXPECTED 3 ARGUMENTS: AWS Access Key, then AWS Secret Key, then S3 Bucket"
      )
      System.exit(1)
//      case Array(func,param1) if(func == "Q4") => Question4.PrintQuestion4(spark)
//      case Array(func,param1) if(func == "Q5") => Question5.getMostDiscussion(spark)
    }
    val accessKey = args.apply(0)
    val secretKey = args.apply(1)
    val filePath = args.apply(2)
    // The following is required in order for this to work properly

    // Path of jar files should be edited 
    spark.sparkContext.addJar(
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.4/hadoop-aws-2.7.4.jar"
    )
    spark.sparkContext.addJar(
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar"
    )
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3.awsAccessKeyId", accessKey)
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3.awsSecretAccessKey", secretKey)
    // Thank you William
    println("Welcome to our Covid analysis. Here are the menu options:\n")
    userOption()
    // End of main()
  }

  // Declaring spark session at global scope
  val spark = SparkSession.builder().appName("Nahshon-test").getOrCreate()
  //loop condition for while loop manipulation.
  var loopagain = true
  def userOption(): Unit = {
    Menu.printMenu()
    println("\nPlease enter an option: ")
    // Timeout for the entire program. Set to ~20 minutes for presentation.
    // I used 30 sec or 5 min for testing.
    var timeout = 5.minutes.fromNow
    while (timeout.hasTimeLeft() || loopagain) {
      // Scanner for user input
      val uiScanner = new Scanner(System.in)
      // This is what we do only when time has run out.
      if (timeout.hasTimeLeft() == false) {
        println("\n[**SESSION TIMEOUT**]")
        // The exit statement ends the entire program. This point means timeout has
        // hit deadline without an exit statement, so we set the program to exit,
        // then ask the user if they would like to continue.
        loopagain = false
        // We establish a future thread to wait for users to choose to continue.
        val continueOption: Future[Unit] = Future {
          StdIn.readLine(
            "The program will close in 1 minute. Would you like to continue?" +
              "\nType yes or no: "
          ) match {
            case input if input.equalsIgnoreCase("yes") => {
              println("\nOkay, returning to main menu...\n")
              loopagain = true
              timeout = 5.minutes.fromNow
              Menu.printMenu()
              println("\nPlease enter an option: ")
            }
            case input if input.equalsIgnoreCase("no") => {
              println("\nThank you for your time!! Goodbye\n")
            }
            case _ => {
              println("\nWe'll take that as a no. Exiting program...")
            }
          }
        }
        // We call the above defined future here, but wait only 1 minute before the
        // final timeout and close program. If the future continues the program (by
        // setting loop again equal to true), then we set the timeout for 5 more
        // minutes, and head back up to the while loop.
        try {
          Await.ready(continueOption, 1.minute)
        } catch {
          case clockout: TimeoutException => println("\nShutting down...")
        }
      } else if (System.in.available() > 0) {
        // This is the code that executes while timeout still has time.
        uiScanner.next() match {
          case question if question.equalsIgnoreCase("Q1") => {
            println(
              "\nWhich Regions handled COVID-19 the best assuming our metrics" +
                "\nare change in GDP by percentage and COVID-19 infection " +
                "\nrate per capita. (Jan 1 2020 - Oct 31 2020)\n"
            )
            println("running code...")
            //        blue.BlueRunner.Q1(spark)
            println("Returning to main menu...")
            Menu.printMenu()
            println("\nPlease enter an option: ")
          }
          case question if question.equalsIgnoreCase("Q2") => {
            println(
              "\nFind the top 5 pairs of countries that share a land border and have the" +
                "\nhighest discrepancy in covid-19 infection rate per capita. " +
                "\nAdditionally, find the top 5 landlocked countries that have the " +
                "\nhighest discrepancy in covid-19 infection rate per capita.\n"
            )
            println("running code...")
            //        color.Question2.runnerfunc(spark)
            println("Returning to main menu...")
            Menu.printMenu()
            println("\nPlease enter an option: ")
          }
          case question if question.equalsIgnoreCase("Q3") => {
            println(
              "\nLive update by Region of current relevant totals from COVID-19 data.\n"
            )
            println("running code...")
            //        color.Question3.runnerfunc(spark)
            println("Returning to main menu...")
            Menu.printMenu()
            println("\nPlease enter an option: ")
          }
          case question if question.equalsIgnoreCase("Q4") => {
            println(
              "\nIs the trend of the global COVID-19 discussion going up or down?" +
                "\nDo spikes in infection rates of the 5-30 age range affect the volume of discussion?\n"
            )
            println("running code...")
            //        color.Question4.runnerfunc(spark)
            println("Returning to main menu...")
            Menu.printMenu()
            println("\nPlease enter an option: ")
          }
          case question if question.equalsIgnoreCase("Q5") => {
            println("\nWhen was COVID-19 being discussed the most?\n")
            println("running code...")
            //        color.Question5.runnerfunc(spark)
            println("Returning to main menu...")
            Menu.printMenu()
            println("\nPlease enter an option: ")
          }
          case question if question.equalsIgnoreCase("Q6") => {
            println(
              "\nWhat percentage of countries have an increasing COVID-19 Infection rate?\n"
            )
            println("running code...")
            //        color.Question6.runnerfunc(spark)
            println("Returning to main menu...")
            Menu.printMenu()
            println("\nPlease enter an option: ")
          }
          case question if question.equalsIgnoreCase("Q7") => {
            println(
              "\nWhat are the hashtags used to describe COVID-19 by Region (e.g. #covid, " +
                "\n#COVID-19, #Coronavirus, #NovelCoronavirus)? Additionally, what are the top 10 " +
                "\ncommonly used hashtags used alongside COVID hashtags?\n"
            )
            println("running code...")
            //        color.Question7.runnerfunc(spark)
            println("Returning to main menu...")
            Menu.printMenu()
            println("\nPlease enter an option: ")
          }
          case question if question.equalsIgnoreCase("Q8") => {
            println(
              "\nIs there a significant relationship between a Region’s cumulative" +
                "\nGDP and Infection Rate per capita? What is the average amount of time" +
                "\nit took for each region to reach its first peak in infection rate per capita?\n"
            )
            println("running code...")
            //  blue.BlueRunner.Q8_1(spark)
            //  blue.BlueRunner.Q8_2(spark)
            println("Returning to main menu...")
            Menu.printMenu()
            println("\nPlease enter an option: ")
          }
          case leave if leave.equalsIgnoreCase("Exit") => {
            println("\nThank you for your time! Goodbye :)\n")
            timeout = timeout - timeout.timeLeft
            loopagain = false
            exit
          }
          case somethingelse => {
            println(
              s"\n'${somethingelse}' is not a recognized option. Maybe you should try again.\n"
            )
            Menu.printMenu()
            println("\nPlease enter an option: ")
          }
        }
      }
    }
  }
}
//EOF: Runner.scala
