/** Question8
  * Currently defunct, but meant to query:
  *     "first peak for each country in region and gdp"
  * using spark SQL on S3 buckets
  * 
  */
object Question8 {

  /** regionCorrelation
    * uses Spark SQL with S3 buckets partitioned by region to find out the peak case timetable
    *  for each region and correlates that metric to the GDP of said region
    *
    * @param spark - The spark session
    * @param df - the dataframe that will contain the data formatted in this function
    * 
    * TODO: change this to GDP vs Value of First Infection Rate Spike {TODO brought over from original file}
    */
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

    //regionNames is an array of strings which are the names of each region mapped from a Spark RDD
    val regionNames = spark
      .sql(s"SELECT DISTINCT region FROM $tableName ORDER BY region")
      .rdd
      .map(_.get(0).toString)
      .collect()

    //the commented code below is the same regionNames declaration above meant for dataframes
    //val regionNames = df.select("region")
    // .sort("region")
    // .distinct()
    // .rdd.map(_.get(0).toString)
    // .collect()

    // This for loop gets the GDP data for each region and country within those regions and outputs the data to
    // the console as a formatted string using the StatFunc class
    for (region <- (0 to regionNames.length - 1)) {
      var gdp = ArrayBuffer[Double]()
      var peak = ArrayBuffer[Double]()
      val specificRegion = regionNames(region)

      val regionCountries = spark
        .sql(s"SELECT DISTINCT country, region FROM $tableName WHERE region = '$specificRegion' ")
        .rdd
        .map(_.get(0).toString)
        .collect()

      // This code commented below does the same declaration as above, but takes in the dataframe
      // directly instead of querying Spark.
      // Luckily this allows us to choose whether or not to use spark SQL queries in our data pulls
      // val regionCountries = df.select("country")
      //    .filter($"region" === regionNames(region))
      //    .distinct()
      //    .rdd
      //    .map(_.get(0).toString)
      //    .collect()
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
        //val countryDF = df.select($"date",$"gdp_per_capita",$"new_cases_per_million")
        //  .where($"country" === regionCountries(country))
        //  .filter($"date" =!= "NULL" && $"year" === "2020" && $"gdp_per_capita" =!= "NULL")
        //  .sort("date")
        //  .distinct()

        //used in the if statement below, temporary metric arrays of doubles that gets the number of cases per
        // million as well as the dates after the pandemic started 
        val tempCases = countryDF
          .select($"new_cases_per_million")
          .collect()
          .map(_.get(0).toString.toDouble)
        val tempDates = countryDF
          .select($"date")
          .collect()
          .map(_.get(0).toString)
          .map(DateFunc.dayInYear(_).toDouble)

          //if statement adds average GDP data to the gdp metric array if the date and cases are higher than 0
          // 0 correlating to the start of the pandemic onwards in both date and number of cases
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
    // Table is dropped at the end of the function. I'm not sure why but it's likely because the table is not
    // needed outside of the analysis.
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
  }

/** regionFirstPeak
  * Calculates the first peak number of cases based on daily case numbers pulled from Spark
  * 
  *
  * @param spark - the spark session
  * @param df - the dataframe that the data will be loaded into
  * @param resultpath - the path that the result will be stored in {UNUSED - ?}
  * 
  * FIXME: refactor this function to either not include resultpath as a parameter, or find a use for it
  */
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
    var peakTime: Double = 
    
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
