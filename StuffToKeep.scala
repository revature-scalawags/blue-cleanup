object Runner {
  //spark session boilerplate
  val spark = SparkSession
    .builder()
    .master("local[4]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val econpath =
    "" //FIXME: path of the tsv/csv that contains the daily case stats
  val casepath =
    "" //FIXME: path of the tsv/csv that contains the daily economic data

  def main(args: Array[String]): Unit = {

    val key = System.getenv("AWS_ACCESS_KEY_ID")
    val secret = System.getenv("AWS_SECRET_ACCESS_KEY")
    val client = S3Client.buildS3Client(
      key,
      secret
    ) // Build the S3 client with access keys
    //FIXME: decide on which spark session builder to use

    val spark = SparkSession.builder().appName("sample").getOrCreate()

    spark.sparkContext.addJar(
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.4/hadoop-aws-2.7.4.jar"
    )
    spark.sparkContext.addJar(
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar"
    )
    spark.sparkContext.addJar(
      "https://repo1.maven.org/maven2/org/apache/spark/spark-sql_2.12/3.0.0/spark-sql_2.12-3.0.0.jar"
    )
    spark.sparkContext.addJar(
      "https://repo1.maven.org/maven2/org/scalanlp/breeze_2.13/1.1/breeze_2.13-1.1.jar"
    )
    spark.sparkContext.addJar(
      "https://repo1.maven.org/maven2/org/scalanlp/breeze-natives_2.13/1.1/breeze-natives_2.13-1.1.jar"
    )
    spark.sparkContext.addJar(
      "https://repo1.maven.org/maven2/org/scalanlp/breeze-viz_2.13/1.1/breeze-viz_2.13-1.1.jar"
    )
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret)
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3.awsAccessKeyId", key)
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3.awsSecretAccessKey", secret)
    //I don't know if we'll actually need these sparkContext builders but I kept them just in case

    println("Start")
    Q1(spark) //references package purple.Q1 (I think)

  }

  /** This is a function that creates a dataframe
    * based on the daily case and economy data
    *
    * @param spark - the spark session
    * @param econpath - path of the tsv/csv that contains the daily economic data
    * @param casepath - path of the tsv/csv that contains the daily case stats
    */
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
    //FIXME: seperate object or refactor as 3 functions within this file?
    fullDF
  }
}

/** Implemented in the runner under the df function, this object joins dataframes together
  * based on specific columns that are established in the input DF's
  */
object DataFrameManipulator {

  /** caseJoin
    * creates a dataframe for the daily case data separated by region
    * (a joiner on the region and case dataframes)
    *
    * @param spark - The spark session
    * @param regionDF - the dataframe that contains the region data
    * @param caseDF - the dataframe that contains the daily case data
    * @return a dataframe with "date", "country", "total_cases",
    *         "total_cases_per_million", "new_cases", "new_cases_per_million",
    *         "region" fields, sorted in descending order by date
    */
  def caseJoin(
      spark: SparkSession,
      regionDF: DataFrame,
      caseDF: DataFrame
  ): DataFrame = {
    import spark.implicits._

    val regionDict = regionDF
      .select($"name", explode($"countries") as "country2")

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
      .drop($"country2")
      //replace 'NULL' values in column with 0, otherwise keep value
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
      .filter($"date" =!= "null") // filter out null dates (inequality test)
      .sort($"date" desc_nulls_first) // sort in desc order (null values first)
  }

  /** econJoin
    * creates a dataframe for the daily economy data separated by region
    * (a joiner on the region and economy dataframes)
    *
    * @param spark - The spark session
    * @param regionDF - the dataframe that contains the region data
    * @param econDF - the dataframe that contains the daily economy data
    * @return a dataframe with "year", "region", "country", "current_prices_gdp",
    *         "gdp_per_capita" fields.
    */
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

  /** joinCaseEcon
    * joins the results of caseJoin and econJoin into a single dataframe
    *
    * @param spark - the spark session
    * @param caseDF - the resulting dataframe from caseJoin
    * @param econDF - the resulting dataframe from econJoin
    * @return a dataframe joined by 'country' and ordered by 'region' and
    *         'gdp_per_capita'
    */
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
