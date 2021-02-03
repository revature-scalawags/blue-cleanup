/** The RankRegions object uses methods to group and sort by some metric passed as an argument.
  */
object RankRegions {

  /** groups dataFrame by region and performs the given operation 'op'
    * on the given field/metric 'metric'
    * example: rankByMetric(spark, df, "new_cases", "avg")
    *          will return a dataFrame with the avg # of new cases for each region
    *
    * @param spark spark session
    * @param fullDS dataFrame to perform operation on
    * @param metric field to perform operation on
    * @param op operation to perform, options:
    *           "avg", "latest", "max", "sum"
    * @return dataFrame with resulting operation applied to the 'metric', grouped by region
    */
  def rankByMetric(
      spark: SparkSession,
      fullDS: DataFrame,
      metric: String,
      op: String = "avg"
  ): DataFrame = {
    import spark.implicits._
    var oneTimeMetric: DataFrame = spark.emptyDataFrame
    op match {
      // get average number of "metric", group by region, and sort in descending order by "metric"
      case "avg" => {
        oneTimeMetric = fullDS
          .groupBy("region")
          .agg(functions.avg(s"$metric") as s"$metric")
          .sort(functions.col(s"$metric") desc)
      }
      // select regions by latest date and sort in desc by metric count
      case "latest" => {
        val latestDate =
          fullDS.select(functions.max("date")).collect().map(_.getString(0))
        oneTimeMetric = fullDS
          .select("region", metric)
          .where($"date" === latestDate(0))
          .sort(functions.col(s"$metric") desc)
      }
      // group by region and get the max metric count and sort in desc order
      // NOTE: This searches for a single max instance of metric count
      //       NOT combined total for each region
      case "max" => {
        oneTimeMetric = fullDS
          .groupBy("region")
          .agg(functions.max(s"$metric") as s"$metric")
          .sort(functions.col(s"$metric") desc)
      }
      // get sum of all metrics by region and sort in desc order
      case "sum" => {
        oneTimeMetric = fullDS
          .groupBy("region")
          .agg(functions.sum(s"$metric") as s"$metric")
          .sort(functions.col(s"$metric") desc)
      }
      // for all other cases (op) return avg metric and sort
      case _ => {
        oneTimeMetric = fullDS
          .groupBy("region")
          .agg(functions.avg(s"$metric") as s"$metric")
          .sort(functions.col(s"$metric") desc)
      }
    }
    oneTimeMetric
  }

  /** returns resulting dataFrame after performing given operation 'op'
    * on some field/metric 'metric' on df
    * If no op is provided default operation is 'avg'
    *
    * @param spark spark session
    * @param fullDS dataFrame from question 1('data') that has been grouped by region
    *               with new population field added
    *               NOTE: original dataFrame is the joined 'econDF' and 'casesDF'
    * @param metric field to perform operation on
    * @param op operation to perform, options:
    *           "max", "pop", "maxpop", default = "avg"
    * @return resulting dataFrame
    *
    * TODO: Figure out what 'max' is doing
    * Warning: Queries below may not be producing correct output
    */
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
      }
      // no idea what this hot mess is doing/suppose to do
      // returns max? sum???
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

  /** NOTE: Not used anywhere inside application
    * No idea what it does
    */
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

  /** NOTE: Not used anywhere inside application
    * No idea what it does
    */
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
    f.saveas(s"${filename}.png")

  }

  /** calculates GDP percent change in each region
    * years used were 2019 & 2020
    *
    * @param spark spark session
    * @param fullDS dataFrame from question 1('data') that has been grouped by region
    *               with new population field added
    *               NOTE: original dataFrame is the joined 'econDF' and 'casesDF'
    * @param metric gdp metric
    * @param percapita Boolean flag (*** NOT USED ANYWHERE)
    */
  def changeGDP(
      spark: SparkSession,
      fullDS: DataFrame,
      metric: String,
      percapita: Boolean
  ): DataFrame = {
    import spark.implicits._

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

/** DUMMY DATA
  *
  * val testDF = Seq(
  *  ("2020-11-14", "Asia", "Japan", 40, 102, 1990),
  *  ("2020-11-18","Carribean","Dominican Republic",41, 122, 1991),
  *  ("2020-11-11","North America", "USA", 31, 90, 1992),
  *  ("2020-11-11","North America", "Canada", 33, 90, 1992),
  *  ("2020-11-11","North America", "USA", 71, 90, 1992),
  *  ("2020-11-19","Asia","China",33, 61, 1989),
  *  ("2020-11-22","Asia","China",45, 84, 1991),
  *  ("2020-11-31","Carribean","Bahamas",35, 72, 1992),
  *  ("2020-12-14","Carribean","Bahamas",25, 66, 1993)
  * ).toDF("date","region","country","cases", "assists", "season")
  */
