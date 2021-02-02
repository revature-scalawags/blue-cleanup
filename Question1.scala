package blue

import org.apache.spark.sql.functions.{max, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Question 1
  * Answers the following queries using spark dataframes and outputs to console:
  *     "Average New Cases per Day in Each Region"
  *     "Average New Cases per Million People per Day in Each Region (normalized before region grouping)"
  *     "Average New Cases per Million People per Day in Each Region"
  *     "Total Cases in Each Region"
  *     "Total Cases per Million People in Each Region (normalized before region grouping)"
  *     "Total Cases per Million People in Each Region"
  *     "Average GDP Percent Change in Each Region"
  *     "Average GDP per Capita Percent Change in Each Region"
  */

object Question1 {
  /**initialSolution
   * takes in the dataframe that reflects the case and economic data output from DataFrameManipulator
   * and outputs the data as formatted tables in the console as well as outputs the formatted data to dataframes in Spark.
   * The DataFrameManipulator prepares raw data from two source files by selecting various, needed columns and renaming a few.
   * The two tables are then joined with the joinCaseEcon method. The resulting dataframe is passed in this methods signature.
   * The RankRegions class uses methods to group and sort by some metric passed as an argument. This is used just prior to displaying results.
   *
   * @param spark Spark Sessions instance for implicits.
   * @param df  Fully prepared and joined dataset from the DataFrameManipulator class.
   * @param resultPath Provides an results, output path for s3 storage.  However, currently not in use. This app only displays console results as of now.
   */
  def initialSolution(spark: SparkSession, df: DataFrame, resultPath: String): Unit ={
    import spark.implicits._
    val data = df
      .withColumn("population",$"total_cases"/$"total_cases_per_million")
      .groupBy("region", "country")
      .agg(max($"population") as "population")
      .groupBy("region")
      .agg(sum($"population") as "population")

    println("\nAverage New Cases per Day in Each Region")
    RankRegions.rankByMetricLow(spark, data, "new_cases").show()

    println("\nAverage New Cases per Million People per Day in Each Region (normalized before region grouping)")
    RankRegions.rankByMetricLow(spark, data, "new_cases_per_million").show()

    println("\nAverage New Cases per Million People per Day in Each Region")
    RankRegions.rankByMetricLow(spark, data, "new_cases", "pop").show()

    println("\nTotal Cases in Each Region")
    RankRegions.rankByMetricLow(spark, data, "total_cases", "max").show()

    println("\nTotal Cases per Million People in Each Region (normalized before region grouping)")
    RankRegions.rankByMetricLow(spark, data, "total_cases_per_million", "max").show()

    println("\nTotal Cases per Million People in Each Region")
    RankRegions.rankByMetricLow(spark, data, "total_cases", "maxpop").show()

    println("\nAverage GDP Percent Change in Each Region")
    RankRegions.changeGDP(spark, data, "current_prices_gdp", percapita = false).show()

    println("\nAverage GDP per Capita Percent Change in Each Region")
    RankRegions.changeGDP(spark, data, "gdp_per_capita", percapita = false).show()
  }
}