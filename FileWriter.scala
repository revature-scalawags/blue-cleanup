// Writes a DataFrame to a .csv file
object FileWriter {
  // Takes DataFrame, what to call the output file name, and Max Records to save (default 100)
  def writeDataFrameToFile(
      dataFrame: DataFrame,
      outputFileName: String,
      maxRecords: Int = 100
  ) = {
    dataFrame
      .limit(maxRecords)
      .write
      .format("csv")
      .save(s"s3a://*S3 Results Directory*//$outputFileName")
  }
}