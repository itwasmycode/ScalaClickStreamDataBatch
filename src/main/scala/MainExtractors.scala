object MainExtractors{
  /*
  def dateInfExtractor(df: DataFrame): DataFrame = df
    .withColumn("hStatus"
      , when(hour(col("date")) >= 2 && hour(col("date")) < 6, "latenight")
        .when(hour(col("date")) >= 6 && hour(col("date")) < 12, "morning")
        .when(hour(col("date")) >= 12 && hour(col("date")) < 18, "afternoon")
        .when(hour(col("date")) >= 18 && hour(col("date")) <= 22, "evening")
        .otherwise("midnight")
    )

  def dateInfExtractor(df: DataFrame): DataFrame =
    df.withColumn("weekend",
      when(col("day") === "Sun" || col("day") === "Sat", 1)
        otherwise (0)
    )


  def itemPopularityExtractor(df: DataFrame) = {
    val df1 = df.groupBy("sDate", "itemID").count()
      .withColumnRenamed("sDate", "sDate1")
      .withColumnRenamed("itemID", "itemID1")
    df.join(df1, df("sDate") === df1("sDate1") && df("itemID") === df1("itemID1"))
  }

*/
        }
