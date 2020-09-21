object MainExtractors {
  def dateInfExtractor(df: DataFrame): DataFrame =
    df.withColumn("date", regexp_replace(regexp_extract(col("date"),
    "[0-9]{4}-[0-9]{2}-[0-9]{2}[A-Za-z][0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}", 0), "[A-Za-z]"," "))
    .withColumn("sDate", regexp_extract(col("date"),"[0-9]{4}-[0-9]{2}-[0-9]{2}",0))
    .withColumn("day",date_format(col("date"),"E"))
    .withColumn("hour",hour(col("date")))
    .withColumn("hStatus"
      ,when(hour(col("date"))>=2 && hour(col("date"))<6, "latenight")
        .when(hour(col("date"))>=6 && hour(col("date"))<12, "morning")
        .when(hour(col("date"))>=12 && hour(col("date"))<18, "afternoon")
        .when(hour(col("date"))>=18 && hour(col("date"))<=22, "evening")
        .otherwise("midnight")
    )
  def typeInfExtractor(df : DataFrame): DataFrame =
    df.withColumn("weekend",
          when(col("day")==="Sun" || col("day")==="Sat",1)
          otherwise(0))
      .withColumn("specOffer"
          ,when(col("type")==="S", "special")
          .when(col("type")===0, "missing")
          .when(length(col("type"))>=8, "brand")
          .otherwise("normal")
    )

  def userPathExtractor(df:DataFrame) : DataFrame =
    df
    .select("sessID","itemID","fullDate","type")
    .withColumn("userPath",collect_set("type").over(Window.partitionBy("sessID")))
}