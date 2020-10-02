import caseClsStack.ClickObjUpdated
import frameless.TypedDataset
import frameless.functions.nonAggregate.when
import frameless.functions.aggregate.{min, max,collectSet,count,sum,avg}
import frameless.functions.udf
import frameless.functions._
import frameless.syntax._
import frameless._
import java.text.SimpleDateFormat

object ConditionalExtractors extends SessionWrapper {
   def pageTypeExtractor(df:TypedDataset[caseClsStack.ClickObj])
        : TypedDataset[ClickObjUpdated] = {
                import spSess.implicits._
                val testUDF = (pType: String) =>
                  if (pType == "S") "special"
                  else if (pType.length >= 8) "brand"
                  else "missing"

        val udf = df.makeUDF(testUDF)
        df.select(df('sessID),
                  df('date),
                  df('itemID),
                  df('pType),
                  df('sDate),
                  df('day),
                  df('hour),
       udf(df('pType))).as[ClickObjUpdated]
   }

  def userPathExtractor(df:TypedDataset[caseClsStack.ClickObj])
        :TypedDataset[caseClsStack.userPathDF] =
  {
          import spSess.implicits._
          df.groupBy(df('sessID))
            .agg(
                    collectSet(df('pType))
            ).as[caseClsStack.userPathDF]
  }
 def timeSpentEachSessionExtractor(df: TypedDataset[caseClsStack.ClickObj]): TypedDataset[caseClsStack.sessIDTimePairDF] =
       {
         import spSess.implicits._
         val strFormat  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
         val testUDF = (sDate: String, fDate: String)=>
           Math.abs(strFormat.parse(sDate).getTime() -
             strFormat.parse(fDate).getTime())/(60 * 1000) % 60

         val retDF = df.groupBy(df('sessID))
           .agg(min(df('date)), max(df('date))).as[caseClsStack.timeSpentSessionDF]

         val timeUDF = retDF.makeUDF(testUDF)
         retDF.select(retDF('sessID),timeUDF(retDF('sDate),retDF('fDateFinal)))
           .as[caseClsStack.sessIDTimePairDF]
 }
  def itemDayPopularityExtractor(df:TypedDataset[caseClsStack.ClickObj]) = {
    import spSess.implicits._
    df.groupBy(df('sDate),df('itemID)).agg(count())
    .as[caseClsStack.itemPopularityPreCounter]
  }

  def itemAllTimePopularityExtractor(df:TypedDataset[caseClsStack.ClickObj]): TypedDataset[caseClsStack.itemAllTimePopularityDF] = {
    import spSess.implicits._
    df.groupBy(df('itemID)).agg(count())
      .as[caseClsStack.itemAllTimePopularityDF]
  }/*
  def dayPopularityExtractor(df:TypedDataset[caseClsStack.ClickObj]): TypedDataset[caseClsStack.itemAllTimePopularityDF] = {
    import spSess.implicits._
    df.groupBy(df('itemID)).agg(count())
      .as[caseClsStack.itemAllTimePopularityDF]
  }*/

  def hourRangeExtractor(df:TypedDataset[caseClsStack.thirdCombiner])
  : TypedDataset[caseClsStack.thirdCombinerHourUpdated]={
    import spSess.implicits._
    val hourUDF = (hour:Int)=>
    if ((hour >= 2) && (hour<6)) "latenight"
      else if ((hour >= 6) && (hour<12)) "morning"
      else if ((hour >= 12) && (hour<18)) "afternoon"
      else if ((hour >= 18) && (hour<22)) "evening"
      else "midnight"

    val udf = df.makeUDF(hourUDF)

    df.withColumnTupled(udf(df('hour)))
      .as[caseClsStack.thirdCombinerHourPreUpdated]
      .drop[caseClsStack.thirdCombinerHourUpdated]
  }

  def catEncoderPre(df:TypedDataset[caseClsStack.thirdCombinerHourUpdated])
  : TypedDataset[caseClsStack.baselineCatFinalEncodedDF] ={
    import spSess.implicits._
    val weekendUDF = (day:String)=>
      if ((day == "Sun") || (day =="Sat")) 1
      else 0

    val specOffUDF = (specOffer:String)=>
      if (specOffer == "special") 1 else 0

    val weekUDF = df
      .makeUDF(weekendUDF)

    val dayLabeledDF = df.withColumnTupled(weekUDF(df('day)))
      .as[caseClsStack.baselineCatEncodedPreDF]

    val specUDF = dayLabeledDF
      .makeUDF(specOffUDF)
    dayLabeledDF.withColumnTupled(
      specUDF(dayLabeledDF('specOffer))
    ).as[caseClsStack.baselineCatEncodedDF]
      .drop[caseClsStack.baselineCatFinalEncodedDF]

  }

  def userPurchaseInfoExtractor(clickDF: TypedDataset[caseClsStack.baselineAvgPopularityCatFinalEncodedDF]
                                ,checkoutDF: TypedDataset[caseClsStack.BuyObj])=
  {

    val transactionCountDF = checkoutDF.groupBy(checkoutDF('sessID))
      .agg(sum(checkoutDF('qty))).as[caseClsStack.sessIDCheckoutCount]

    val baseJoinedDF =  clickDF
      .joinRight(transactionCountDF) {
        clickDF('sessID) === transactionCountDF('sessID)
      }

    baseJoinedDF
  }

  def avgItemPopularity(df: TypedDataset[caseClsStack.baselineCatFinalEncodedDF])={
   val averageVisTimeDF =  df.groupBy(df('sessID))
    .agg(avg(df('pCountPartial))).as[caseClsStack.avgItemPopularity]
    val baseJoinedDF = df.joinInner(averageVisTimeDF){
      df('sessID)=== averageVisTimeDF('sessID)
    }
    baseJoinedDF.select(
      baseJoinedDF.colMany('_1,'sessID),
      baseJoinedDF.colMany('_1,'timeSpent),
      baseJoinedDF.colMany('_1,'pHour),
      baseJoinedDF.colMany('_1,'encday),
      baseJoinedDF.colMany('_1,'encspecOffer),
      baseJoinedDF.colMany('_2,'pCountPartialAvg)
    ).distinct.as[caseClsStack.baselineAvgPopularityCatFinalEncodedDF]
  }

}
