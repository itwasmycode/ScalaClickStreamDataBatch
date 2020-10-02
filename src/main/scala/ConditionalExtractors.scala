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
  /*
  * Transforms and cleans messy data set to useful form that can be used in ML Model.
   */
   def pageTypeExtractor(df:TypedDataset[caseClsStack.ClickObj])
        : TypedDataset[ClickObjUpdated] = {
     /** Converts a PageType column to three reasonably important category.(Special,brand,missing)
      *
      *  @param df TypedDataFrame which has PageType column inside it.
      * @return TypedDataFrame
      */
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
    /** Creates a userPath using how many different page user visited
     * TODO: Use it in RNN and measure how order of page visiting can change the prediction power.
     *
     *  @param df TypedDataFrame which has the PageType.
     *  @return TypedDataFrame
     */
          import spSess.implicits._
          df.groupBy(df('sessID))
            .agg(
                    collectSet(df('pType))
            ).as[caseClsStack.userPathDF]
  }

 def timeSpentEachSessionExtractor(df: TypedDataset[caseClsStack.ClickObj]): TypedDataset[caseClsStack.sessIDTimePairDF] =
       {
         /** Creates a column based on user First Seen time in Session and Last. Substracts two of them to find
          * time spent in this session.
          *
          *  @param df TypedDataFrame that has column date column.
          * @return TypedDataFrame
          */

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
    /** Calculates day popularity of item. If it is popular, there is a prior belief it will affect the
     * status of session.
     *  @param df TypedDataFrame that has itemID,CurrentDay column.
     * @return TypedDataFrame
     */
    import spSess.implicits._
    df.groupBy(df('sDate),df('itemID)).agg(count())
    .as[caseClsStack.itemPopularityPreCounter]
  }

  def itemAllTimePopularityExtractor(df:TypedDataset[caseClsStack.ClickObj]): TypedDataset[caseClsStack.itemAllTimePopularityDF] = {
    /** Calculates all time popularity of item. It uses previous data.
     * If it is popular, there is a prior belief it will affect the
     * status of session.
     *  @param df TypedDataFrame that has itemID column.
     * @return TypedDataFrame
     */
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
    /** Transforms hour column to binned hour column. There is four 5 bin based on shopping behaviour of users.
     *  @param df TypedDataFrame that has hour Column.
     * @return TypedDataFrame
     */
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
    /** It serves for 2 main goal. One is indicator value for weekend, and another one is "item is specialOffer now?"
     *  @param df TypedDataFrame that has PageType column.
     * @return TypedDataFrame
     */
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
    /** If user matches with Buying DataFrame it calculates how much product bought.
     *  @param clickDF TypedDataFrame that is preprocessed.
     *  @param checkoutDF TypedDataFrame that is defined at the SessionWrapper.
     * @return TypedDataFrame
     */
    val transactionCountDF = checkoutDF.groupBy(checkoutDF('sessID))
      .agg(sum(checkoutDF('qty))).as[caseClsStack.sessIDCheckoutCount]

    val baseJoinedDF =  clickDF
      .joinRight(transactionCountDF) {
        clickDF('sessID) === transactionCountDF('sessID)
      }

    baseJoinedDF
  }

  def avgItemPopularity(df: TypedDataset[caseClsStack.baselineCatFinalEncodedDF])={
    /** Since data wrangling needs to transform base TypedDataset to Final one, for distinct operation
     * this function uses mean of average popularity of viewed items.
     *  @param df TypedDataFrame that needs pre calculated data set using Item Popularity functions.
     * @return TypedDataFrame
     */
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
