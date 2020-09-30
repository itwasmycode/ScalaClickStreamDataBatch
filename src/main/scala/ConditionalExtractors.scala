import caseClsStack.ClickObjUpdated
import frameless.TypedDataset
import frameless.functions.nonAggregate.when
import frameless.functions.aggregate.{min, max,collectSet,count}
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
  }
  def dayPopularityExtractor(df:TypedDataset[caseClsStack.ClickObj]): TypedDataset[caseClsStack.itemAllTimePopularityDF] = {
    import spSess.implicits._
    df.groupBy(df('itemID)).agg(count())
      .as[caseClsStack.itemAllTimePopularityDF]
  }


}
