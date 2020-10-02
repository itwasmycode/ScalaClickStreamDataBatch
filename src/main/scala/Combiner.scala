import frameless.TypedDataset
import frameless.syntax._
import frameless.functions._
import frameless._

object Combiner extends SessionWrapper {

 def pageTypeUserPathCombiner(df1:TypedDataset[caseClsStack.ClickObjUpdated],
                             df2: TypedDataset[caseClsStack.userPathDF])
 :TypedDataset[caseClsStack.firstCombiner] =
 {
     import spSess.implicits._
   val joinedDF = df1.joinInner(df2){
     df1('sessID) === df2('sessID)
   }
     joinedDF.select(
        joinedDF.colMany('_1,'sessID),
        joinedDF.colMany('_1,'date),
        joinedDF.colMany('_1,'itemID),
        joinedDF.colMany('_1,'pType),
        joinedDF.colMany('_1,'sDate),
        joinedDF.colMany('_1,'day),
        joinedDF.colMany('_1,'hour),
        joinedDF.colMany('_1,'specOffer),
        joinedDF.colMany('_2,'userPath)
     ).as[caseClsStack.firstCombiner]
 }
  def updatedUserPathTimespentCombiner(df1:TypedDataset[caseClsStack.firstCombiner],
                               df2: TypedDataset[caseClsStack.sessIDTimePairDF])
  :TypedDataset[caseClsStack.secondCombiner] =
  {
    import spSess.implicits._
    val joinedDF = df1.joinInner(df2){
      df1('sessID) === df2('sessID)
    }
    joinedDF.select(
        joinedDF.colMany('_1,'sessID),
        joinedDF.colMany('_1,'date),
        joinedDF.colMany('_1,'itemID),
        joinedDF.colMany('_1,'pType),
        joinedDF.colMany('_1,'sDate),
        joinedDF.colMany('_1,'day),
        joinedDF.colMany('_1,'hour),
        joinedDF.colMany('_1,'specOffer),
        joinedDF.colMany('_1,'userPath),
        joinedDF.colMany('_2,'timeSpent)
    ).as[caseClsStack.secondCombiner]
  }

  def updatedBaseCasesDayPopularity(df1:TypedDataset[caseClsStack.secondCombiner],
                                       df2: TypedDataset[caseClsStack.itemPopularityPreCounter]):TypedDataset[caseClsStack.thirdCombiner] =
  {
    import spSess.implicits._
    val joinedDF = df1.joinInner(df2){
      (df1('sDate) === df2('sDate)) && (df1('itemID) === df2('itemID))
    }
    joinedDF.select(
      joinedDF.colMany('_1,'sessID),
      joinedDF.colMany('_1,'date),
      joinedDF.colMany('_1,'itemID),
      joinedDF.colMany('_1,'sDate),
      joinedDF.colMany('_1,'day),
      joinedDF.colMany('_1,'hour),
      joinedDF.colMany('_1,'specOffer),
      joinedDF.colMany('_1,'userPath),
      joinedDF.colMany('_1,'timeSpent),
      joinedDF.colMany('_2,'pCountPartial)).as[caseClsStack.thirdCombiner]
  }


}
