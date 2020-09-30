import frameless.TypedDataset
import frameless.syntax._
import frameless.functions._
import frameless._

object Combiner extends SessionWrapper {
 def pageTypeUserPathCombiner(df1:TypedDataset[caseClsStack.ClickObjUpdated],
                             df2: TypedDataset[caseClsStack.userPathDF]) ={
     import spSess.implicits._
     val joinedDF = df1.joinInner(df2) { df1('sessID) === df2('sessID) }
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

 def userPurchaseInfoExtractor(df: TypedDataset[caseClsStack.ClickObj])={
  ???
 }


}
