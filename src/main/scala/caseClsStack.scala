import java.sql.Timestamp

import ConditionalExtractors.spSess

object caseClsStack{
   import spSess.implicits._
  sealed trait BaseClsObject
  sealed trait CombinerClsObject

  final case class ClickObj(sessID:String,
                 date:String,
                 itemID:Long,
                 pType:String,
                 sDate:String,
                 day:String,
                 hour:Int) extends BaseClsObject

  final case class ClickObjUpdated(sessID:String,
                        date:String,
                        itemID:Long,
                        pType:String,
                        sDate:String,
                        day:String,
                        hour:Int,
                        specOffer:String) extends BaseClsObject

  final case class ClickObjUpdatednoDummies(sessID:String,
                                 date:String,
                                 itemID:Long,
                                 pType:String,
                                 sDate:String,
                                 day:String,
                                 hour:Int,
                                 specOffer:String) extends BaseClsObject


  final case class BuyObj(sessID:String,
               date:String,
               itemID:Long,
               price:Long,
               qty:Int,
               sDate:String,
               day:String,
               hour:Int) extends BaseClsObject

  final case class userPathDF(sessID: String,userPath: Vector[String]) extends BaseClsObject

  final case class timeSpentSessionDF(sessID: String,
                                       sDate: String,
                                       fDateFinal: String) extends BaseClsObject

  final case class sessIDTimePairDF(sessID : String,
                                    timeSpent: Long) extends BaseClsObject


  final case class itemPopularityPreCounter(sDay:String,
                                            itemID:Long,
                                            pCount: Long) extends BaseClsObject

  final case class itemAllTimePopularityDF(itemID:Long,
                                            pCount: Long) extends BaseClsObject

  // Caseclasses for grouping operations

  final case class firstCombiner(sessID:String,
                                   date:String,
                                   itemID:Long,
                                   pType:String,
                                   sDate:String,
                                   day:String,
                                   hour:Int,
                                   specOffer:String,
                                   userPath : Vector[String]) extends CombinerClsObject
}
