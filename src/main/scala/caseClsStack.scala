import java.sql.Timestamp

import ConditionalExtractors.spSess

object caseClsStack{
 import spSess.implicits._
/*sealed trait ObjCls[+T]*/
final case class ClickObj(sessID:String,
               date:String,
               itemID:Long,
               pType:String,
               sDate:String,
               day:String,
               hour:Int)

 case class ClickObjUpdated(sessID:String,
                      date:String,
                      itemID:Long,
                      pType:String,
                      sDate:String,
                      day:String,
                      hour:Int,
                      specOffer:String)

 case class ClickObjUpdatednoDummies(sessID:String,
                               date:String,
                               itemID:Long,
                               pType:String,
                               sDate:String,
                               day:String,
                               hour:Int,
                               specOffer:String)

  case class TestDummy(sessID:String,
                date:String,
                itemID:Long,
                pType:String,
                sDate:String,
                day:String,
                hour:Int,
                specOffer:String,
                caseLbl:Int)

final case class BuyObj(sessID:String,
             date:String,
             itemID:Long,
             price:Long,
             qty:Int,
             sDate:String,
             day:String,
             hour:Int)

 final case class userPathDF(sessID: String,userPath: Vector[String])

 final case class timeSpentSessionDF(sessID: String,
                                     sDate: String,
                                     fDateFinal: String)

 final case class sessIDTimePairDF(sessID : String,
                                  timeSpent: Long)
}
