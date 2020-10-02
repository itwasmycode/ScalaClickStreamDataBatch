object caseClsStack{
  /** Keeps case classes inside in order to serve when it is required.
   * Params are parameter for case classes not functions.
   * For Click Data Set Base Case Class Parameters.
   *  @param sessID sessID that belongs to transaction. Every sessID unique to user.
   *  @param date Date belongs to transaction completion time.
   *  @param itemID Current itemID, it can be view type or in the checkout page.
   *  @param pType PageType of current event,page. It can be special offer, brand offer or another page type.
   *  @param sDate Shortened version of Date. Consist day, it needs for GroupBy operations.
   *  @param day Current day of transaction. In order to measure relationship of day-purchase.
   *  @param hour Current hour of transaction. In order to measure relationship hour-purchase.
   * For Buy Data Set Base Case Class Parameters
   *  @param sessID sessID that belongs to transaction. Every sessID unique to user.
   *  @param date Date belongs to transaction completion time.
   *  @param itemID Current itemID, it can be view type or in the checkout page.
   *  @param price Price of purchased item.
   *  @param qty Quantity of purchased item.
   *  @param sDate Shortened version of Date. Consist day, it needs for GroupBy operations.
   *  @param day Current day of transaction. In order to measure relationship of day-purchase.
   *  @param hour Current hour of transaction. In order to measure relationship hour-purchase.
   *
   * @return Any
   */
  sealed trait BaseClsObject

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


  final case class itemPopularityPreCounter(sDate: String,
                                            itemID: Long,
                                            pCountPartial: Long) extends BaseClsObject

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
                                   userPath : Vector[String]) extends BaseClsObject

  final case class secondCombiner(sessID:String,
                                  date:String,
                                  itemID:Long,
                                  pType:String,
                                  sDate:String,
                                  day:String,
                                  hour:Int,
                                  specOffer:String,
                                  userPath : Vector[String],
                                 timeSpent : Long) extends BaseClsObject

  final case class thirdCombiner(sessID:String,
                                  date:String,
                                  itemID:Long,
                                  sDate:String,
                                  day:String,
                                  hour:Int,
                                  specOffer:String,
                                  userPath : Vector[String],
                                 timeSpent : Long,
                                pCountPartial:Long) extends BaseClsObject

  final case class thirdCombinerHourPreUpdated(sessID:String,
                                            date:String,
                                            itemID:Long,
                                            sDate:String,
                                            day:String,
                                            hour: Int,
                                            specOffer:String,
                                            userPath : Vector[String],
                                            timeSpent : Long,
                                            pCountPartial:Long,
                                            pHour:String) extends BaseClsObject

  final case class thirdCombinerHourUpdated(sessID:String,
                                  itemID:Long,
                                  sDate:String,
                                  day:String,
                                  specOffer:String,
                                 timeSpent : Long,
                                 pCountPartial:Long,
                                  pHour:String) extends BaseClsObject

  final case class baselineCatEncodedPreDF(sessID:String,
                                        itemID:Long,
                                        sDate:String,
                                           day:String,
                                        specOffer:String,
                                        timeSpent : Long,
                                        pCountPartial:Long,
                                        pHour:String,
                                          encday:Int) extends BaseClsObject

  final case class baselineCatEncodedDF(sessID:String,
                                        itemID:Long,
                                        sDate:String,
                                        day:String,
                                        specOffer:String,
                                        timeSpent : Long,
                                        pCountPartial:Long,
                                        pHour:String,
                                        encday:Int,
                                        encspecOffer:Int) extends BaseClsObject

  final case class baselineCatFinalEncodedDF(sessID:String,
                                        itemID:Long,
                                        timeSpent : Long,
                                        pCountPartial:Long,
                                        pHour:String,
                                        encday:Int,
                                        encspecOffer:Int) extends BaseClsObject

  final case class distinctedFinalEncodedDF(sessID: String,
                                           timeSpent:Long,
                                           pCountPartial:Long,
                                           pHour:String,
                                           encday:Int,
                                           encspecOffer:Int)

  final case class sessIDCheckoutCount(sessID:String,
                                      cCount:Long)

  final case class avgItemPopularity(sessID:String,
                                    pCountPartialAvg:Double)

  final case class baselineAvgPopularityCatFinalEncodedDF(sessID:String,
                                             timeSpent : Long,
                                             pHour:String,
                                             encday:Int,
                                             encspecOffer:Int,
                                             pCountPartialAvg:Double) extends BaseClsObject

  final case class readyForBaseModelDF(sessID:String,
                                      timeSpent:Long,
                                      pHour:Long,
                                      encday:Long,
                                      encspecOffer:Long,
                                      pCountPartialAvg:Double,
                                      buy:Int)

 /* final case class baselineCatProjectedDF(timespent:Long,
                                         pCountPartial:Long,
                                         encDay:Int,
                                         encspecOffer:Int)*/
}
