package main


package object caseClsStack {
  sealed trait ObjCls
  case class ClickObjUpdated(sessID: String,
                             date: String,
                             itemID: Long,
                             pType: Int,
                             sDate:String,
                             day:String,
                             hour:Int,
                             pTypeUpdated: Int)

}