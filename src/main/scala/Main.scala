import frameless.syntax._
import org.apache.log4j.{Level, Logger}
import java.sql.Date


object Main extends App with SessionWrapper {
  Logger.getLogger("org").setLevel(Level.ERROR)
  case class BuyObj(sessID:String,
                    date:Date,
                    itemID:String,
                    price:Long,
                    qty:String)


  case class ClickObj(sessID: String,
                      date: Date,
                      itemID: String,
                      pType: String)

  typedClickDF.select(typedClickDF('date)).show().run()
}
