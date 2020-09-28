import ConditionalExtractors.spSess
import frameless._
import frameless.syntax._
import frameless.functions._
import org.apache.log4j.{Level, Logger}

object Main extends App with PreProcFetcher with SessionWrapper{
        import spSess.implicits._
        Logger.getLogger("org").setLevel(Level.ERROR)




}
