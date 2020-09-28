import ConditionalExtractors.spSess
import frameless._
import frameless.syntax._


trait PreProcFetcher extends SessionWrapper{
        /* Implementing classes*/
        import spSess.implicits._
        val pageTypeUpdatedDF: TypedDataset[caseClsStack.ClickObjUpdated] = ConditionalExtractors
          .pageTypeExtractorn(typedClickDF)

        val userPathExtractedDF: TypedDataset[caseClsStack.userPathDF] = ConditionalExtractors
          .userPathExtractor(typedClickDF)

        val sessTimeExtractedDF: TypedDataset[caseClsStack.sessIDTimePairDF] = ConditionalExtractors
          .timeSpentEachSessionExtractor(typedClickDF)

}
