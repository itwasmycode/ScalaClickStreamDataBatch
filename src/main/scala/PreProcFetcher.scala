import ConditionalExtractors.spSess

import frameless._
import frameless.syntax._


trait PreProcFetcher extends SessionWrapper{
        /* Implementing classes*/

        import spSess.implicits._
        val pageTypeUpdatedDF: TypedDataset[caseClsStack.ClickObjUpdated] = ConditionalExtractors
          .pageTypeExtractor(typedClickDF)

        val userPathExtractedDF: TypedDataset[caseClsStack.userPathDF] = ConditionalExtractors
          .userPathExtractor(typedClickDF)

        val sessTimeExtractedDF: TypedDataset[caseClsStack.sessIDTimePairDF] = ConditionalExtractors
          .timeSpentEachSessionExtractor(typedClickDF)

        val itemDayPopularityExtractedDF: TypedDataset[caseClsStack.itemPopularityPreCounter] =
                ConditionalExtractors.itemDayPopularityExtractor(typedClickDF)

        val itemAllTimePopularityExtractedDF: TypedDataset[caseClsStack.itemAllTimePopularityDF] =
                ConditionalExtractors.itemAllTimePopularityExtractor(typedClickDF)

        Combiner.pageTypeUserPathCombiner(pageTypeUpdatedDF,userPathExtractedDF).show(10).run()
}
