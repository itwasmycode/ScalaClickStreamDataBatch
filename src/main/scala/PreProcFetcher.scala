import ConditionalExtractors.spSess

import frameless._
import frameless.syntax._
import frameless.functions._


trait PreProcFetcher extends SessionWrapper {
  /* Implementing classes*/

  import spSess.implicits._

  val pageTypeUpdatedDF: TypedDataset[caseClsStack.ClickObjUpdated] = ConditionalExtractors
    .pageTypeExtractor(typedClickDF)

  val userPathExtractedDF: TypedDataset[caseClsStack.userPathDF] = ConditionalExtractors
    .userPathExtractor(typedClickDF)

  val sessTimeExtractedDF: TypedDataset[caseClsStack.sessIDTimePairDF] =
    ConditionalExtractors.timeSpentEachSessionExtractor(typedClickDF)

  val itemDayPopularityExtractedDF: TypedDataset[caseClsStack.itemPopularityPreCounter] =
    ConditionalExtractors.itemDayPopularityExtractor(typedClickDF)

  val itemAllTimePopularityExtractedDF: TypedDataset[caseClsStack.itemAllTimePopularityDF] =
    ConditionalExtractors.itemAllTimePopularityExtractor(typedClickDF)

  val firstCombinedDF: TypedDataset[caseClsStack.firstCombiner] =
    Combiner.pageTypeUserPathCombiner(pageTypeUpdatedDF, userPathExtractedDF)

  val secondCombinedDF: TypedDataset[caseClsStack.secondCombiner] =
    Combiner.updatedUserPathTimespentCombiner(firstCombinedDF, sessTimeExtractedDF)

  val thirdCombinedDF: TypedDataset[caseClsStack.thirdCombiner] =
    Combiner.updatedBaseCasesDayPopularity(secondCombinedDF, itemDayPopularityExtractedDF)

  val preExtractedDF: TypedDataset[caseClsStack.thirdCombinerHourUpdated] =
    ConditionalExtractors.hourRangeExtractor(thirdCombinedDF)

  val preCatEncodedDF: TypedDataset[caseClsStack.baselineCatFinalEncodedDF] =
    ConditionalExtractors.catEncoderPre(preExtractedDF)


  val readyForLabelDF: TypedDataset[caseClsStack.baselineAvgPopularityCatFinalEncodedDF] =
    ConditionalExtractors.avgItemPopularity(preCatEncodedDF)

    val readyForModelDF:TypedDataset[caseClsStack.readyForBaseModelDF] = readConditionalExtractors.userPurchaseInfoExtractor(readyForLabelDF,typedBuyDF)


}
