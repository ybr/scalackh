package ckh.native

// Determines till which state SELECT query should be executed
sealed trait QueryProcessingStage
case object FetchColumns extends QueryProcessingStage
case object WithMergeableState extends QueryProcessingStage
case object Complete extends QueryProcessingStage