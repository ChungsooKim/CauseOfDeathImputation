#' Execute the Imputation study
#' @NAME createImputationData
#' @details This function will create the data for Imputation
#' @import dplyr 
#' @import ROCR
#' @import pROC
#' @import caret
#'
#' @param TAR                  Time at risk for determining risk window
#' @param outputFolder         Name of local folder to place results; make sure to use forward slashes
#'                             (/)
#' @export


createImputationData <- function(outputFolder, TAR) {
  
  ### Set save folder directory
  ParallelLogger::logInfo("Setting save folder...")
  savePath <- file.path(outputFolder, "CauseOfDeathImputation")
  if (!file.exists(savePath))
    dir.create(savePath)
  
  outpath <- file.path(getwd(), "inst", "settings", "settings.csv")
  settings <- utils::read.csv(outpath)
  
  ### Read setting file in plpResult folder
  ParallelLogger::logInfo("Read a setting file in plpResult folder...")
  settings <- settings %>% filter(riskWindowEnd == TAR)
  length <- length(settings$plpResultFolder)
  id <- settings$analysisId
  
  outList <- vector(mode = "list", length= length)
  
  for(i in 1:length) {
    analysispath <- paste(outputFolder, databaseName, "Analysis", sep = "/")
    analysispath <- paste(analysispath, id[i], sep = "_")
    outList[[i]]$analysispath <- analysispath
  }
  

  ### Read RDS files
  for(j in 1:length){
      rds <- readRDS(file.path(outList[[j]], "validationResult.rds"))
      outList[[j]]$prediction <- rds$prediction
      outList[[j]]$cohorts <- rds$cohorts
      outList[[j]]$demographics <- rds$demographics 
    names(outList)[[j]] <- paste("prediction", id[j], sep = "_")
  }
  
  
  ### Merge prediction values and outcomes
  ParallelLogger::logInfo("Creating Imputation data...")
  outDFvalue1 <- data.frame()
  outDFvalue2 <- data.frame()
  
  # 1- lasso logistic regression, 2- gradient boosting machine
  model1 <- which(settings$modelSettingId == 1)
  model2 <- which(settings$modelSettingId == 2)
  
  for (j in model1) {
    df1 <- outList[[j]]$prediction %>% select(subjectId, value)
    colnames(df1)[2]<- paste(settings$outcomeName[j], settings$modelSettingsId[j], sep = "_")
    if (length(outDFvalue1) == 0) {
      outDFvalue1 <- df1
    }
    else{
      outDFvalue1 <- dplyr::left_join(outDFvalue1, df1, by = "subjectId")
    }
  }
  valueName <- c("subjectId", "DeathValue1", "CancerValue1",
                 "IHDValue1", "CerebroValue1", "PneumoValue1",
                 "DMValue1", "LiverValue1", "CLRDValue1", "HTValue1")
  names(outDFvalue1) <- valueName
  
  for (j in model2) {
    df2 <- outList[[j]]$prediction %>% select(subjectId, value)
    colnames(df2)[2]<- paste(settings$outcomeName[j], settings$modelSettingsId[j], sep = "_")
    if (length(outDFvalue2) == 0) {
      outDFvalue2 <- df2
    }
    else{
      outDFvalue2 <- dplyr::left_join(outDFvalue2, df2, by = "subjectId")
    }
  }
  
  valueName <- c("subjectId", "DeathValue2", "CancerValue2",
                 "IHDValue2", "CerebroValue2", "PneumoValue2",
                 "DMValue2", "LiverValue2", "CLRDValue2", "HTValue2")
  names(outDFvalue2) <- valueName
  
  
  outDFdemographics <- data.frame()
  
  df3 <- outList[[1]]$cohorts %>% select(subjectId, rowId, cohortStartDate)
  df4 <- outList[[1]]$demographics
  df4$covariateId <- as.factor(df4$covariateId)
  
  lev <- c(8532001, 1002, seq(1003, 18003, by = 1000))
  levels <- levels(df4$covariateId)
  diff <- setdiff(lev, levels)
  blank <- rep(0, nrow(df3))
  
  df4 <- reshape2::dcast(df4, rowId ~ covariateId, value.var = "covariateValue")
  df4[is.na(df4)]<-0
  
  outDFdemographics <- merge(df3, df4, by = "rowId", all.x = T) %>% select (-rowId)
  for (i in 1:length(diff)){
    outDFdemographics <- cbind(outDFdemographics, blank)
    names(outDFdemographics)[length(outDFdemographics)] <- paste(diff[i])
    if(i == length(diff)) break
  }

 
  outDFdemographics <- outDFdemographics %>%
    select(subjectId, cohortStartDate, "8532001", "1002", everything())
  
  outDF <- left_join(outDFdemographics, outDFvalue1, by = "subjectId")
  outDF <- left_join(outDF, outDFvalue2, by = "subjectId")
  outDF <- na.omit(outDF)
  
  ### save file in save directory
  ParallelLogger::logInfo("Save Imputation data file in save folder...")
  savepath <- file.path(savePath, "out_df_")
  savepath <- paste(savepath,TAR,".rds", sep = "")
  saveRDS(outDF, file = savepath)
}
