#' Execute the Imputation study
#' @NAME causeImputation
#' @details This function will run the random forest model for classify causes of death
#' @import dplyr 
#' @import ROCR
#' @import pROC
#' @import caret
#' @import ggplot2
#' @import xlsx
#' @import rJava
#' @import ParallelLogger
#'
#' @param TAR                  Time at risk for determining risk window
#' @param outputFolder         Name of local folder to place results; make sure to use forward slashes
#'                             (/)
#' @export


causeImputation <- function(outputFolder, TAR){
  
  `%notin%` <- Negate(`%in%`)
  
  saveFolder <- file.path(outputFolder ,"CauseOfDeathImputation")
  if (!file.exists(saveFolder))
    dir.create(saveFolder, recursive = T)
  
  savepath <- file.path(saveFolder,"out_df_")
  savepath <- paste(savepath, TAR,".rds", sep = "")
  outDF <- readRDS(savepath)
  
  ParallelLogger::logInfo("Imputation Start...")
  ParallelLogger::logInfo("Read Imputation table file in save folder...")
  
  ### Run Imputation
  ParallelLogger::logInfo("Read the cause prediction model...")
  modelpath <- paste(getwd(), "inst", "finalModels", "final_model", sep = "/")
  modelpath <- paste(modelpath, TAR, sep = "_")
  modelpath <- paste(modelpath, "rds", sep = ".")
  cause.model <- readRDS(modelpath)
  
  ### Result
  ParallelLogger::logInfo("Predicting response and calculating prediction values...")
  dataTest <- outDF
  
  dataTestResult <- dataTest
  dataTestResult$cause.prediction <- predict(cause.model, dataTest)
  #dataTestResult$cause.value <- predict(cause.model, dataTest, type = "prob")
  
  dataTestResult <- dataTestResult %>% select(1:22, cause.prediction)
  
  
  
  
  ## Result file
  causeName <- data.frame(cause.prediction = c(0:8,99), 
                          CauseName = c("No Death", "Malignant cancer", "Ischemic heart disease", 
                                        "Cerebrovascular disease","Pneumonia", "Diabetes", 
                                        "Liver disease", "Chronic lower respiratory disease", 
                                        "Hypertensive disease", "Others"))
  causeName$cause.prediction <- as.factor(causeName$cause.prediction)
  
  totalN <- nrow(dataTestResult)
  deathN <- nrow(dataTestResult[dataTestResult$cause.prediction!=0,])
  
  table1 <- dataTestResult %>%
    group_by(cause.prediction) %>%
    summarise(
      Count = n(), 
      "Percent(%)" = round(n()/totalN*100,2), 
      "Percent in Death(%)" = round(n()/deathN*100,2)
    )
  
  table1 <- merge(causeName, table1, by = "cause.prediction", all.x = T) 
  table1[is.na(table1)] <- 0
  
  temp <- table1[1,] %>% mutate(CauseName ="total N of target cohort", Count = totalN, 
                                "Percent(%)" = totalN/totalN*100, "Percent in Death(%)" = NA)
  
  table1 <- rbind(temp, table1)
  
  table1$`Percent in Death(%)`[1:2] <- "-"
  table1 <- table1 %>%
    arrange(desc(`Percent(%)`)) %>%
    select(-cause.prediction)
  
  # Cause of death by Year
  # table2 - raw counts, table3 - percent in death, table4 - rank by year
  
  table2 <- dataTestResult %>%
    group_by(cohortStartDate, cause.prediction) %>%
    summarise(personCount = n())
  
  dataTestResult$cohortStartDate <- as.factor(dataTestResult$cohortStartDate)
  lengthYear <- length(levels(dataTestResult$cohortStartDate))
  
  personCountYearly <- dataTestResult %>%
    group_by(cohortStartDate) %>%
    summarise(personCountYearly = n())
  
  deathCountYearly <- dataTestResult[dataTestResult$cause.prediction != 0,] %>%
    group_by(cohortStartDate) %>%
    summarise(deathCountYearly = n())
  deathCountYearly <- merge(personCountYearly, deathCountYearly, by = "cohortStartDate", all.x = T)
  deathCountYearly[is.na(deathCountYearly)] <- 0
  
  temp <- data.frame(cause.prediction = c(0:8,99))
  table2 <- reshape2::dcast(table2, cause.prediction ~ cohortStartDate, value.var = "personCount")
  table2 <- merge(temp, table2, by = "cause.prediction", all.x = T)
  table2[is.na(table2)] <- 0
  table2 <- table2 %>% mutate(Total = rowSums(table2[2:length(table2)]))
  
  
  table3 <- table2 %>%
    filter(cause.prediction != 0)
  
  table2 <- merge(causeName, table2, by = "cause.prediction", all.y = T) %>% select(-cause.prediction)
  table2[11,2:length(table2)] <-table2[2:length(table2)] %>% summarise_all(funs(sum))
  levels(table2$CauseName) <- c(levels(table2$CauseName), "Total")
  table2$CauseName[11] <- "Total"
  
  for (i in 1:lengthYear){
    table3[,i+1] <- round(table3[,i+1]/rep(deathCountYearly$deathCountYearly[i], 
                                           times = length(levels(causeName$cause.prediction))-1)*100,2)  
  }
  
  table3[is.na(table3)] <- "-"
  table3 <- merge(causeName[causeName$CauseName!="No Death",], 
                  table3, by = "cause.prediction", all.x = T) %>% select(-cause.prediction)
  
  table4 <- table3

  for(i in 1:lengthYear){
    temp <- table4 %>%
      select(CauseName, levels(dataTestResult$cohortStartDate)[i])
    temp <- temp %>% arrange(desc(temp[,2]))
    table4[,i+1] <- paste0(temp[,1], " (", temp[,2],"%)")
  }
  table4 <- table4 %>%
    mutate(Rank = c(1:9)) %>% select (Rank, -CauseName, 2:lengthYear+1, -Total)

  #Age groups in 5 years
  #table 5 - row counts by age group in 5 years
  #table 6 - percent in death by age group in 5 years
  #table 7 - rank by age group in 5 year
  
  
  temp <- data.frame(cause.prediction = c(0:8,99))
  
  for(j in 1:18){
      temp2 <- dataTestResult[dataTestResult[j+4] == 1,]
      temp2 <- temp2 %>% 
        group_by(cause.prediction) %>%
        summarise(personCount = n()) %>% group_by()
      colnames(temp2)[2] <- colnames(dataTestResult[j+4])
    temp <- merge(temp, temp2, by = "cause.prediction", all.x = T)
    temp[is.na(temp)] <- 0
  }
  
  temp <- temp %>% select(cause.prediction, paste(seq(1003, 18003, by = 1000)))
  table5 <- rename(temp, "0-4"="1003", "5-9"="2003", "10-14"="3003", 
                   "15-19"="4003", "20-24"="5003", "25-29"="6003", 
                   "30-34"="7003", "35-39"="8003", "40-44"="9003", "45-49"="10003",
                   "50-54"="11003","55-59"="12003","60-64"="13003","65-69"="14003",
                   "70-74"="15003","75-79"="16003","80-84"="17003","85-89"="18003")
  
  table6 <- table5 %>% filter (cause.prediction != 0)
  table5 <- merge(causeName, table5, by = "cause.prediction", all.x = T) %>% select(-cause.prediction)
  
  deathCountAge <- colSums(table6)[2:19] 
 
   for(i in 1:18){
    table6[,i+1] <- round(table6[,i+1]/deathCountAge[i]*100 ,2)
  }
  table6 <- merge(causeName[causeName$CauseName!="No Death",],
                  table6, by = "cause.prediction", all.x = T) %>% select(-cause.prediction)
  table6[is.na(table6)] <- "-"
  
  table7 <- table6
  
  for(i in 1:18){
    temp <- table7 %>%
      select(CauseName, i+1)
    temp <- temp %>% arrange(desc(temp[,2]))
    temp[is.na(temp)] <- "-"
    table7[,i+1] <- paste0(temp[,1], " (", temp[,2],"%)")
  }
  table7 <- table7 %>% mutate(Rank = c(1:9)) %>% select (Rank, 2:19)
  
  
  #Age groups in 10 years
  #table 8 - row counts by age group in 5 years
  #table 9 - percent in death by age group in 5 years
  #table 10 - rank by age group in 5 year
  
  table8 <- data.frame(cause.prediction = c(0:8,99))
  for(i in 1:9){
    table8[i+1] <- table5[2*i]+table5[2*i+1]
    colnames(table8)[i+1] <- paste0(strsplit(colnames(table5[2*i]),"-")[[1]][1],
                                    "-", strsplit(colnames(table5[2*i+1]),"-")[[1]][2])
  }
  table8 <- table8 %>% mutate(Total = rowSums(table8[2:length(table8)]), )

  table9 <- table8 %>% filter(cause.prediction != 0)
  
  table8 <- merge(causeName, table8, by = "cause.prediction", all.y = T) %>% select(-cause.prediction)
  table8[11,2:length(table8)] <-table8[2:length(table8)] %>% summarise_all(funs(sum))
  levels(table8$CauseName) <- c(levels(table8$CauseName), "Total")
  table8$CauseName[11] <- "Total"
  
  deathCountAge10 <- colSums(table9[2:10])
  
  for(i in 1:9){
    table9[,i+1] <- round(table9[,i+1]/deathCountAge10[i]*100 ,2)
  }
  table9 <- merge(causeName[causeName$CauseName!="No Death",], table9, by = "cause.prediction", all.x = T) %>% select(-cause.prediction)
  table9[is.na(table9)] <- "-"
  
  table10 <- table9
  
  for(i in 1:9){
    temp <- table10 %>%
      select(CauseName, i+1)
    temp <- temp %>% arrange(desc(temp[,2]))
    temp[is.na(temp)] <- "-"
    table10[,i+1] <- paste0(temp[,1], " (", temp[,2],"%)")
  }
  table10 <- table10 %>% mutate(Rank = c(1:9)) %>% select (Rank, 2:10)
  

  # outDFdemographics <- rename(outDFdemographics, "ageYr"="1002", "0-4"="1003", "5-9"="2003",
  #        "10-14"="3003", "15-19"="4003", "20-24"="5003", "25-29"="6003",
  #        "30-34"="7003", "35-39"="8003", "40-44"="9003", "45-49"="10003",
  #        "50-54"="11003","55-59"="12003","60-64"="13003","65-69"="14003",
  #        "70-74"="15003","75-79"="16003","80-84"="17003","85-89"="18003",
  #        "Female"="8532001")
  
  ### Save files in saveFolder
  ParallelLogger::logInfo("Saving the results...")
  
  saveName <- "CauseOfDeathResult"
  saveName <- paste0(saveName, ".xlsx")
  savepath <- file.path(saveFolder, saveName)
  xlsx::write.xlsx(table1, file = savepath, sheetName = "total result", col.names = T, row.names = F, append = T)
  xlsx::write.xlsx(table2, file = savepath, sheetName = "CountByYear", col.names = T, row.names = F, append = T)
  xlsx::write.xlsx(table3, file = savepath, sheetName = "PercentByYear", col.names = T, row.names = F, append = T)
  xlsx::write.xlsx(table4, file = savepath, sheetName = "RankByYear", col.names = T, row.names = F, append = T)
  #xlsx::write.xlsx(table5, file = savepath, sheetName = "CountByYear", col.names = T, row.names = F, append = T)
  # xlsx::write.xlsx(table6, file = savepath, sheetName = "PercentByAge5", col.names = T, row.names = F, append = T)
  # xlsx::write.xlsx(table7, file = savepath, sheetName = "RankByAge5", col.names = T, row.names = F, append = T)
  xlsx::write.xlsx(table8, file = savepath, sheetName = "CountByAge10", col.names = T, row.names = F, append = T)
  xlsx::write.xlsx(table9, file = savepath, sheetName = "PercentByAge10", col.names = T, row.names = F, append = T)
  xlsx::write.xlsx(table10, file = savepath, sheetName = "RankByAge10", col.names = T, row.names = F, append = T)

  ParallelLogger::logInfo("Done")
  
}

