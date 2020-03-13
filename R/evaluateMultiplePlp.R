# @file RunMultiplePlp.R
#
# Copyright 2019 Observational Health Data Sciences and Informatics
#
# This file is part of PatientLevelPrediction
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' externally validate the multiple plp models across new datasets
#' @description
#' This function loads all the models in a multiple plp analysis folder and
#' validates the models on new data
#' @details
#' Users need to input a location where the results of the multiple plp analyses
#' are found and the connection and database settings for the new data
#' 
#' @param analysesLocation                The location where the multiple plp analyses are
#' @param outputLocation                The location to save to validation results
#' @param connectionDetails                The connection details for extracting the new data 
#' @param validationSchemaTarget         A string or list of strings specifying the database containing the target cohorts
#' @param validationSchemaOutcome       A string or list of strings specifying the database containing the outcome cohorts
#' @param validationSchemaCdm            A string or list of strings specifying the database containing the cdm
#' @param databaseNames                  A string of lift of strings specifying sharing friendly database names corresponding to validationSchemaCdm
#' @param validationTableTarget          A string or list of strings specifying the table containing the target cohorts
#' @param validationTableOutcome        A string or list of strings specifying the table containing the outcome cohorts
#' @param validationIdTarget             An iteger or list of integers specifying the cohort id for the target cohorts
#' @param validationIdOutcome           An iteger or list of integers specifying the cohort id for the outcome cohorts
#' @param oracleTempSchema                 The temp oracle schema requires read/write 
#' @param verbosity                        Sets the level of the verbosity. If the log level is at or higher in priority than the logger threshold, a message will print. The levels are:
#'                                         \itemize{
#'                                         \item{DEBUG}{Highest verbosity showing all debug statements}
#'                                         \item{TRACE}{Showing information about start and end of steps}
#'                                         \item{INFO}{Show informative information (Default)}
#'                                         \item{WARN}{Show warning messages}
#'                                         \item{ERROR}{Show error messages}
#'                                         \item{FATAL}{Be silent except for fatal errors}
#'                                         }
#' @param keepPrediction                   Whether to keep the predicitons for the new data                                         
#' @param sampleSize                       If not NULL, the number of people to sample from the target cohort
#' 
#' @export 
evaluateMultiplePlp <- function(analysesLocation,
                                outputLocation,
                                connectionDetails, 
                                validationSchemaTarget,
                                validationSchemaOutcome,
                                validationSchemaCdm, 
                                databaseNames,
                                validationTableTarget,
                                validationTableOutcome,
                                validationIdTarget = NULL,
                                validationIdOutcome = NULL,
                                oracleTempSchema = NULL,
                                verbosity = 'INFO',
                                keepPrediction = F,
                                sampleSize = NULL){
  
  clearLoggerType <- function(type='PLP log'){
    logs <- ParallelLogger::getLoggers()
    logNames <- unlist(lapply(logs, function(x) x$name))
    ind <- which(logNames==type)
    
    for(i in ind){
      ParallelLogger::unregisterLogger(logNames[i])
    }
    
    return(NULL)
  }
  
  clearLoggerType("Multple Evaluate PLP Log")
  if(!dir.exists(outputLocation)){dir.create(outputLocation,recursive=T)}
  logFileName = paste0(outputLocation,'/plplog.txt')
  logger <- ParallelLogger::createLogger(name = "Multple Evaluate PLP Log",
                                      threshold = verbosity,
                                      appenders = list(ParallelLogger::createFileAppender(layout = ParallelLogger::layoutParallel,
                                                                                       fileName = logFileName)))
  ParallelLogger::registerLogger(logger)
  
  if(missing(databaseNames)){
    stop('Need to put a shareable name/s for the database/s')
  }

  # for each model run externalValidatePlp()
  modelSettings <- dir(analysesLocation, recursive = F, full.names = T)
  
  # now fine all analysis folders..
  modelSettings <- modelSettings[grep('Analysis_',modelSettings)]
  
  for(i in 1:length(modelSettings)){
    
    ParallelLogger::logInfo(paste0('Evaluating model in ',modelSettings[i] ))
    
    if(dir.exists(file.path(modelSettings[i],'plpResult'))){
      ParallelLogger::logInfo(paste0('plpResult found in ',modelSettings[i] ))
      
      plpResult <- CauseOfDeathImputation::loadPlpResult(file.path(modelSettings[i],'plpResult'))
      
      validations <-   tryCatch(CauseOfDeathImputation::externalValidatePlp(plpResult = plpResult,
                                                    connectionDetails = connectionDetails, 
                                                    validationSchemaTarget = validationSchemaTarget,
                                                    validationSchemaOutcome = validationSchemaOutcome, 
                                                    validationSchemaCdm = validationSchemaCdm, 
                                                    databaseNames = databaseNames,
                                                    validationTableTarget = validationTableTarget, 
                                                    validationTableOutcome = validationTableOutcome,
                                                    validationIdTarget = validationIdTarget, 
                                                    validationIdOutcome = validationIdOutcome,
                                                    oracleTempSchema = oracleTempSchema,
                                                    verbosity = verbosity, 
                                                    keepPrediction = keepPrediction,
                                                    sampleSize=sampleSize),
                                error = function(cont){ParallelLogger::logInfo(paste0('Error: ',cont ))
                                  ;})
      
      if(!is.null(validations)){
        if(length(validations$validation)>1){
          for(j in 1:length(validations$validation)){
            saveName <- file.path(outputLocation, databaseNames[j], paste0(plpResult$analysisRef$analysisId))
            if(!dir.exists(saveName)){
              dir.create(saveName, recursive = T)
            }
            ParallelLogger::logInfo(paste0('Evaluation result save in ',file.path(saveName,'validationResult.rds') ))
            
            saveRDS(validations$validation[[j]], file.path(saveName,'validationResult.rds'))
            
          }
        } else {
          saveName <- file.path(outputLocation, databaseNames,paste0(plpResult$analysisRef$analysisId))
          if(!dir.exists(saveName)){
            dir.create(saveName, recursive = T)
          }
          ParallelLogger::logInfo(paste0('Evaluation result save in ',file.path(saveName,'validationResult.rds') ))
          saveRDS(validations$validation[[1]], file.path(saveName,'validationResult.rds'))
        }
      }
    }
  }
  
}