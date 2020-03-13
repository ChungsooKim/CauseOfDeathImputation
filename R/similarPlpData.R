#' Extract new plpData using plpModel settings
#' use metadata in plpModel to extract similar data and population for new databases:
#'
#' @param plpModel         The trained PatientLevelPrediction model or object returned by runPlp()
#' @param createCohorts          Create the tables for the target and outcome - requires sql in the plpModel object
#' @param newConnectionDetails      The connectionDetails for the new database
#' @param newCdmDatabaseSchema      The database schema for the new CDM database 
#' @param newCohortDatabaseSchema   The database schema where the cohort table is stored
#' @param newCohortTable            The table name of the cohort table
#' @param newCohortId               The cohort_definition_id for the cohort of at risk people
#' @param newOutcomeDatabaseSchema  The database schema where the outcome table is stored
#' @param newOutcomeTable           The table name of the outcome table
#' @param newOutcomeId              The cohort_definition_id for the outcome  
#' @param newOracleTempSchema       The temp coracle schema
#' @param sample                    The number of people to sample (default is NULL meaning use all data)
#' @param createPopulation          Whether to create the study population as well
#'
#' @examples
#' \dontrun{
#' # set the connection
#' connectionDetails <- DatabaseConnector::createConnectionDetails()
#'    
#' # load the model and data
#' plpModel <- loadPlpModel("C:/plpmodel")
#'
#' # extract the new data in the 'newData.dbo' schema using the model settings 
#' newDataList <- similarPlpData(plpModel=plpModel, 
#'                               newConnectionDetails = connectionDetails,
#'                               newCdmDatabaseSchema = 'newData.dbo',
#'                               newCohortDatabaseSchema = 'newData.dbo',   
#'                               newCohortTable = 'cohort', 
#'                               newCohortId = 1, 
#'                               newOutcomeDatabaseSchema = 'newData.dbo', 
#'                               newOutcomeTable = 'outcome',     
#'                               newOutcomeId = 2)    
#'                
#' # get the prediction:
#' prediction <- applyModel(newDataList$population, newDataList$plpData, plpModel)$prediction
#' }
#' @export

similarPlpData <- function (plpModel = NULL, createCohorts = T, newConnectionDetails, 
          newCdmDatabaseSchema = NULL, newCohortDatabaseSchema = NULL, 
          newCohortTable = NULL, newCohortId = NULL, newOutcomeDatabaseSchema = NULL, 
          newOutcomeTable = NULL, newOutcomeId = NULL, newOracleTempSchema = newCdmDatabaseSchema, 
          sample = NULL, createPopulation = T) 
{
  if (length(ParallelLogger::getLoggers()) == 0) {
    logger <- ParallelLogger::createLogger(name = "SIMPLE", 
                                           threshold = "INFO", appenders = list(ParallelLogger::createConsoleAppender(layout = ParallelLogger::layoutTimestamp)))
    ParallelLogger::registerLogger(logger)
  }
  if (is.null(plpModel)) 
    return(NULL)
  if (class(plpModel) != "plpModel" && class(plpModel) != 
      "runPlp") 
    return(NULL)
  if (class(plpModel) == "runPlp") 
    plpModel <- plpModel$model
  if (missing(newConnectionDetails)) {
    stop("connection details not entered")
  }else{
    connection <- DatabaseConnector::connect(newConnectionDetails)
  }
  if (createCohorts) {
    if (is.null(plpModel$metaData$cohortCreate$targetCohort$sql)) 
      stop("No target cohort code")
    if (is.null(plpModel$metaData$cohortCreate$outcomeCohorts[[1]]$sql)) 
      stop("No outcome cohort code")
    exists <- toupper(newCohortTable) %in% DatabaseConnector::getTableNames(connection, 
                                                                            newCohortDatabaseSchema)
    if (!exists) {
      ParallelLogger::logTrace("Creating temp cohort table")
      sql <- "create table @target_cohort_schema.@target_cohort_table(cohort_definition_id bigint, subject_id bigint, cohort_start_date datetime, cohort_end_date datetime)"
      sql <- SqlRender::translateSql(sql, targetDialect = connectionDetails$dbms)$sql
      sql <- SqlRender::renderSql(sql, target_cohort_schema = newCohortDatabaseSchema, 
                                  target_cohort_table = newCohortTable)$sql
      tryCatch(DatabaseConnector::executeSql(connection, 
                                             sql), error = stop, finally = ParallelLogger::logTrace("Cohort table created"))
    }
    exists <- toupper(newOutcomeTable) %in% DatabaseConnector::getTableNames(connection, 
                                                                             newOutcomeDatabaseSchema)
    if (!exists) {
      sql <- "create table @target_cohort_schema.@target_cohort_table(cohort_definition_id bigint, subject_id bigint, cohort_start_date datetime, cohort_end_date datetime)"
      sql <- SqlRender::translateSql(sql, targetDialect = connectionDetails$dbms)$sql
      sql <- SqlRender::renderSql(sql, target_cohort_schema = newOutcomeDatabaseSchema, 
                                  target_cohort_table = newOutcomeTable)$sql
      tryCatch(DatabaseConnector::executeSql(connection, 
                                             sql), error = stop, finally = ParallelLogger::logTrace("outcome table created"))
    }
    ParallelLogger::logTrace("Populating cohort tables")
    targetSql <- plpModel$metaData$cohortCreate$targetCohort$sql
    targetSql <- SqlRender::renderSql(targetSql, cdm_database_schema = ifelse(is.null(newCdmDatabaseSchema), 
                                                                              plpModel$metaData$call$cdmDatabaseSchema, newCdmDatabaseSchema), 
                                      target_database_schema = ifelse(is.null(newCohortDatabaseSchema), 
                                                                      plpModel$metaData$call$cdmDatabaseSchema, newCohortDatabaseSchema), 
                                      target_cohort_table = ifelse(is.null(newCohortTable), 
                                                                   plpModel$metaData$call$newCohortTable, newCohortTable), 
                                      target_cohort_id = ifelse(is.null(newCohortId), plpModel$metaData$call$cohortId, 
                                                                newCohortId))$sql
    targetSql <- SqlRender::translateSql(targetSql, targetDialect = ifelse(is.null(newConnectionDetails$dbms), 
                                                                           "pdw", newConnectionDetails$dbms))$sql
    DatabaseConnector::executeSql(connection, targetSql)
    for (outcomesql in plpModel$metaData$cohortCreate$outcomeCohorts) {
      outcomeSql <- outcomesql$sql
      outcomeSql <- SqlRender::renderSql(outcomeSql, cdm_database_schema = ifelse(is.null(newCdmDatabaseSchema), 
                                                                                  plpModel$metaData$call$cdmDatabaseSchema, newCdmDatabaseSchema), 
                                         target_database_schema = ifelse(is.null(newOutcomeDatabaseSchema), 
                                                                         plpModel$metaData$call$cdmDatabaseSchema, newOutcomeDatabaseSchema), 
                                         target_cohort_table = ifelse(is.null(newOutcomeTable), 
                                                                      plpModel$metaData$call$newOutcomeTable, newOutcomeTable), 
                                         target_cohort_id = ifelse(is.null(newOutcomeId), 
                                                                   plpModel$metaData$call$outcomeId, newOutcomeId))$sql
      outcomeSql <- SqlRender::translateSql(outcomeSql, 
                                            targetDialect = ifelse(is.null(newConnectionDetails$dbms), 
                                                                   "pdw", newConnectionDetails$dbms))$sql
      DatabaseConnector::executeSql(connection, outcomeSql)
    }
  }
  ParallelLogger::logTrace("Loading model data extraction settings")
  dataOptions <- as.list(plpModel$metaData$call)
  dataOptions[[1]] <- NULL
  dataOptions$sampleSize <- sample
  dataOptions$covariateSettings$includedCovariateIds <- plpModel$varImp$covariateId[plpModel$varImp$covariateValue != 0]
  ParallelLogger::logTrace("Adding new settings if set...")
  if (is.null(newCdmDatabaseSchema)) 
    return(NULL)
  dataOptions$cdmDatabaseSchema <- newCdmDatabaseSchema
  if (!is.null(newConnectionDetails)) 
    dataOptions$connectionDetails <- newConnectionDetails
  if (!is.null(newCohortId)) 
    dataOptions$cohortId <- newCohortId
  if (!is.null(newOutcomeId)) 
    dataOptions$outcomeIds <- newOutcomeId
  if (!is.null(newCohortDatabaseSchema)) 
    dataOptions$cohortDatabaseSchema <- newCohortDatabaseSchema
  if (!is.null(newCohortTable)) 
    dataOptions$cohortTable <- newCohortTable
  if (!is.null(newOutcomeDatabaseSchema)) 
    dataOptions$outcomeDatabaseSchema <- newOutcomeDatabaseSchema
  if (!is.null(newOutcomeTable)) 
    dataOptions$outcomeTable <- newOutcomeTable
  if (!is.null(newOracleTempSchema)) 
    dataOptions$oracleTempSchema <- newOracleTempSchema
  dataOptions$baseUrl <- NULL
  plpData <- do.call(CauseOfDeathImputation::getPlpData, dataOptions)
  if (!createPopulation) 
    return(plpData)
  ParallelLogger::logTrace("Loading model population settings")
  popOptions <- plpModel$populationSettings
  popOptions$cohortId <- dataOptions$cohortId
  popOptions$outcomeId <- dataOptions$outcomeIds
  popOptions$plpData <- plpData
  population <- do.call(CauseOfDeathImputation::createStudyPopulation, 
                        popOptions)
  ParallelLogger::logTrace("Returning population and plpData for new data using model settings")
  return(list(population = population, plpData = plpData))
}
