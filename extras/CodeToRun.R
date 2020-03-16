library(CauseOfDeathImputation)

# USER INPUTS
#=======================
# The folder where the study intermediate and result files will be written:
outputFolder <- "./ImputationResult"

# Specify where the temporary files (used by the ff package) will be created:
options(fftempdir = "location with space to save big data")

# Details for connecting to the server:
dbms <- "you dbms"
user <- 'your username'
pw <- 'your password'
server <- 'your server'
port <- 'your port'

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                server = server,
                                                                user = user,
                                                                password = pw,
                                                                port = port)

# Add the database containing the OMOP CDM data
cdmDatabaseSchema <- 'cdm database schema'
databaseName <- 'cdm database name'

# Add a database with read/write access as this is where the cohorts will be generated
cohortDatabaseSchema <- 'work database schema'

oracleTempSchema <- NULL

# table name where the cohorts will be generated
cohortTable <- 'ValidationCohort'

#=======================

execute(connectionDetails,
        databaseName,
        cdmDatabaseSchema,
        cohortDatabaseSchema,
        oracleTempSchema,
        cohortTable,
        outputFolder,
        createCohorts = T,
        runValidation = T,
        createImputationData = T,
        causeImputation = T,
        packageResults = F,
        minCellCount = 1,
        sampleSize = NULL)

#=======================

# Please send me (ted9219@ajou.ac.kr) the result file. 
# ("~/outputFolder/CauseOfDeathImputation/CauseOfDeathResult.xlsx")

