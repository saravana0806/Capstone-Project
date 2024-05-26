#Set up workspace
setwd('C:/Users/saravana.ayyappa/Desktop/Capstone-master')
getwd()

#Import libraries
rm(list = ls())

library(iterators)
library(scales)
library(forecast)
library(parallel)
library(foreach)
library(doParallel)
library(tseries)
library(MASS)
library(RODBC)
library(glmnet)
library(dlm)
library(fpp)
library(rJava)
library(xlsx)
library(WriteXLS)
library(bindr)
library(glue)
library(dplyr)
library(ggplot2)

#User Defined Inputs - Specify all parameters that you need to specify here
#Specify the name of the file with relevant data (in CSV format)
input_csv_file <- 'LE_Sales.xlsx'

#Names of variables you want to use to generate the model
flags <- c('New_Year','Republic_Day','Holi','Good_Friday','Labour_Day','Christmas'
           ,'Independence_Day','Vinayaka_Chaturthi','Dussehra','Diwali')

marcoms <- c('Newspaper','Magazines')

pricing <- c('Price_Offline')

model_vars <- c('Fiscal_Week','Calls',pricing, marcoms,flags)
model_vars

first_week <-'FY15 Q1 W01'
last_week <- 'FY19 Q2 W13'

step_count <- 1
min_train_weeks <- 221

#Specify the output file (Excel) name
filename <- 'LE_Calls'

######################################################################################################################
#################################       Data Processing & EDA     ###########################################################
######################################################################################################################

#Import Data
input_df <- read.xlsx(input_csv_file,header = T,stringsAsFactors = F,sheetIndex = 1)
input_df <- input_df[,names(input_df) %in% model_vars] #Keep only desired columns
input_df <- input_df[which(input_df[,1]== first_week):which(input_df[,1]== last_week),] #Slice data according to weeks to be considered


#Ensure all columns that are supposed to be numeric are numeric
input_df[,2] <- sapply(input_df[,2], as.numeric)
input_df[,names(input_df) %in% pricing] <- sapply(input_df[,names(input_df) %in% pricing],as.numeric)
input_df[,names(input_df) %in% marcoms] <- sapply(input_df[,names(input_df) %in% marcoms],as.numeric)
input_df[,names(input_df) %in% flags] <- sapply(input_df[,names(input_df) %in% flags],as.numeric)

varcount <- as.numeric(ncol(input_df))

#Feature Engineering

input_df$Price_Offline <- ifelse(input_df$Price_Offline > 0 , log(input_df$Price_Offline),0)
#input_df$Margin_Offline <- ifelse(input_df$Margin_Offline > 0 , log(input_df$Margin_Offline),0)

varcount <- as.numeric(ncol(input_df)) #update varcount

######################################################################################################################
#################################       Function Definitions    ###########################################################
######################################################################################################################


IDMValidate <- function (data, yCol, covCol, PDQpdq, trainStart, stepSize,steps, freq, boxCox, cores) {
  orders <- unname(as.list(as.data.frame(t(PDQpdq[,1:3]))))
  seasons <- unname(as.list(as.data.frame(t(PDQpdq[,4:6]))))
  multArima <- function (N, ord, seas, series, covX) {
    orderTemp <- unlist(orders[N])
    seasonTemp <- unlist(seasons[N])
    tempModel <- tryCatch(Arima(series,orderTemp, seasonal = seasonTemp, 
                                xreg = covX),error=function(e)NULL)
    return(tempModel)
  }
  
  mapeFrame <- PDQpdq
  fullArima <- list(list())
  fcArima <- list(list())
  mapeList <- list(list())
  mapeListList <- list(list())
  IDMV <- list()
  for (i in 1:steps)
  {
    stepAdj <- (as.numeric(i) == steps)* 13
    trainCount <- trainStart + stepSize*(i-1)
    trainTS <- ts(BoxCox(data[1:trainCount,yCol], lambda = boxCox), frequency = freq)
    #trainTS <- ts(BoxCox(log(data[1:trainCount,yCol]), lambda = boxCox), frequency = freq)
    testTS <- data[(trainCount+1):(trainCount + 2*stepSize - stepAdj),yCol]
    temp_df <- data[1:trainCount,]## modified original - objective is to remove cols with all 0s for the given number of rows
    cols_with_variation <- names(temp_df[,colSums(temp_df != 0)>0])## modified original - objective is to remove cols with all 0s for the given number of rows
    scaleCov <- data.frame(scale(data[1:(trainStart+stepSize*steps),unlist(covCol[i])]))## modified original - no need to manually map covCols depending on the dataset
    covTrain <- as.matrix(scaleCov[1:trainCount,names(scaleCov) %in% cols_with_variation]) ## modified original - objective is to remove cols with all 0s for the given number of rows
    covTest <- as.matrix(scaleCov[(trainCount +1):(trainCount + 2*stepSize - stepAdj), names(scaleCov) %in% cols_with_variation])##
    cluster <- makeCluster(cores)
    registerDoParallel(cluster)
    fullArima[[i]] <- foreach(N = 1:length(orders), .packages = 'forecast') %dopar% 
    multArima(N,orders,seasons, trainTS, covTrain)
    stopCluster(cluster)
    
    fcArima[[i]] <- Map(function(model)tryCatch(as.numeric((forecast(model, h = (2*stepSize - stepAdj),
                                                                     xreg = covTest)$mean*boxCox+1)^(1/boxCox)), error=function(e)NULL), fullArima[[i]])
    
    # fcArima[[i]] <- Map(function(model)tryCatch(as.numeric(exp((forecast(model, h = (2*stepSize - stepAdj),
    #                                                                  xreg = covTest)$mean*boxCox+1)^(1/boxCox))), error=function(e)NULL), fullArima[[i]])
    
    mapeList[[i]] <- Map(function(fc)tryCatch(mean(abs(fc[(1 + stepSize - stepAdj):(length(fc))]-
                                                         testTS[(1 + stepSize - stepAdj):(length(testTS))])/testTS[(1 + stepSize - stepAdj):(length(testTS))]), 
                                                         error=function(e)NULL),fcArima[[i]])
    
    mapeListList[[i]] <- Map(function(fc)tryCatch((fc[(1 + stepSize - stepAdj):(length(fc))]-
                                                     testTS[(1 + stepSize - stepAdj):(length(testTS))])/testTS[(1 + stepSize - stepAdj):(length(testTS))], 
                                                  error=function(e)NULL),fcArima[[i]])
    mapeFrame <- data.frame(cbind(mapeFrame, unlist(mapeList[[i]])))
  }
  IDMV$results <- tryCatch(data.frame(cbind(mapeFrame, do.call(rbind, mapeListList[[steps]]))), error=function(e)mapeFrame)
  IDMV$timeseries <- trainTS
  IDMV$xreg <- covTrain
  IDMV$models <- fullArima[[steps]]
  IDMV$data <- data
  IDMV$yCol <- yCol
  IDMV$covCol <- covCol
  IDMV$PDQpdq <- PDQpdq
  IDMV$trainStart <- trainStart
  IDMV$stepSize <- stepSize
  IDMV$steps <- steps
  IDMV$freq <- freq
  IDMV$boxCox <- boxCox
  IDMV$forecasts <- fcArima[[steps]]
  IDMV$mapes <- mapeListList[[steps]]
  IDMV
}


IDMExamine <- function (index, iteration) {
  bestModelInError <- (((fitted(iteration$models[[index]])*iteration$boxCox+1)^(1/iteration$boxCox)) - 
                         iteration$data[1:(iteration$trainStart +(iteration$stepSize*(iteration$steps-1))),iteration$yCol]) /  
    iteration$data[1:(iteration$trainStart +(iteration$stepSize*(iteration$steps-1))),iteration$yCol]
  bestModelError <- c(bestModelInError,iteration$mapes[[index]])
  bestForecasts <- c((fitted(iteration$models[[index]])*iteration$boxCox+1)^(1/iteration$boxCox), iteration$forecasts[[index]])
  ModelExamine <- cbind(iteration$data[1:(iteration$trainStart +(iteration$stepSize*iteration$steps)),iteration$yCol]
                        , cbind(bestForecasts, bestModelError) ,
                        iteration$data[1:(iteration$trainStart +
                                            (iteration$stepSize*iteration$steps)),(iteration$covCol[[iteration$steps]])])  
  ModelExamine
}

######################################################################################################################
#################################       Model Build    ###########################################################
######################################################################################################################


#Train Forecast Model
train_model <- IDMValidate(data = input_df,
                                          yCol = 2,
                                          covCol = list(c(3:varcount)),
                                          PDQpdq =  rbind(expand.grid(c(0:3), c(0:2), c(0:3), 0, 0, 0)),
                                          trainStart =  min_train_weeks,
                                          stepSize =  13,
                                          steps =  step_count,
                                          freq =  52,
                                          boxCox = 0.02,
                                          cores = 4)


#Create the MAPE data frame
mapes <- train_model$results
#When step_count = 1, calculating rowMeans() does not make sense
if(step_count == 1){
  mapes<-cbind(Mean_of_Maps = abs(mapes[,c(7:(step_count+6))]),mapes)
} else {
  mapes<-cbind(Mean_of_Maps = rowMeans(abs(mapes[,c(7:(step_count+6))])),mapes)
}
mapes<-cbind(index=1:nrow(mapes),mapes)
mapes<-mapes[order(mapes$Mean_of_Maps),]
mapes<-cbind(PDQ = paste(mapes$Var1,",",mapes$Var2,",",mapes$Var3),mapes)

######################################################################################################################
#################################     Result Data Frames    ###########################################################
######################################################################################################################


#Create Co-efficients Sheet
pdq <- c('SerialNo')
index <- c(0)
mean_of_map <- c(0)

vars <- colnames(input_df)
coeff_all_Data <- data.frame(SerialNo = 1:length(vars))
rownames(coeff_all_Data) <- vars

for (i in 1:48)
{
  x <- data.frame(train_model$models[[mapes[i,'index']]][1])
  pdq <- c(pdq,as.character(mapes[i,'PDQ']))
  index <- c(index,format(mapes[i,'index'],digits = 2, nsmall = 0))
  mean_of_map <- c(mean_of_map,format(mapes[i,'Mean_of_Maps'],digits = 2,nsmall = 4))
  
  coeff_all_Data <- cbind(coeff_all_Data,x[,"coef"][match(rownames(coeff_all_Data),rownames(x))])
}


rowname <- rownames(coeff_all_Data)
rowname <- c("index","mean_of_map",rowname)
coeff_all_Data <- rbind(index,mean_of_map,coeff_all_Data)
rownames(coeff_all_Data) <- rowname
colnames(coeff_all_Data) <- pdq


###############################################################################################################

# Create Examine Sheet
 examine_df <- IDMExamine(index = mapes[1,'index'] ,iteration =  train_model)
 examine_df <- examine_df[,c(1,2)]

 for (i in 2:48)
 {
   temp_examine_df <- IDMExamine(index = mapes[i,'index'] ,iteration =  train_model)
   temp_examine_df <- temp_examine_df[,c(2)]
   examine_df  <- cbind(examine_df,temp_examine_df)
 }

 forecast_Examine_week_Name <- input_df[1:nrow(examine_df),1]
 examine_df  <- cbind(forecast_Examine_week_Name,examine_df)

 pdq <- pdq[2:49]
 examine_colnames <- c("Week","Actuals",pdq)
 colnames(examine_df)<- examine_colnames

######################################################################################################################
#################################       Write Results to Excel      ###########################################################
######################################################################################################################

write.xlsx(input_df, paste(filename,".xlsx"), sheetName="Input_Sheet", 
           col.names=TRUE, row.names=FALSE, append=TRUE)
write.xlsx(mapes, paste(filename,".xlsx"), sheetName="Mapes", 
           col.names=TRUE, row.names=FALSE, append=TRUE)
write.xlsx(coeff_all_Data, paste(filename,".xlsx"), sheetName="coeff", 
           col.names=TRUE, row.names=TRUE, append=TRUE)
write.xlsx(examine_df, paste(filename,".xlsx"), sheetName="examine", 
           col.names=TRUE, row.names=TRUE, append=TRUE)

wb <- loadWorkbook(paste(filename,".xlsx"))

sheets <- getSheets(wb)
autoSizeColumn(sheets[[1]], colIndex=1:100)
autoSizeColumn(sheets[[2]], colIndex=1:100)
autoSizeColumn(sheets[[3]], colIndex=1:100)
autoSizeColumn(sheets[[4]], colIndex=1:100)
createFreezePane(sheets[[1]], 2, 2,2,2)
createFreezePane(sheets[[2]], 2, 2,2,2)
createFreezePane(sheets[[3]], 2, 2,2,2)
createFreezePane(sheets[[4]], 2, 2,2,2)
saveWorkbook(wb,paste(filename,".xlsx"))


####################################################
#
