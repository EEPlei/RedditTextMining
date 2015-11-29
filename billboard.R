
library(dplyr)
library(magrittr)

load("~/Sta523/Team4_hw5/Jan.Rdata")
load("~/Sta523/Team4_hw5/Feb.Rdata")
load("~/Sta523/Team4_hw5/Mar.Rdata")
load("~/Sta523/Team4_hw5/Apr.Rdata")
load("~/Sta523/Team4_hw5/May.Rdata")

mnth <- list(Jan = Jan, Feb = Feb, Mar = Mar, Apr = Apr, May = May)

example <- function(x){
  x25 <- x[1:25,]
  return(x25)
}

bb <- function(x,y){
  x25 <- x[1:25,]
  # top 25 subreddit for x month
  x25y <- y %>% filter(key %in% x25$key)
  # x top 25 subreddit from month y 
  x25y <- x25y[match(x25$key, x25y$key),]
  # reorder y to match x 
  per.change <- (x25$value - x25y$value)/x25y$value
  per.change <- sprintf("%1.2f%%", 100*per.change)
  x25$PercentageChange <- per.change
  # percent change from y to x 
  colnames(x25) <- c("SubReddit", "Frequency", "%Change")
  return(x25)
}

billboard <- list(January = Jan[1:25,], 
                  February = bb(Feb, Jan), 
                  March = bb(Mar, Feb), 
                  April = bb(Apr, Mar),
                  May = bb(May, Apr))

