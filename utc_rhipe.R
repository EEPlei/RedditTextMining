
Sys.setenv(HADOOP="/data/hadoop")
Sys.setenv(HADOOP_HOME="/data/hadoop")
Sys.setenv(HADOOP_BIN="/data/hadoop/bin") 
Sys.setenv(HADOOP_CMD="/data/hadoop/bin/hadoop") 
Sys.setenv(HADOOP_CONF_DIR="/data/hadoop/etc/hadoop") 
Sys.setenv(HADOOP_LIBS=system("/data/hadoop/bin/hadoop classpath | tr -d '*'",TRUE))


if (!("Rhipe" %in% installed.packages()))
{
  install.packages("/data/hadoop/rhipe/Rhipe_0.75.1.6_hadoop-2.tar.gz", repos=NULL)
}

library(Rhipe)
rhinit()

## Uncomment following lines if you need non-base packages
rhoptions(zips = '/R/R.Pkg.tar.gz')
rhoptions(runner = 'sh ./R.Pkg/library/Rhipe/bin/RhipeMapReduce.sh')


#############
##COUNT UTC##
#############

user_reduce = expression(
  pre = {
    total = 0
  },
  reduce = {
    total = total + sum(unlist(reduce.values))
  },
  post = {
    rhcollect(reduce.key, total)
  }
)

user_map = expression({
  suppressMessages(library(jsonlite))
  suppressMessages(library(stringr))
  
  time_converter = function(time){
    #below code gives me the numeric version of each utcs
    newtime = lapply(time, as.numeric)
    #below code applies as.POSIXct function to the newtime dataframe
    stdtime = lapply(newtime, as.POSIXct,origin = '1970-01-01')
    #below code replaces the minutes and seconds with 00:00 so its easy to aggregate
    stacked_time = lapply(stdtime, str_replace,":[0-5][0-9]:[0-5][0-9]", ":00:00")
    #below codes converts back the time to utc

    back_to_utc = lapply(stacked_time, as.POSIXct)
    converted_utc = sapply(back_to_utc, as.numeric)
    
    
    return(converted_utc)
  }
  lapply(
    seq_along(map.keys), 
    function(r) 
    {
      time = fromJSON(map.values[[r]])$created_utc
      key = time_converter(time)
      value = 1
      lapply(key, rhcollect, value=1)
    }
  )
})

get_val = function(x,i) x[[i]]

MapReduce <- function(file){
  user = rhwatch(
    map      = user_map,
    reduce   = user_reduce,
    input    = rhfmt(file, type = "text")
  )
  counts = data.frame(key = sapply(user,get_val,i=1),
                      value = sapply(user,get_val,i=2), 
                      stringsAsFactors=FALSE)
  return(counts)
}

jan_utc <- MapReduce("/data/RC_2015-01.json")
save(jan_utc, file = "jan_utc.Rdata")

#############
##GILDED UTC##
#############

user_reduce = expression(
  pre = {
    total = 0
  },
  reduce = {
    total = total + sum(unlist(reduce.values))
  },
  post = {
    rhcollect(reduce.key, total)
  }
)

user_map = expression({
  suppressMessages(library(jsonlite))
  suppressMessages(library(stringr))
  
  time_converter = function(time){
    #below code gives me the numeric version of each utcs
    newtime = lapply(time, as.numeric)
    #below code applies as.POSIXct function to the newtime dataframe
    stdtime = lapply(newtime, as.POSIXct,origin = '1970-01-01')
    #below code replaces the minutes and seconds with 00:00 so its easy to aggregate
    stacked_time = lapply(stdtime, str_replace,":[0-5][0-9]:[0-5][0-9]", ":00:00")
    #below codes converts back the time to utc
    
    back_to_utc = lapply(stacked_time, as.POSIXct)
    converted_utc = sapply(back_to_utc, as.numeric)

    return(converted_utc)
  }
  gold <- lapply(
    seq_along(map.keys), 
    function(r) 
    {
      gilded = fromJSON(map.values[[r]])$gilded
    }
  ) 
  gindex <- lapply(gold, as.numeric)
  gindex <- which(gindex == 1)
  map.keys <- map.keys[gindex]
  lapply(
    seq_along(map.keys), 
    function(r) 
    {
      time = fromJSON(map.values[[r]])$created_utc
      key = time_converter(time)
      value = 1
      lapply(key, rhcollect, value=1)
    }
  )
})

get_val = function(x,i) x[[i]]

MapReduce <- function(file){
  user = rhwatch(
    map      = user_map,
    reduce   = user_reduce,
    input    = rhfmt(file, type = "text")
  )
  counts = data.frame(key = sapply(user,get_val,i=1),
                      value = sapply(user,get_val,i=2), 
                      stringsAsFactors=FALSE)
  return(counts)
}
jan_gilded <- MapReduce("/data/RC_2015-01.json")
save(jan_gilded, file = "jan_gild.Rdata")
