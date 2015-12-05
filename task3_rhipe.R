### Initialization of Rhipe and Hadoop

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


### Word Count Example


wc_reduce = expression(
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

wc_map = expression({
  suppressMessages(library(stringr))
  suppressMessages(library(jsonlite))
  suppressMessages(library(NLP))
  suppressMessages(library(tm))
  x <- stopwords("en")
  y <- stopwords("SMART")
  sw <- union(x,y)
  lapply(
    seq_along(map.keys), 
    function(r) 
    {
      time = fromJSON(map.values[[r]])$created_utc
      new.time = as.POSIXct(as.numeric(time), origin='1970-01-01')
      strs = strsplit(toString(new.time), " ")
      strs2 = strsplit(strs[[1]][1], "-")
      date = strs2[[1]][3]
      if(date == "14"){
        line = tolower(fromJSON(map.values[[r]])$body)
        line = gsub("[-—]"," ",line)
        line = gsub("[^'`’[:alpha:][:space:]]","",line,perl=TRUE)
        line = gsub("(^\\s+|\\s+$)","",line)
        line = strsplit(line, "\\s+")[[1]]
        line = line[line != ""]
        line <- setdiff(line, sw)
        lapply(line, rhcollect, value=1)
      }
    }     
  )
})


#wc = rhwatch(
#  map      = wc_map,
#  reduce   = wc_reduce,
#  input    = rhfmt("/data/RC_2015-02.json", type = "text")
#)


get_val = function(x,i) x[[i]]

MapReduce <- function(file){
  user = rhwatch(
    map      = wc_map,
    reduce   = wc_reduce,
    input    = rhfmt(file, type = "text")
  )
  counts = data.frame(key = sapply(user,get_val,i=1),
                      value = sapply(user,get_val,i=2), 
                      stringsAsFactors=FALSE)
  counts
}

files <- c(
  "/data/RC_2015-01.json",
  "/data/RC_2015-02.json",
  "/data/RC_2015-03.json"
)

valentine_data <- lapply(files,MapReduce)
# 
# 
# user = rhwatch(
#   map      = user_map,
#   reduce   = user_reduce,
#   input    = rhfmt("/data/RC_2015-01.json", type = "text")
# )


data1 <- valentine_data[[1]]
Jan_14 <- data1[order(data1$value,decreasing = TRUE),]
save(Jan_14,file = "Jan_14.Rdata")

data2 <- valentine_data[[2]]
Feb_14 <- data2[order(data2$value,decreasing = TRUE),]
save(Feb_14,file = "Feb_14.Rdata")

data3 <- valentine_data[[3]]
Mar_14 <- data3[order(data3$value,decreasing = TRUE),]
save(Mar_14,file = "Mar_14.Rdata")

