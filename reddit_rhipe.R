# ("/data/Sta523/reddit/short_1e3.json")
### Initialization of Rhipe and Hadoop


install.packages("testthat")
install.packages("rJava")

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



## Uncomment following lines if you need non-base packages
rhoptions(zips = '/R/R.Pkg.tar.gz')
rhoptions(runner = 'sh ./R.Pkg/library/Rhipe/bin/RhipeMapReduce.sh')


library(Rhipe)
rhinit()

### put a file to HDFS so we can use it
#rhput("/data/Shakespeare/hamlet.txt","/data/")

#rhls("/data")
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
  
  lapply(
    seq_along(map.keys), 
    function(r) 
    {
      key = fromJSON(map.values[[r]])$subreddit
      value = 1
      rhcollect(key,value)
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
  counts = data.frame(key = sapply(user,get_val,i=1),value = sapply(user,get_val,i=2), stringsAsFactors=FALSE)
  counts
}

files <- c(
  "/data/RC_2015-01.json",
  "/data/RC_2015-02.json",
  "/data/RC_2015-03.json",
  "/data/RC_2015-04.json",
  "/data/RC_2015-05.json"
  )

monthly_subreddits <- lapply(files,MapReduce)
# 
# 
# user = rhwatch(
#   map      = user_map,
#   reduce   = user_reduce,
#   input    = rhfmt("/data/RC_2015-01.json", type = "text")
# )


data <- monthly_subreddits[[5]]
May <- data[order(data$value,decreasing = TRUE),]
save(May,file = "May.Rdata")


