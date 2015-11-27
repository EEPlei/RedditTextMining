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

library(Rhipe)
rhinit()

## Uncomment following lines if you need non-base packages
rhoptions(zips = '/R/R.Pkg.tar.gz')
rhoptions(runner = 'sh ./R.Pkg/library/Rhipe/bin/RhipeMapReduce.sh')

### put a file to HDFS so we can use it
#rhput("/data/Shakespeare/hamlet.txt","/data/")

rhls("/data")


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
  lapply(
    seq_along(map.keys), 
    function(r) 
    {
      line = tolower(map.values[[r]])
      line = gsub("[-—]"," ",line)
      line = gsub("[^'`’[:alpha:][:space:]]","",line,perl=TRUE)
      line = gsub("(^\\s+|\\s+$)","",line)
      line = strsplit(line, "\\s+")[[1]]
      line = line[line != ""]
      
      lapply(line, rhcollect, value=1)
    }
  )
})


wc = rhwatch(
  map      = wc_map,
  reduce   = wc_reduce,
  input    = rhfmt("/data/Shakespeare/hamlet.txt", type = "text")
)


get_val = function(x,i) x[[i]]

counts = data.frame(key = sapply(wc,get_val,i=1),value = sapply(wc,get_val,i=2), stringsAsFactors=FALSE)



## Line counting example

lc_reduce = expression(
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

lc_map = expression({
  lapply(
    seq_along(map.keys), 
    function(r) 
    {
      rhcollect("line",1)
    }
  )
})


lc = rhwatch(
  map      = lc_map,
  reduce   = lc_reduce,
  input    = rhfmt("/data/Shakespeare/hamlet.txt", type = "text")
  #input    = rhfmt("/data/RC_2015-01.json", type = "text")
)


## User Comment Example


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
      key = fromJSON(map.values[[r]])$author
      value = 1
      rhcollect(key,value)
    }
  )
})


user = rhwatch(
  map      = user_map,
  reduce   = user_reduce,
  input    = rhfmt("/data/short_1e3.json", type = "text")
)


get_val = function(x,i) x[[i]]

counts = data.frame(key = sapply(user,get_val,i=1),value = sapply(user,get_val,i=2), stringsAsFactors=FALSE)