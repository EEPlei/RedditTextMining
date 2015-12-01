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
#rhoptions(zips = '/R/R.Pkg.tar.gz')
#rhoptions(runner = 'sh ./R.Pkg/library/Rhipe/bin/RhipeMapReduce.sh')



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
  stopwords <- c("a","the","it")
  lapply(
    seq_along(map.keys), 
    function(r) 
    {
      time <- str_extract(map.values[[r]],"[0-9]{10}")
      new.time <- as.POSIXct(as.numeric(time), origin='1970-01-01')
      strs = strsplit(toString(new.time), " ")
      strs2 = strsplit(strs[[1]][1], "-")
      date = strs2[[1]][3]
      if(date == "14"){
        line = tolower(map.values[[r]]$body)
        line = gsub("[-—]"," ",line)
        line = gsub("[^'`’[:alpha:][:space:]]","",line,perl=TRUE)
        line = gsub("(^\\s+|\\s+$)","",line)
        line = strsplit(line, "\\s+")[[1]]
        line = line[line != ""]
        line <- setdiff(line, stopwords)
      }
      lapply(line, rhcollect, value=1)
    }     
  )
})


wc = rhwatch(
  map      = wc_map,
  reduce   = wc_reduce,
  input    = rhfmt("/data/short_1e3.json", type = "text")
)


get_val = function(x,i) x[[i]]

counts = data.frame(key = sapply(wc,get_val,i=1),value = sapply(wc,get_val,i=2), stringsAsFactors=FALSE)
