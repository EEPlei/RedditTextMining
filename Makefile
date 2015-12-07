all: hw_5.html

hw2.html: hw_5.Rmd Jan.Rdata Feb.Rdata Mar.Rdata Apr.Rdata May.Rdata Jan_14.Rdata Feb_14.Rdata Mar_14.Rdata jan_gild.Rdata jan_utc.Rdata
	Rscript -e "library(rmarkdown);render('hw5.Rmd')"

Jan.Rdata: reddit_rhipe.R
	R --no-save < reddit_rhipe.R
	
Feb.Rdata: reddit_rhipe.R
	R --no-save < reddit_rhipe.R
	
Mar.Rdata: reddit_rhipe.R
	R --no-save < reddit_rhipe.R
	
Apr.Rdata: reddit_rhipe.R
	R --no-save < reddit_rhipe.R
	
May.Rdata: reddit_rhipe.R
	R --no-save < reddit_rhipe.R

Jan_14.Rdata: task3_rhipe.R
	R --no-save < task3_rhipe.R
	
Feb_14.Rdata: task3_rhipe.R
	R --no-save < task3_rhipe.R

Mar_14.Rdata: task3_rhipe.R
	R --no-save < task3_rhipe.R
	
jan_gild.Rdata: utc_rhipe.R
	R --no-save < utc_rhipe.R
	
jan_utc.Rdata: utc_rhipe.R
	R --no-save < utc_rhipe.R

clean:
	rm -rf data/
	rm -f hw2.html

.PHONY: all clean
