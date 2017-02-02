# Team4_hw5

## Task 1 - What's popular with Redditors?

Each comment belongs to a particular subreddit, we would like to know for the given time period what were the 25 most popular subreddits? We would also like to have this broken down by month (Jan to May) - create a [Billboard](http://www.billboard.com/charts/hot-100)-like table for each month that shows the top 25 subreddits for that month that also includes the change in rank since the previous month. Comment on any subreddits that show a strong positive or negative trend.

<br/>

## Task 2 - When do Redditors post?

Create plots that show the frequency of Reddit comments over the entire time period (aggregate to an hourly level). Also create plots that show the frequency of comments over the days of the week (data should again be at an hour level). Comment on any patterns you notice, particularly days with unusually large or small numbers of comments.

Recreate the above plots but only for comments that were gilded (commentor was given [Reddit gold](https://www.reddit.com/gold/about) by another user). Comment on the similarly or difference in the plot collections.

## Task 3 - What are Redditors saying (on Valentine's Day)?

Valentine's day is on February 14 every year, our data contains this for 2015 - pick two other dates and perform a word frequency analysis of these three days and see if what Redditors say on Valentine's day appears to be different from your control days. This does not need to be a fully quantitative analysis but do make sure to clean up the data (e.g. strip things like punctuation and capitalization, remove stop words, etc.) 

<br/>


## Data

The data is available on saxon via HDFS in the `/data/` directory as `RC_2015-01.json`, `RC_2015-02.json`, `RC_2015-03.json`,`RC_2015-04.json`, `RC_2015-05.json` respectively. A subsets of `RC_2015-01.json` with 1,000,000 and 1,000 comments are available as `short_1e6.json` and `short_1e4.json` respectively - these files are available on saxon's HDFS as well as in `/data/Sta523/reddit` on the regular filesystem. It is recommended that you start your analyses with the smaller datasets and work your way up in size.


## Work Product

* `hw5.Rmd` - write up detailing the specifics of your implementation (e.g. query approach)

* mapreduce/spark jobs - I recommend having one separate file per task that contains the related mapreduce / spark implementation. These files should perform the query and save the result to a local Rdata file (these should not be commited, but should be used by `hw5.Rmd` so the jobs don't need to be rerun).

* `Makefile` - write a Makefile that will run all of the necessary mapreduce / spark jobs and then compile `hw5.Rmd`.
<br/>


<div style="text-align: left">
Assignment from STA 523 Statistical Programming with Professor Colin Rundel    
</div>   
<div style="text-align: left">
http://www2.stat.duke.edu/~cr173/Sta523_Fa15/hw/hw0.html 
</div>