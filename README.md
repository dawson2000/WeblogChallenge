# WeblogChallenge

this project is base on Spark 2.0 version

### scala/WebLogProcess.scala

It is Processing for all data Processing and analytics


# Processing & Analytical goals (scala/WebLogProcess.scala)  :

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window.

2. Determine the average session time


3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

4. Find the most engaged users, ie the IPs with the longest session times

# Additional questions

1.Predict the expected load (requests/second) in the next minute

#### python/workload.py    

2.Predict the session length for a given IP

### scala/WebLogSessionLengthModel.scala

3.Predict the number of unique URL visits by a given IP

### scala/WebLogUniqueUrlModel.scala
