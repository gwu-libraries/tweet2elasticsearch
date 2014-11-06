tweet2elasticsearch
===================

A commandline utility for loading tweets into elasticsearch and perform queries against the resulting index.

Full documentation is available from the commandline using the `-h` flag.

Loading tweets
--------------
Tweets are loaded from gzipped, line-oriented JSON files.  The files can be local or in an S3 bucket.

For each tweet, the following fields are loaded:
* id
* text
* screen name
* post date
* user mentions
* hashtags
* file
* offset

Searching tweets
----------------
Tweets can be searched by:
* text
* date from
* date to
* screen names
* user mentions
* hashtags

The query parameters can be provided from the commandline or in a file.

Paging or complete results is supports. Results can be sent to stdout or to a CSV file.