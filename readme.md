# Document-search-engine-computing-TF-IDF-scores-using-Hadoop

#### Run the following commands to execute each file

#### DocWordCount.java

the output of this DocWordCount program
is of the form ‘word#####filename count​’, where ‘#####​’ serves as a
delimiter between word ​and filename ​and tab ​serves as a delimiter between
filename​ and count

#### creating the input directory
hadoop fs -mkdir /user/cloudera/docwordcount /user/cloudera/docwordcount/input

cd canterbury

hadoop fs -put * /user/cloudera/docwordcount/input

cd ..

mkdir -p docbuild
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* DocWordCount.java -d docbuild -Xlint
jar -cvf docwordcount.jar -C docbuild/ .
hadoop jar docwordcount.jar org.myorg.DocWordCount /user/cloudera/docwordcount/input /user/cloudera/docwordcount/output


#### TermFrequency.java

#### Term Frequency:
Term frequency is the number of times a particular word t occurs in a document d.
TF(t, d) = No. of ti ​ mes ​t appears in document ​d
Since the importance of a word in a document does not necessarily scale linearly with the
frequency of its appearance, a common modification is to instead use the logarithm of the
raw term frequency.
	WF(t,d) = 1 + log​10​(TF(t,d)) if TF(t,d) > 0, and 0 otherwise (equation#1)

This class contains the MR job to compute the logarithmic TermFrequency 

hadoop fs -mkdir /user/cloudera/termfrequency /user/cloudera/termfrequency/input

cd canterbury

hadoop fs -put * /user/cloudera/termfrequency/input

cd ..

mkdir -p tfbuild
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TermFrequency.java -d tfbuild -Xlint
jar -cvf termfrequency.jar -C tfbuild/ .
hadoop jar termfrequency.jar org.myorg.TermFrequency /user/cloudera/termfrequency/input /user/cloudera/termfrequency/output


#### TFIDF.java

#### Inverse Document Frequency:
The inverse document frequency (IDF) is a measure of how common or rare a term is
across all documents in the collection. It is the logarithmically scaled fraction of the
documents that contain the word, and is obtained by taking the logarithm of the ratio of
the total number of documents to the number of documents containing the term.
	IDF(t) = log​10​(Total # of documents / # of documents containing term t) (equation#2)
Under this IDF formula, terms appearing in all documents are assumed to be stopwords and
subsequently assigned IDF=0. We will use the smoothed version of this formula as follows:
	IDF(t) = log​10​(1 + Total # of documents / # of documents containing term t) (equation#3)
Practically, smoothed IDF helps alleviating the out of vocabulary problem (OOV), where it is
better to return to the user results rather than nothing even if his query matches every single
document in the collection.


#### TF-IDF:
Term frequency–inverse document frequency (TF-IDF) is a numerical statistic that is
intended to reflect how important a word is to a document in a collection or corpus of
documents. It is often used as a weighting factor in information retrieval and text mining.
	TF-IDF(t, d) = WF(t,d) * IDF(t) (equation#4)
	
This class contains the MR jobs to compute the TFIDF

hadoop fs -mkdir /user/cloudera/tfidf /user/cloudera/tfidf/input

mkdir -p tfidfbuild
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TFIDF.java TermFrequency.java -d tfidfbuild -Xlint
jar -cvf tfidf.jar -C tfidfbuild/ .
hadoop jar tfidf.jar org.myorg.TFIDF /user/cloudera/termfrequency/input /user/cloudera/tf/output /user/cloudera/tfidf/output

hadoop fs -copyToLocal /user/cloudera/rank/output1 output

#### Search.java

The job (Search.java) accepts as input a user query and outputs a
list of documents with scores that best matches the query (a.k.a search hits​).

mkdir -p searchbuild
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Search.java -d searchbuild -Xlint
jar -cvf search.jar -C searchbuild/ .
hadoop jar search.jar org.myorg.Search /user/cloudera/tfidf/output /user/cloudera/search/output

#### Rank.java

The job would accept the output of Search.java and output the search hits ranked by
scores

mkdir -p rankbuild
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Rank.java -d rankbuild -Xlint
jar -cvf rank.jar -C rankbuild/ .
hadoop jar rank.jar org.myorg.Rank /user/cloudera/search/output /user/cloudera/rank/output

hadoop fs -copyToLocal /user/cloudera/rank/output output








