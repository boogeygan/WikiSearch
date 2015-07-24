# WikiSearch
=== Programs ===
WikiSearchMain,  WikiXMLParser, Retriever, Stemmer, WikiPageIndexer

=== Author ===
Gagandeep Chhabra

=== Description ===
The programs here are used to create the index on the input XML file (Wikipedia XML Dump) and then search on the corpus. Basically its an offline serach tool for wikipedia XML dump.

=== Technology ===
Java

=== Explanation Of Implementation ===
1. To Reduce the size of the Index the Page Ids and pageLengths are stored in Hexadecimal format(considerable amount of space is saved).
2. Also three levels of Index is created. Primary Index has entire data, Secondary Index has Interleaved data from Primary and finally Tertiary Index has interleaved data from Secondary Index.
3. When parsing a page from Corpus, a Forward Index is created. Forward Index has page Id, corresponding page length and the title of it. This is useful when normalizing the weight in retrieval.
4. Also static precedence has been given to fields like TITLE is given max value of 5, content of Info box has weight of 4 and so on.
