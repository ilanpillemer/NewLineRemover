# NewLineRemover
MapReduce Program that uses a modified LineReader to remove new line characters in Quotes ( typical data preprocessing task ) 

This project is a Mapreduce program on the input folder removing all new line characters between quotes.

New line characters in database text fields is a common data preparation problem.
This mapreduce program uses a custom Record Reader to remove any new line characters between quotes "
I.e. Text files like 

1,"Ben","Ben was working
on our cluster"

2,"Karl","Karl is an hadoop engineer"
...

Would be translated to

1,"Ben","Ben was working on our cluster"

2,"Karl","Karl is an hadoop engineer"

To use a different quote character than " set the QUOTE value in the QuotationLineReader
private static final byte QUOTE = '\"';

To change the way newline characters are handled change the processing in the mapper. Currently they are escaped. Removing them would also be valid.
line = line.replaceAll("\n", "\\n");
line = line.replaceAll("\r", "\\r");
