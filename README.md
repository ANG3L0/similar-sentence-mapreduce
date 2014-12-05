Your task is to quickly find the number of pairs of sentences that are at the word-level edit distance at most 1. Two sentences S1 and S2 they are at edit distance 1 if S1 can be transformed to S2 by: adding, removing or substituting a single word.

For example, consider the following sentences where each letter represents a word:
• S1: A B C D
• S2: A B X D
• S3: A B C
• S4: A B X C
Then pairs the following pairs of sentences are at word edit distance 1 or less: (S1, S2), (S1, S3), (S2, S4), (S3, S4).

The input data has 9,397,023 sentences, each one divided by a new line and with the sentence id at the beginning of the line. The zip compressed file size is around 500MB and it's located here.
All sentences in the input data are at least 10 words long. A straightforward LSH approach like the one taught in class for jaccard similarity can be used to solve this problem, however it may not necessarily be the faster approach.


Tried a bunch of different crap to make this faster, but since this problem was framed in the context of mapreduce (50min runtime).
Could have done the following to preprocess (since duplicate pairs of edit distance = 0 take up 426 of the 429 million):

-Read in file sequentially
-have hash key: sentence string, value: count
after file is finished being read just do:
for each key,value in hash:
  send key to mapreduce framework
  totalcount+=value*(value+1)>>1;

that way mapreduce will need a ton less memory since a lot of sentences were duplicates.  also, most pairs will not be considered now since they won't hit the same key.
