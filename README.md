# Challenge 2 #

I made all my solutions in **C++(11)** because I found it easier to deal with threads that way, and the computational speed was also considerably faster this way. I tried to comment everything extensively, to help understanding. The whole code can be found here: [code](https://github.com/agyimr/BigData-challenge-2).

I used an external library to process json files. It can be found here: [json](https://github.com/nlohmann/json).

In all my solutions I only worked with the text file.

My computer is running on an **intel i7 4970** processor which is capable of handling 8 threads at a time. Therefore I was always using 8 threads. And it has only **8 Gb of ram** built in which made everything far more challenging...

---

## Exercise 1 ##

Which subreddits have the largest vocabulary? Return the 10 subreddits with the largest vocabularies and how large their vocabularies are!
 * The size of a vocabulary for a subreddit is defined as the total number of distinct words is used in the comments of the subreddit.

#### Solution ####

The challenge here was to somehow be able to store all the distinct words for each subreddit. In order to somehow make the dataset a bit smaller I mapped every distinct word to a unique number. This number can be stored in a much smaller space in memory. 

The following data structure was used to store all the data in the memory. I used an unordered\_map with string keys (subreddit names), and unordered\_sets of numbers as values. Every subreddit's words are mapped to numbers which we put into the subreddit's unordered\_set. The set only let's us store each number once, therefore no duplicates will be generated. Only the distinct words.

#### Algorythm ####
 1. First we go through the file (with the help of the class SharedFileReader which let's all the threads work on the same file in parallel)
 2. We extract each line, get the comment and the subreddit's name, clean the comment from special characters, and divide it to words.
 3. Then map each word to a number and add this number to the subreddit's set.
 4. After we finished going through all the lines we just need to go through all the subreddits and compare the length of the unordered_sets. 
 5. At last we print the largest ten.

#### Results ####

**Execution time: 23 minutes**

Number | Subreddit | Number of distinct words
--- | --- | ---
1 | AskReddit | 470078
2 | funny | 192024
3 | pics | 180308
4 | videos | 163793
5 | todayilearned | 161560
6 | worldnews | 158867
7 | subredditreports | 144235
8 | leagueoflegends | 140919
9 | AdviceAnimals | 121523
10 | pcmasterrace | 119164

#### Note ####
I used my version of differenciating between words, therefore you might not got the exact same results as me...

---

## Exercise 2 ##

Which pairs of subreddits have the most comment authors in common? Show the 10 subreddit pairs with most authors in common and how many authors they have in common!

#### Solution ####

Here the most important factor was to somehow come up with a considerably fast algorythm. Because of the fact that we do not need to store that much information this time, I tried to fasten things up with storing the same data twice in different data structures. One for lookup speed (this one also grants no duplicates) and one for itaration.

To reduce the amount of data stored I mapped each author's name to a unique number with a helper class. This made me possible to only store the id-s, this way using less memory.

The following data structure was used: I store two unordered\_maps for each subreddit. In both cases the key is the name of the subreddit. The value is in one case an unordered\_set of the author\_id-s, and in the other case a vector of the author\_id-s. Of course for each subreddit only the authors who wrote there was being put into both maps. 

#### Algorythm ####

Data gathering: (parallel)
1. First we go through all the lines one by one, and extract the author and the subreddit. 
2. Map the author to a unique number.
3. Add this unique number to both the maps for each subreddit.

After this finishes, we have all the data in memory, now we have to find the common authors. (parallel)
1. We go through all the subreddits in our map one by one.
2. For each subreddit we check whether it has any common authors with the other subreddits still in front of him in the map. So if we are examining the 6th subreddit, we do not compare it with the first 5, we only compare it with the others starting from 6. This way we only compare every pair once. 
3. During the comparison, we use the vector of author\_id-s to iterate over authors, and the unordered\_set of author\_id-s to check whether it is in the set or not. This gives us a huge speedup.
  * we basically iterate over all the authors and check whether they exist or not in the other subreddit for each case.
4. For every subreddit pair, we add the number of common authors to the toplist. But toplist only adds it in if it is big enough to get listed. 
5. We pring the results.

#### Results ####

**Execution time: 52 minutes**

**Only used about ~850 Mb of memory**

Number | Subreddit 1   | Subreddit 2 | Number of common authors
--- | --- | --- | ---
1 | AskReddit | funny | 141792
2 | AskReddit | pics | 138676
3 | AskReddit | todayilearned | 105143
4 | pics | funny | 104904
5 | AskReddit | videos | 91976
6 | AskReddit | AdviceAnimals | 84458
7 | todayilearned | funny | 71667
8 | todayilearned | pics | 71560
9 | AskReddit | WTF | 71249
10 | pics | videos | 67091

---

## Exercise 3 ##

Which subreddit has the deepest comment threads on average? Show the 10 subreddits with deepest average comment threads and how deep they are on average!

#### Solution ####

The data structure used is the following: For each subreddit we store a key-value pair (unordered\_map). The key is the name of the subreddit, while the value is a custom container class called SubredditMetaData. This consists of an unordered\_set of comment id-s (for fast lookup), a vector of (comment\_id, parent\_id) pairs and a vector of integers to store the number of comments in each depth level.

#### Algorythm ####

Data gathering... (parallel)
1. Each thread grabs a line from the text file, and extracts the subreddit's name, the parent\_id, the link\_id  and the id (name) of the comment. We need the link\_id in order to determine whether the comment is a thread beginning comment or not. If it is the same as the parent_id then it is a thread beginner comment.
 * we need this information, to determine where to save the comment's metadata. If it is a thread-beginner comment, we do not need to store the parent\_id, so we put it into the unordered\_set of the SubredditMetaData. If it is not thread-beginner comment, then we need to store it's (comment\_id, parent\_id) pair. Of course in these cases we do not know who the parent is.

Data processing (parallel)
1. For each subreddit we check at the comments for which we do not know who the parent is (not a thread-beginner) if we can find the parent at the thread-beginners.
2. Based on if we found it or not, we store the unknown nodes in two separate places.
3. After going through all of them, we count how much did we found the parent for, and substract this number from the already existing thread-beginners array's length. The difference is the number of comments who has depth of 0. 
4. As a final step we **replace** the two already existing arrays (the comments for which we know the parent for and the comments for which we do not) with the newly built ones. 
5. We repeat this procedure until we do not find any comment's parent anymory. 
6. Calculate the average depth of the subreddit based on the collected data.
7. Add it to the toplist, but it will only get listed if it's big enough.
8. Print results.

#### Results ####

**Execution time: 12 minutes**

Number | Subreddit | Average depth
--- | --- | ---
1 | counting | 298.396
2 | EroticRolePlay | 283.733
3 | pepsibot | 250
4 | SburbRP | 188
5 | TheTwoCenturions | 156
6 | RidersOfBerk | 138
7 | survivorrankdown2 | 83
8 | roleplaypinies | 74
9 | DigitalMonsterRP | 70
10 | ExploreFiction | 68.25

---

#### Remark ####
 If I could do it again I would not map all the words and authors to numbers, I would just put them in an unordered_set and store each one's reference at every subreddit. Therefore we could use significantly less memory. 