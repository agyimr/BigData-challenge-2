// bigdata_challenge_2.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "json.hpp"
#include <unordered_map>
#include <unordered_set>
#include <fstream>
#include <iostream>
#include <string>
#include <regex>
#include <mutex>
#include <thread>


using namespace std;
using json = nlohmann::json;

/* 
 * Vocabularity is a container class to hold the result of each subreddit
 * It stores a string which is the name of the subreddit, and the number of
 * distinct words.
 */
class Vocabularity {
	string subreddit;
	long vocabularity;
public:

	// default constructor, because we'll need arrays of these...
	Vocabularity() {
		subreddit = "";
		vocabularity = 0;
	}

	Vocabularity(string subr, long voc) {
		subreddit = subr;
		vocabularity = voc;
	}

	long getVoc() {
		return vocabularity;
	}

	string getName() {
		return subreddit;
	}
};

/*
 * WordsMap is a class to ensure multi-thread safe mapping of words into a hashed map.
 * We map each distinct word to a long number. We do this in order to save memory. Now
 * every time we encounter a word in a comment, we only have to store it's number for 
 * the subreddit, instead of the whole word. 
 */
class WordsMap {
	long counter;
	mutex mu_write;
	// unordered map, so lookup is constant, and fast.
	unordered_map<string, long> map;
public:
	// we map the first word to 1.
	WordsMap() {
		counter = 1;
	}

	/*
	 * Here is where the magic happens. This function can be executed only by one 
	 * thread at a time. If it gets a word that is not in the map yet, it adds it.
	 * Either way in the end it returns the number for which the word has been mapped.
	 */
	long shared_insert(string key) {
		lock_guard<mutex> locker(mu_write);
		long value = map[key];
		if (value == 0) {
			map[key] = counter;
			counter++;
			return (counter - 1);
		}
		return value;
	}

	long getNumberOfElements() {
		return map.size();
	}
};

/*
 * Kind of same purpuse as the WordsMap except here we store each subreddit's
 * vocabulary in a map. The key is the name of the subreddit, and the value is
 * an unordered set of words. We use this data structure, because it let's us
 * only store one value for every word. Therefore only distinct words will be
 * stored. And we only need the size of this later, which can be queried in a
 * really fast manner as well...
 */
class Subreddits {
	mutex mu_write;
	unordered_map<string, unordered_set<long>> map;
public:
	/*
	 * This function can be only executed by one thread at a time. It adds a word's
	 * mapped number to the set of words. If the reddit is new it creates it. 
	 */
	void shared_insert(string subreddit, long word_number) {
		lock_guard<mutex> locker(mu_write);
		if (map.count(subreddit) == 0) {
			unordered_set<long> tmp;
			map[subreddit] = tmp;
		}
		map[subreddit].insert(word_number);
	}

	long getNumberOfWordsInSubreddit(string subreddit) {
		return map[subreddit].size();
	}

	/* 
	 * this function prints a list of Vocabularities with the most lexically 
	 * diverse subreddits.
	 */
	void getMostDiverse(int number) {
		Vocabularity* top = new Vocabularity[number];
		for (auto element = map.begin(); element != map.end(); ++element) {
			Vocabularity current(element->first, element->second.size());
			for (int i = 0; i < number; ++i) {
				if (current.getVoc() > top[i].getVoc()) {
					if (i == 0) {
						top[0] = current;
					}
					else {
						Vocabularity tmp = top[i];
						top[i] = top[i - 1];
						top[i - 1] = tmp;
					}
				}
			}
		}
		for (int i = 0; i < number; ++i) {
			cout << top[i].getName() << ": " << top[i].getVoc() << endl;
		}
	}
};


/*
 * SharedFileReader is a class responsible of reading in a thread-safe manner from the
 * same file without leaving out lines, or reading the same twice.
 */
class SharedFileReader {
	mutex mu_read, mu_write;
	ifstream input;
public:
	SharedFileReader() {
		input = ifstream("C:\\reddit\\reddit");
	}

	// this can only be executed by one thread at a time, and it returns a new line.
	basic_istream<char>& shared_read(string& line) {
		lock_guard<mutex> locker(mu_read);
		return getline(input, line);
	}

	// this is a thread-safe print, just for debugging purposes.
	void shared_print(string line) {
		lock_guard<mutex> locker2(mu_write);
		cout << line << endl;
	}


};

// cleaning the comments from special chars and converting everything to lowercase.
string clear_lines(string line) {
	// every char to lowercase...
	for (size_t i = 0, ilen = line.length(); i < ilen; ++i) {
		line[i] = tolower(line[i]);
	}

	// getting rid of special chars...
	line = regex_replace(line, regex("[^a-z ]"), " ");
	return line;
}

// mapping the line to distinct words. 
vector<string> get_words(string line) {
	// splitting the line to words...
	istringstream iss(line);
	vector<string> results((istream_iterator<string>(iss)), istream_iterator<string>());
	return results;
}

/*
 * Each thread executes this function repeatedly until the end of the file is reached.
 * Basically it goes like:
 *   1. grab the next line from the data file
 *   2. parse it to get the actual comment and the name of the subreddit
 *   3. clean the comment, and divide it to words
 *   4. map every word to a number
 *   5. add each mapped value to the actual subreddit's vocabulary
 *        - note: no word can be stored twice. This is achieved with the data structure
 *                used to store the numbers (unordered_set)
 */
void do_work(SharedFileReader& reader, Subreddits& subreddits, WordsMap& words) {
	for (string line; reader.shared_read(line); ) {
		auto json_line = json::parse(line.c_str());
		string subreddit = json_line["subreddit"];
		for (auto& word : get_words(clear_lines(json_line["body"]))) {
			long word_index = words.shared_insert(word);
			subreddits.shared_insert(subreddit, word_index);
		}
		
	}
	
}

int main()
{
	// create shared file reader, we will pass it to each thread. 
	SharedFileReader file_reader;
	// create words map, to map each word to number. We will pass it to each thread.
	WordsMap words;
	// create subreddits to store each subreddit's vocabulary with mapped words.
	Subreddits subreddits;

	// create four threads. Because this is how much my computer can handle...
	// pass the shared assets to each of them and the function to execute.
	thread t1(do_work, ref(file_reader), ref(subreddits), ref(words));
	thread t2(do_work, ref(file_reader), ref(subreddits), ref(words));
	thread t3(do_work, ref(file_reader), ref(subreddits), ref(words));
	thread t4(do_work, ref(file_reader), ref(subreddits), ref(words));
	thread t5(do_work, ref(file_reader), ref(subreddits), ref(words));
	thread t6(do_work, ref(file_reader), ref(subreddits), ref(words));
	thread t7(do_work, ref(file_reader), ref(subreddits), ref(words));
	thread t8(do_work, ref(file_reader), ref(subreddits), ref(words));
	// here we wait for each thread to finish work
	t1.join();
	t2.join();
	t3.join();
	t4.join();
	t5.join();
	t6.join();
	t7.join();
	t8.join();

	// and in the end we print the most diverse 10 subreddits.
	subreddits.getMostDiverse(10);

	// This line waits for an enter press. This way the program does not exits.
	cin.get();
    return 0;
}

