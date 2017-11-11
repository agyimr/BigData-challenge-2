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
 * Pair is a class to contain two subreddit's names and the number of their common
 * commenters. 
 */
class Pair {
	string subreddit1;
	string subreddit2;
	long number_of_common;
public:
	Pair() {
		subreddit1 = "";
		subreddit2 = "";
		number_of_common = 0;
	}

	Pair(string sub1, string sub2, long nr) {
		subreddit1 = sub1;
		subreddit2 = sub2;
		number_of_common = nr;
	}

	long getNumberOfCommon() {
		return number_of_common;
	}

	string getSubreddit1() {
		return subreddit1;
	}

	string getSubreddit2() {
		return subreddit2;
	}
};

/*
 * Toplist is a list which contains ordered elements. It has a thread-safe add function. 
 * Anyone calling this function will try to add the element to the list. It will automa-
 * tically put it to the correct place. 
 */
class TopList {
	mutex mu_write;
	int size;
	Pair* toplist;
public:
	// you can set the length of the list.
	TopList(int s) {
		size = s;
		toplist = new Pair[size];
	}
	// Thread-safe add method. Only one thread can use it at a time.
	void add(Pair p) {
		lock_guard<mutex> locker(mu_write);
		for (int i = 0; i < size; ++i) {
			if (p.getNumberOfCommon() > toplist[i].getNumberOfCommon()) {
				if (i == 0) {
					toplist[0] = p;
				}
				else {
					Pair tmp = toplist[i];
					toplist[i] = toplist[i - 1];
					toplist[i - 1] = tmp;
				}
			}
		}
	}

	// prints out the elements in the list.
	void print() {
		for (int i = 0; i < size; ++i) {
			cout << toplist[i].getSubreddit1() << ", " << toplist[i].getSubreddit2() << ": " << toplist[i].getNumberOfCommon() << endl;
		}
	}

	long getSmallest() {
		return toplist[0].getNumberOfCommon();
	}

	~TopList() {
		delete[] toplist;
	}
};

/*
 * AuthorMap is a class to store each author and map their names to a unique number
 * in a way that each actor is only mapped and stored once. This way we can refer 
 * each author with number, which is more memory efficient.
 */
class AuthorMap {
	long counter;
	mutex mu_write;
	unordered_map<string, long> map;
public:
	// first author's mapped value will be 1.
	AuthorMap() {
		counter = 1;
	}

	/*
	 * This function map's an author to a number and returns the mapped value. If it
	 * did not existed yet in the map, it adds it first. Thread-safe, only one thread
	 * can execute it at a time.
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
};

/*
 * A class to store each subreddit's commenters. We store the commenters in two
 * different data structures. In an unordered_set which gives us really fast 
 * lookup (to check if a commenter is in it or not), and in a vector which provi-
 * des quick iteration and the possibility to start from a given index.
 */
class Subreddits {
	mutex mu_write;
	unordered_map<string, unordered_set<long>> map;
	unordered_map<string, vector<long>> v_map;
public:

	/*
	 * Thread safe addition of a subreddit and a commenter (author). It creates the
	 * subreddit if it did not existed yet, and adds the author's mapped value to the
	 * lists if it was not there yet. Thread-safe which means it can be only executed
	 * by one thread at a time.
	 */
	void shared_insert(string subreddit, long author_id) {
		lock_guard<mutex> locker(mu_write);
		if (map.count(subreddit) == 0) {
			vector<long> v_tmp;
			unordered_set<long> tmp;
			v_map[subreddit] = v_tmp;
			map[subreddit] = tmp;
		}
		else {
			if (map[subreddit].count(author_id) == 0) {
				v_map[subreddit].push_back(author_id);
				map[subreddit].insert(author_id);
			}
		}
	}

	unordered_set<long> getActorsForSubreddit(string subreddit) {
		return map[subreddit];
	}

	unordered_map<string, unordered_set<long>>* getSubreddits() {
		return &map;
	}

	unordered_map<string, vector<long>>* getSubredditsVect() {
		return &v_map;
	}
};

/*
 * This class is responsible for a thread-safe way of reading in lines from the file.
 * It contains a thread safe method which reads in a new line from the file until it
 * reaches it's end.
 */
class SharedFileReader {
	mutex mu_read, mu_write;
	ifstream input;
public:
	SharedFileReader() {
		input = ifstream("C:\\reddit\\reddit");
	}

	// Thread safe readline. Only one thread at a time.
	basic_istream<char>& shared_read(string& line) {
		lock_guard<mutex> locker(mu_read);
		return getline(input, line);
	}

	// This is just a thread-safe print for debugging purposes.
	void shared_print(string line) {
		lock_guard<mutex> locker2(mu_write);
		cout << line << endl;
	}


};

/*
 * This class is responsible of providing a thread-safe method for each thread to get
 * a the next subreddit's map to the vector containing it's authors. It is really like
 * the file reader except it does not return the next line's of a file, instead it re-
 * turns the next subreddit's name mapped to a vector of authors...
 */
class SharedVectorReader {
	mutex mu_read;
	Subreddits* subreddits;
	unordered_map<string, vector<long>>::iterator subreddit_v_iterator;
public:
	SharedVectorReader(Subreddits* s) {
		subreddits = s;
		subreddit_v_iterator = s->getSubredditsVect()->begin();
	}

	// thread-safe function to give the next element in the subreddit's vector map.
	auto getNext() {
		lock_guard<mutex> locker(mu_read);
		if (subreddit_v_iterator != subreddits->getSubredditsVect()->end()) {
			return subreddit_v_iterator++;
		}
		return subreddit_v_iterator;
	}
};

/*
 * This is the data-gathering function that all the thread's execute until they reach the
 * end of the file. 
 *   1. read in the next line from the file (thread-safe with SharedFileReader)
 *   2. JSON parse it and get the author's name as well as the subreddit's name.
 *   3. Map the author's name to a number value for memory efficiency.
 *   4. Add the author to the subreddit.
 */
void do_work(SharedFileReader& reader, Subreddits& subreddits, AuthorMap& authors) {
	for (string line; reader.shared_read(line); ) {
		auto json_line = json::parse(line.c_str());
		string subreddit = json_line["subreddit"];
		string author = json_line["author"];
		long auth_id = authors.shared_insert(author);
		subreddits.shared_insert(subreddit, auth_id);
	}

}

/*
 * The is the second phase of the whole process. It gets executed by all the threads
 * after the data-gathering. It goes through every possible combination of subreddits
 * and checks how many common author's they have.
 *   1. first get the next subreddit in the line
 *   2. check each subreddit after(!) this one for common authors.
 *   3. for each subreddit pair, add the number of common authors to the toplist
 *        - it will only going to be added if it is big enough to be on the toplist.
 */
void do_sorting_work(SharedVectorReader& reader, Subreddits& subreddits, TopList& top) {
	// getting the next subreddit in the line in a thread-safe manner (through SharedVectorReader)
	// we get a map with a key of the name of the subreddit and a value of a vector of author ids.
	/* this function could be optimised with providing a vector iterator here...*/
	for (auto subreddit = reader.getNext(); subreddit != subreddits.getSubredditsVect()->end(); subreddit = reader.getNext()) {
		string subreddit_name = subreddit->first;
		// we grab the vector for the outter subreddit, because we can iterate through it much faster.
		vector<long> subreddit_authors = subreddit->second;

		// check all the subreddits after the one we are already examining.
		/* this function could be optimised with providing a vector iterator here...*/
		for (auto subreddit_in = subreddit; subreddit_in != subreddits.getSubredditsVect()->end(); ++ subreddit_in) {
			string subreddit_in_name = subreddit_in->first;

			// we grab the set of actors for the inner subreddit, because the lookup is much faster in
			// the set. 
			unordered_set<long> subreddit_in_authors = subreddits.getActorsForSubreddit(subreddit_in_name);
			if (subreddit_in_name != subreddit_name) {

				// counting common authors.
				long common_authors = 0;
				for (const auto& author_name : subreddit_authors) {
					// if contains
					if (subreddit_in_authors.count(author_name) != 0) {
						common_authors++;
					}
				}

				// adding the pair of subreddits with the number of common authors to the toplist
				// in a thread-safe way (top.add is threadsafe).
				Pair p(subreddit_name, subreddit_in_name, common_authors);
				top.add(p);
			}
		}
	}
	
}

int main()
{
	// create assets and add their references to the threads. 
	SharedFileReader file_reader;
	AuthorMap authors;
	Subreddits subreddits;
	// first part, only gathering the data
	thread t1(do_work, ref(file_reader), ref(subreddits), ref(authors));
	thread t2(do_work, ref(file_reader), ref(subreddits), ref(authors));
	thread t3(do_work, ref(file_reader), ref(subreddits), ref(authors));
	thread t4(do_work, ref(file_reader), ref(subreddits), ref(authors));
	thread t5(do_work, ref(file_reader), ref(subreddits), ref(authors));
	thread t6(do_work, ref(file_reader), ref(subreddits), ref(authors));
	thread t7(do_work, ref(file_reader), ref(subreddits), ref(authors));
	thread t8(do_work, ref(file_reader), ref(subreddits), ref(authors));
	// waiting for each thread to finish.
	t1.join();
	t2.join();
	t3.join();
	t4.join();
	t5.join();
	t6.join();
	t7.join();
	t8.join();
	cout << "Finished with first multithreadding..." << endl;

	// creating assets and adding their reference to the threads in the second phase
	SharedVectorReader vector_reader(&subreddits);
	TopList top(10);

	thread t11(do_sorting_work, ref(vector_reader), ref(subreddits), ref(top));
	thread t12(do_sorting_work, ref(vector_reader), ref(subreddits), ref(top));
	thread t13(do_sorting_work, ref(vector_reader), ref(subreddits), ref(top));
	thread t14(do_sorting_work, ref(vector_reader), ref(subreddits), ref(top));
	thread t15(do_sorting_work, ref(vector_reader), ref(subreddits), ref(top));
	thread t16(do_sorting_work, ref(vector_reader), ref(subreddits), ref(top));
	thread t17(do_sorting_work, ref(vector_reader), ref(subreddits), ref(top));
	thread t18(do_sorting_work, ref(vector_reader), ref(subreddits), ref(top));

	// waiting for the threads to finish
	t11.join();
	t12.join();
	t13.join();
	t14.join();
	t15.join();
	t16.join();
	t17.join();
	t18.join();
	cout << "Finished with second multithreadding..." << endl;

	// print out results. 
	top.print();

	cin.get();
	return 0;
}

