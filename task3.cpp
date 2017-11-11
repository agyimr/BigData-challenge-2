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

// Container class to store a comment's id and its parent's id.
class Node {
	string id;
	string parent_id;
public:
	Node() {
		id = "";
		parent_id = "";
	}

	Node(string id_in, string parent_id_in) {
		id = id_in;
		parent_id = parent_id_in;
	}

	string get_id() {
		return id;
	}

	string get_parent_id() {
		return parent_id;
	}
};

// Container class for a subreddit's name and it's average comment depth. 
class Pair {
	string subreddit;
	double value;
public:
	Pair() {
		subreddit = "";
		value = 0;
	}

	Pair(string s, double v) {
		subreddit = s;
		value = v;
	}

	double getValue() {
		return value;
	}

	string getSubreddit() {
		return subreddit;
	}


};

// This class provides a way to thread-safely add a Pair to the toplist. It will
// only add it if it the average depth is big enough to be added. 
class TopList {
	mutex mu_write;
	int size;
	Pair* toplist;
public:
	// you can set the list size.
	TopList(int s) {
		size = s;
		toplist = new Pair[size];
	}

	// only can be executed by one thread at a time, and only adds the pair if it is
	// big enough to get listed, but it puts it to the correct place. s
	void add(Pair p) {
		lock_guard<mutex> locker(mu_write);
		for (int i = 0; i < size; ++i) {
			if (p.getValue() > toplist[i].getValue()) {
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

	// print out the list. 
	void print() {
		for (int i = 0; i < size; ++i) {
			cout << toplist[i].getSubreddit() << ": " << toplist[i].getValue() << endl;
		}
	}

	~TopList() {
		delete[] toplist;
	}
};

// Contaner class to store nodes (comments) for a subreddit.
class SubredditMetaData {
	// first level for each comment's id, where we already know who the parent is. 
	unordered_set<string> first_level;
	// other level for each other comment. We don't know yet who their parent is.
	vector<Node> other_level;
	// levels contains a list if integers to store how much thread with a certain depth
	// there are. 
	vector<int> levels;
public:
	SubredditMetaData() {
		
	}

	SubredditMetaData(unordered_set<string> f_level_in, vector<Node> o_level_in) {
		first_level = f_level_in;
		other_level = o_level_in;
	}

	vector<int> get_levels() {
		return levels;
	}

	void add_level(int number_of_comments) {
		levels.push_back(number_of_comments);
	}

	void set_first_level(unordered_set<string> level_in) {
		first_level = level_in;
	}

	void set_other_level(vector<Node> level_in) {
		other_level = level_in;
	}

	void add_first_level(string id) {
		first_level.insert(id);
	}

	void add_other_level(Node n) {
		other_level.push_back(n);
	}

	unordered_set<string>* get_first_level() {
		return &first_level;
	}

	vector<Node>* get_other_level() {
		return &other_level;
	}
};

// Subreddits is a class to store all subreddit's data in a map for quick lookup
// it also provides a function to add a new comment to the existing pool. The data
// is stored in a map. The key is the name of the subreddit, and the value is a 
// SubredditMetaData object.
class Subreddits {
	mutex mu_write;
	unordered_map<string, SubredditMetaData> map;
public:

	// thread-safe way of adding a new comment to the already existing pool. It needs the name
	// of the subreddit, the id of the comment, the parent_id of the comment and a boolean to
	// indicate whether it's a thread starter comment or not (this can be decided by only looking
	// at the metadata of the comment)
	void shared_insert(string subreddit, string id, string parent_id, bool isFirstLevel) {
		lock_guard<mutex> locker(mu_write);
		if (map.count(subreddit) == 0) {
			SubredditMetaData tmp;
			map[subreddit] = tmp;
		}
		if (isFirstLevel) {
			map[subreddit].add_first_level(id);
		}
		else {
			map[subreddit].add_other_level(Node(id, parent_id));
		}
	}

	void print() {
		for (auto element = map.begin(); element != map.end(); ++element) {
			cout << element->first << "    : number of first level: " << element->second.get_first_level()->size() << ", number of other level: " << element->second.get_other_level()->size() << endl;
		}
	}

	SubredditMetaData* get_meta_data(string subreddit) {
		return &map[subreddit];
	}

	unordered_map<string, SubredditMetaData>* getMap() {
		return &map;
	}
};

// SharedFileReader provides a thread-safe way of reading lines from the same file
// without data collision.
class SharedFileReader {
	mutex mu_read, mu_write;
	ifstream input;
public:
	SharedFileReader() {
		input = ifstream("E:\\reddit\\reddit");
	}

	// can only be executed by one thread at a time, returns the next line of the file.
	basic_istream<char>& shared_read(string& line) {
		lock_guard<mutex> locker(mu_read);
		return getline(input, line);
	}

	// thread-safe print function for debugging purposes.
	void shared_print(string line) {
		lock_guard<mutex> locker2(mu_write);
		cout << line << endl;
	}


};

// This class is responsible of providing a thread safe way to iterate through the 
// subreddits. 
class SharedMapReader {
	mutex mu_read;
	Subreddits* subreddits;
	unordered_map<string, SubredditMetaData>::iterator subreddit_iterator;
public:
	SharedMapReader(Subreddits* s) {
		subreddits = s;
		subreddit_iterator = s->getMap()->begin();
	}

	// this function is thread safe, it can only be executed by one thread at a time, 
	// and it returns the next subreddit with it's metadata. 
	auto getNext() {
		lock_guard<mutex> locker(mu_read);
		if (subreddit_iterator != subreddits->getMap()->end()) {
			return subreddit_iterator++;
		}
		return subreddit_iterator;
	}
};


// this is the function that all the threads will execute in the first phase
// basically this is data gathering and organising. 
//   1. get the next line from the file on the disk
//   2. extract the subreddit name, the parent_id, comment id, and link_id from the line
//   3. add the comment to the subreddits
//        - We already know that if the link_id is equal to parent_id then the comment is
//          the first in the thread.
void do_work(SharedFileReader& reader, Subreddits& subreddits) {
	for (string line; reader.shared_read(line); ) {
		auto json_line = json::parse(line.c_str());
		string subreddit = json_line["subreddit"];
		string parent_id = json_line["parent_id"];
		string link_id = json_line["link_id"];
		string id = json_line["name"];
		subreddits.shared_insert(subreddit, id, parent_id, (link_id == parent_id));
	}

}

// calculates the average depth of a thread and returns it
// the vector contains a list of ints, where each number represents the number
// of threads with the depth of the index of the element in the list
// (1, 4, 3) => one with depth 0, 4 with depth 1, 3 with depth 2...
double calculate_average_dist(vector<int> levels) {
	int i = 0;
	int sum_weighted = 0;
	int sum = 0;
	for (const auto& n : levels) {
		sum_weighted += n*i;
		sum += n;
		i++;
	}
	if (sum != 0) {
		return sum_weighted / (double)sum;
	}
	return 0;
}

// this is the function that all the threads will execute in the second phase, basically thi is
// the data processing part. 
//    1. Grab the next subreddit from the subreddits (until end of subreddits)
//    2. Go through all the 'other_levels' of the subredditmetadata, and look for
//       nodes that has their parents in the first_level. Save and count them.
//           - After we found the nodes that has their parents in the first level
//             we replace the first level with them, and add the count of them to 
//             the levels of the actual subreddit. 
//           - we repeat this process until we can't find the parent of any node in
//             the other_level
// in the end we calculate the average depth and add it to the toplist.
void do_sorting_work(SharedMapReader& reader, Subreddits& subreddits, TopList& top) {
	// grab next subreddit
	for (auto subreddit = reader.getNext(); subreddit != subreddits.getMap()->end(); subreddit = reader.getNext()) {
		string subreddit_name = subreddit->first;
		int moved = -1;
		while (moved != 0) {
			unordered_set<string> next_level_base;
			vector<Node> next_level_high;
			moved = 0;

			// go through all the nodes that we don't know the parent yet
			for (auto current_node = subreddit->second.get_other_level()->begin(); current_node != subreddit->second.get_other_level()->end(); ++current_node) {
				// if the parent is in the first_level, we save it and count the occurences
				if (subreddit->second.get_first_level()->count(current_node->get_parent_id()) != 0) {
					moved++;
					next_level_base.insert(current_node->get_id());
				}
				// else we save it as well but in an other vector
				else {
					next_level_high.push_back(Node(current_node->get_id(), current_node->get_parent_id()));
				}
			}
			
			// if we found at least one's parent in the first level, we add the number difference between the
			// first level and the next first level to the levels, and then set first level as next_level_base
			// and set other_level as next_level_high. This basically means that we go through all the comments, 
			// and always search for parent connections until we end up having no more.
			if (moved != 0) {
				subreddit->second.add_level(subreddit->second.get_first_level()->size() - next_level_base.size());
				subreddit->second.set_first_level(next_level_base);
				subreddit->second.set_other_level(next_level_high);
			}
		}
		// in the end we add the last level's number to levels
		subreddit->second.add_level(subreddit->second.get_first_level()->size());

		// calculate average depth and add it to the toplist.
		top.add(Pair(subreddit_name, calculate_average_dist(subreddit->second.get_levels())));
	}

}


int main()
{
	// we first create shared assets and pass their reference for the threads, as well as
	// provide the function to execute. 
	SharedFileReader file_reader;
	Subreddits subreddits;

	thread t1(do_work, ref(file_reader), ref(subreddits));
	thread t2(do_work, ref(file_reader), ref(subreddits));
	thread t3(do_work, ref(file_reader), ref(subreddits));
	thread t4(do_work, ref(file_reader), ref(subreddits));
	thread t5(do_work, ref(file_reader), ref(subreddits));
	thread t6(do_work, ref(file_reader), ref(subreddits));
	thread t7(do_work, ref(file_reader), ref(subreddits));
	thread t8(do_work, ref(file_reader), ref(subreddits));

	// wait for every thread to finish.
	t1.join();
	t2.join();
	t3.join();
	t4.join();
	t5.join();
	t6.join();
	t7.join();
	t8.join();
	cout << "Finished with first multithreadding..." << endl;

	// create every asset for the second part and distribute them to the new threads.
	SharedMapReader map_reader(&subreddits);
	TopList top(10);

	thread t11(do_sorting_work, ref(map_reader), ref(subreddits), ref(top));
	thread t12(do_sorting_work, ref(map_reader), ref(subreddits), ref(top));
	thread t13(do_sorting_work, ref(map_reader), ref(subreddits), ref(top));
	thread t14(do_sorting_work, ref(map_reader), ref(subreddits), ref(top));
	thread t15(do_sorting_work, ref(map_reader), ref(subreddits), ref(top));
	thread t16(do_sorting_work, ref(map_reader), ref(subreddits), ref(top));
	thread t17(do_sorting_work, ref(map_reader), ref(subreddits), ref(top));
	thread t18(do_sorting_work, ref(map_reader), ref(subreddits), ref(top));

	// wait for all of them to finish
	t11.join();
	t12.join();
	t13.join();
	t14.join();
	t15.join();
	t16.join();
	t17.join();
	t18.join();
	cout << "Finished with second multithreadding..." << endl;

	// print results...
	top.print();


	cin.get();
	return 0;
}

