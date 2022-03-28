#include <vector>
#include <cassert>
#include <cstdlib>
#include <string>
#include <iostream>
#include <unistd.h>
#include <cstring>
#include <thread>
#include <chrono>
#include <future>

using namespace std;

template<class T>
class CuckooHashSetSequential {

private:
	vector<T> table[2];
	int capacity;
	int LIMIT = 50;
	int currTable;
	int hash0Carry, hash1Carry;

	int hash0(T value) {
		int hash = std::hash<T>()(value) + hash0Carry;
		return hash % capacity;
	}

	int hash1(T value) {
		int hash = std::hash<T>()(value) + hash1Carry;
		return hash % capacity;
	}

	T swap(int hash, T value) {
		T oldValue;
		if ((oldValue = table[0][hash]) == 0) {
			table[0][hash] = value;
			return oldValue;
		}
		else if ((oldValue = table[1][hash]) == 0) {
			table[1][hash] = value;
			return oldValue;
		}
		else {
			for (int i = 0; i < 2; i++) {
				if (table[i][hash] != 0) {
					oldValue = table[i][hash];
					table[i][hash] = value;
					break;
				}
			}
		}
		return oldValue;
	}

public:
	CuckooHashSetSequential(int size) {
		this->capacity = size;
		table[0].resize(capacity);
		table[1].resize(capacity);
		this->hash0Carry = 2;
		this->hash1Carry = 3;
	}

	bool add(T value) {
		if (contains(value)) {
			return false;
		}

		for (int i = 0; i < LIMIT; i++) {
			if ((value = swap(hash0(value), value)) == 0) {
				return true;
			}
			else if ((value = swap(hash1(value), value)) == 0) {
				return true;
			}
		}

		resize();
		return add(value);
	}

	bool contains(T value) {

		if (table[0][hash0(value)] == value) {
			return true;
		}
		else if (table[1][hash1(value)] == value) {
			return true;
		}
		return false;

	}

	void resize() {

		vector<T> oldTable[2];
		oldTable[0] = table[0];
		oldTable[1] = table[1];

		capacity = capacity * 2;
		vector<T> newTable[2];
		newTable[0].resize(capacity);
		newTable[1].resize(capacity);

		table[0] = newTable[0];
		table[1] = newTable[1];

		for (int i = 0; i < 2; i++) {
			for (auto it = oldTable[i].begin(); it != oldTable[i].end(); ++it) {
				add(*it);
			}
		}

	}

	bool remove(T value) {

		int valHash0 = hash0(value);
		int valHash1 = hash1(value);
		
		if (table[0][valHash0] == value) {
			table[0][valHash0] = 0;
			return true;
		}
		else if (table[1][valHash1] == value) {
			table[1][valHash1] = 0;
			return true;
		}

		return false;
	}

	int size() {

		int numElems = 0;
		for (int i = 0; i < 2; i++) {
			for (auto it = table[i].begin(); it < table[i].end(); ++it) {
				if (*it != 0) {
					numElems++;
				}
			}
		}
		return numElems;

	}
 
 	int populate(vector<T> values) {

 		int amount = 0;
 		for (auto it = values.begin(); it != values.end(); ++it) {
 			if (add(*it) == true) {
 				amount++;
 			}
 		}
 		return amount;

 	}

};

pair<double, int> do_work(
	CuckooHashSetSequential<int> &set,
	vector<int> &workDistribution,
	vector<int> &randomValues
) {
	auto start = std::chrono::system_clock::now();
	int expectedSize = 0;
	int index = 0;
	for (auto it = workDistribution.begin(); it != workDistribution.end(); ++it) {
		// printf("started work iter %d ", index);
		// printf("operation: %d ", *it);
		// printf("randomValue: %d\n", randomValues[index]);
		
		if (*it == 0) {
			set.contains(randomValues[index]);
		}
		else if (*it == 1) {
			if (set.add(randomValues[index]) == true) {
				expectedSize++;
			}
		}
		else if (*it == 2) {
			if (set.remove(randomValues[index]) == true) {
				expectedSize--;
			}
		}
		index++;
	}
	auto end = std::chrono::system_clock::now();
	double elapsed = std::chrono::duration_cast<chrono::duration<double>>(end - start).count();
	
	pair<double, int> result = make_pair(elapsed, expectedSize);
	return result;
}

struct config_t {

	   // The number of iterations for which a test should run
    int iters;

    // The number of threads to use
    int numThreads;

    // simple constructor
    config_t() : iters(1024), numThreads(1) { }

};

int main(int argc, char** argv) {

	config_t config;
	//parseargs(argc, argv, config);
	int opt;
    while ((opt = getopt(argc, argv, "i:t:")) != -1) {
    	if (atoi(optarg) != 0) {
    		switch (opt) {
          		case 'i': config.iters = atoi(optarg); break;
          		case 't': config.numThreads = atoi(optarg); break;
        	}
    	}

    }
    printf("%d\n", config.iters);
    printf("%d\n", config.numThreads);

	CuckooHashSetSequential<int> cuckooHashSet(100);

	vector<int> initialValues(config.iters);
	for (int i = 0; i < config.iters-1; i++) {
		//initialValues[i] = i+1;
		initialValues[i] = 5;
	}
	initialValues[config.iters - 1] = 2;

	int total_size = cuckooHashSet.populate(initialValues);

	vector<int> workDistribution(config.iters);
	vector<int> randomValues(config.iters);

	for (int i = 0; i < config.iters; i++) {
		int rand_value = rand() % 100;
		//printf("rand value: %d\n", rand_value);
		if (rand_value < 50) {
			workDistribution[i] = 0;
			continue;
		}
		else if (rand_value < 75.0) {
			workDistribution[i] = 1;
			continue;
		}
		else {
			workDistribution[i] = 2;
		}
		//randomValues[i] = rand_value;
	}

	for (int i = 0; i < config.iters; i++) {
		int randomValue = rand() % 100;
		randomValues[i] = randomValue;
	}
	std::thread threads[config.numThreads];
	vector<double> loop_times(config.numThreads);
	vector<int> loop_sizes(config.numThreads);

	for (int i = 0; i < config.numThreads; i++) {

		std::future<pair<double, int>> timeSizePairFuture = std::async(std::launch::async, [&] {
			return do_work(
				cuckooHashSet,
				workDistribution,
				randomValues
			);
		});
		pair<double, int> timeSizePair = timeSizePairFuture.get();
		loop_times[i] = timeSizePair.first;
		loop_sizes[i] = timeSizePair.second;
	}

	double total_time = 0;
	for (int i = 0; i < config.numThreads; i++) {
		printf("do_work time for thread %d: %.7lf\n", i, loop_times[i]);
		total_time += loop_times[i];
		total_size += loop_sizes[i];
	}

	printf("Total Time: %.7lf\n", total_time);

	printf("Expected: Size: %d\n", total_size);
	printf("Ending Size: %d\n", cuckooHashSet.size());

}