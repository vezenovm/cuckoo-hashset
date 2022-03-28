#include <vector>
#include <cassert>
#include <cstdlib>
#include <string>
#include <iostream>
#include <unistd.h>
#include <cstring>
#include <thread>
#include <chrono>
#include <mutex>
#include <map>
#include <algorithm>    
#include <future>

using namespace std;



template<class T>
class CuckooHashSetConcurrent {

private:
	const static int LOCKS_SIZE = 1000;
	const static int LIMIT = 50;
	const static int PROBE_SIZE = 4;
	const static int THRESHOLD = 2;

	std::recursive_mutex locks[2][LOCKS_SIZE];
	vector<vector<T>> table[2];
	int capacity;
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

	int probeSetSize(vector<T> set) {
		//printf("got here");
		int count = 0;
		while (count < (int)set.size()) {
			if (set[count] == 0) {
				break;
			}
			count++;
		}
		return count;
	}

public:
	CuckooHashSetConcurrent(int size) {
		this->capacity = size;
		table[0].resize(capacity);
		table[1].resize(capacity);

		for (int i = 0; i < 2; i++) {
			for (int j = 0; j < capacity; j++) {
				table[i][j].resize(PROBE_SIZE);
			}
		}

		this->hash0Carry = 2;
		this->hash1Carry = 3;
	}

	bool add(T value) {
		//T y = NULL;

		acquire(value);
		int i = -1, h = -1;
		bool mustResize = false;

		//printf("capacity: %d\n", capacity);


		if (contains(value)) {
			return false;
		}

		vector<T> set0 = table[0][hash0(value)];
		vector<T> set1 = table[1][hash1(value)];

		//printf("hash 0: %d\n", hash0(value));
		//printf("hash 1: %d\n", hash1(value));


		int set0Size = probeSetSize(set0);
		int set1Size = probeSetSize(set1);
		//printf("set 0 size: %d\n", set0Size);
		//printf("set 1 size: %d\n", set1Size);
		if (set0Size < THRESHOLD) {
			//printf("added to set 0\n");
			table[0][hash0(value)][set0Size] = value;
		//printf("probe set 1 size: %d\n", probeSetSize(set0));
		//printf("probe set 2 size: %d\n", probeSetSize(set1));
			// for (auto it = set0.begin(); it != set0.end(); ++it) {
			// 	printf("%d\n", *it);
			// }
			//printf("%d\n", table[0][hash0(value)][set0Size]);
			//wprintf("got here 2\n");
			//release(value);
			return true;
		}
		else if (set1Size < THRESHOLD) {
			//printf("added to set 1\n");
			table[1][hash1(value)][set1Size] = value;
			//printf("%d\n", set1[set1Size]);
			//printf("got here 3\n");
			//release(value);
			return true;
		}
		else if (set0Size < PROBE_SIZE) {
			//printf("added to set 0\n");
			//set0[set0Size] = value;
			table[0][hash0(value)][set0Size] = value;
			i = 0;
			h = hash0(value);
			//printf("%d\n", set0[set0Size]);
			//printf("got here 4\n");
		}
		else if (set1Size < PROBE_SIZE) {
			//printf("added to set 1\n");
			table[1][hash1(value)][set1Size] = value;
			i = 1;
			h = hash1(value);
			//printf("%d\n", set1[set1Size]);
			//printf("got here 5\n");
		}
		else {
			mustResize = true;
		}

		if (mustResize) {
			printf("%d\n", mustResize);
			resize();
			add(value);
		}
		else if (!relocate(i, h)) {
			resize();
		}
		return true;
	}

	bool relocate(int i, int hi) {
		int hj = 0;
		int j = 1 - i;
		acquire(table[i][hi][0]);
		for (int round = 0; round < LIMIT; round++) {
			vector<T> iSet = table[i][hi];
			T y = iSet[0];

			if (i == 0) {
				hj = hash1(y);
			} else {
				hj = hash0(y);
			}
			//acquire(y);
			vector<T> jSet = table[j][hj];

			bool eraseResult = false;
			int index = 0;
			for (auto it = iSet.begin(); it != iSet.end(); ++it) {
				index++;
				if (*it == y) {
					table[i][hi][index] = 0;
					//iSet.erase(it);
					eraseResult = true;
					break;
				}
			}
			int jSetSize = probeSetSize(jSet);
			int iSetSize = probeSetSize(iSet);
			if (eraseResult) {
				if (jSetSize < THRESHOLD) {
					//jSet[jSetSize] = y;
					table[j][hj][jSetSize] = y;
					//release(y);
					return true;
				}
				else if (jSetSize < PROBE_SIZE) {
					//jSet[jSetSize] = y;
					table[j][hj][jSetSize] = y;
					i = 1 - i;
					hi = hj;
					j = 1 - j;
					//release(y);
				}
				else {
					//iSet[iSetSize] = y;
					table[i][hi][iSetSize] = y;
					//release(y);
					return false;
				}
			}
			else if (iSetSize >= THRESHOLD) {
				//release(y);
				continue;
			}
			else {
				//release(y);
				return true;
			}
			//release(y);
		}
		return false;
	}

	bool contains(T value) {
		//printf("got into contains\n");
		acquire(value);

		vector<T> set0 = table[0][hash0(value)];
		vector<T> set1 = table[1][hash1(value)];


		for (auto it = set0.begin(); it != set0.end(); ++it) {
			if (*it == value) {
				return true;
			}
		}

		for (auto it = set1.begin(); it != set1.end(); ++it) {
			if (*it == value) {
				//release(value);
				return true;
			}
		}
		//printf("nah i dont got it\n");
		//release(value);
		return false;

	}

	void resize() {
		//printf("got into resize");
		int oldCapacity = capacity;
		// for (auto it = locks[0].begin(); it != locks[0].end(); ++it) {
		// 	*it.lock();
		// }
		for (int i = 0; i < LOCKS_SIZE; i++) {
			// std::recursive_mutex &currMutex = locks[0][i];
			// currMutex.lock();
			std::unique_lock<std::recursive_mutex> lock1(locks[0][i]);
			//std::unique_lock<std::recursive_mutex> lock2(locks[1][i]);
		}
		// for (int i = 0; i < LIMIT; i++) {
		// 	recursive_mutex &currMutex = locks[1][i];
		// 	currMutex.lock();
		// }

		if (capacity != oldCapacity) {
			return;
		}

		vector<vector<T>> oldTable[2];
		oldTable[0].resize(oldCapacity);
		oldTable[1].resize(oldCapacity);

		for (int i = 0; i < 2; i++) {
			for (int j = 0; j < capacity; j++) {
				oldTable[i][j] = table[i][j];
				for (auto it = oldTable[i][j].begin(); it != oldTable[i][j].begin(); ++it) {
		 			printf("%d\n", *it);
		 		}
			}
		}

		capacity = capacity * 2;

		table[0].resize(capacity);
		table[1].resize(capacity);
		for (int i = 0; i < 2; i++) {
			for (int j = 0; j < capacity; j++) {
				table[i][j].resize(PROBE_SIZE);
			}
		}

		for (int i = 0; i < 2; i++) {
			for (int j = 0; j < oldCapacity; j++) {
				for (auto it = oldTable[i][j].begin(); it != oldTable[i][j].end(); ++it) {
					add(*it);
				}
			}
		}
	}

	bool remove(T value) {
		acquire(value);

		int valHash0 = hash0(value);
		int valHash1 = hash1(value);

		vector<T> set0 = table[0][valHash0];
		int index = 0;
		for (auto it = set0.begin(); it != set0.end(); ++it) {
			index++;
			if (*it == value) {
				table[0][valHash0][index] = 0;
				//release(value);
				return true;
			}
		}

		vector<T> set1 = table[1][valHash1];
		index = 0;
		for (auto iter = set1.begin(); iter != set1.end(); ++iter) {
			index++;
			if (*iter == value) {
				table[1][valHash1][index] = 0;
				//release(value);
				return true;
			}
		}
		//release(value);

		return false;
	}

	int size() {

		int numElems = 0;
		for (int i = 0; i < 2; i++) {
			for (int j = 0; j < capacity; j++) {
				for (auto it = table[i][j].begin(); it != table[i][j].end(); ++it) {
					if (*it != 0) {
						numElems++;
						//printf("%d ", *it);
					}					
				}
			}
		}
		printf("capacity: %d\n", capacity);
		return numElems;

	}

	void acquire(T value) {
		// std::recursive_mutex &firstMutex = locks[0][hash0(value)];
		// firstMutex.lock();
		// std::recursive_mutex &secondMutex = locks[1][hash1(value)];
		// secondMutex.lock();
		std::unique_lock<std::recursive_mutex> lock1(locks[0][hash0(value) % LOCKS_SIZE]);
		std::unique_lock<std::recursive_mutex> lock2(locks[1][hash1(value) % LOCKS_SIZE]);
	}

	// void release(T value) {
	// 	std::recursive_mutex &firstMutex = locks[0][hash0(value)];
	// 	firstMutex.unlock();
	// 	std::recursive_mutex &secondMutex = locks[1][hash1(value)];
	// 	secondMutex.unlock();
	// }
 
 	int populate(vector<T> values) {
		//printf("started populate\n");
 		int amount = 0;
 		for (auto it = values.begin(); it != values.end(); ++it) {
 			//printf("about to add\n");
 			if (add(*it) == true) {
 				amount++;
 				//printf("finished add\n");
 			}
 		}
 		return amount;

 	}

};

pair<double, int> do_work(
	CuckooHashSetConcurrent<int> &set,
	vector<int> &workDistribution,
	vector<int> &randomValues
) {
	auto start = std::chrono::system_clock::now();
	std::atomic<int> expectedSize(0);
	int index = 0;
	for (auto it = workDistribution.begin(); it != workDistribution.end(); ++it) {
		//printf("started work iter %d ", index);
		//printf("operation: %d ", *it);
		//printf("randomValue: %d\n", randomValues[index]);

		if (*it == 0) {
			//workOutput.push_back(std::make_pair(i, set.contains(randomValues[i])));
			set.contains(randomValues[index]);
		}
		else if (*it == 1) {
			//workOutput.push_back(std::make_pair(i, set.add(randomValues[i])));
			if (set.add(randomValues[index]) == true) {
				expectedSize++;
			}
		}
		else if (*it == 2) {
			//workOutput.push_back(std::make_pair(i, set.remove(randomValues[i])));
			if (set.remove(randomValues[index]) == true) {
				expectedSize--;
			}
		}
		index++;
	}
	auto end = std::chrono::system_clock::now();

	double elapsed = std::chrono::duration_cast<chrono::duration<double>>(end - start).count();
	
	pair<double, int> result = make_pair(elapsed, expectedSize.load());
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

	map<int, pair<vector<int>, vector<int>>> workBreakdown; 
	config_t config;
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

	CuckooHashSetConcurrent<int> cuckooHashSet(100000);
	printf("just initialized hashset\n");

	vector<int> initialValues(config.iters);
	vector<int> seenValues;
	for (int i = 0; i < config.iters - 1; i++) {
		//printf("%d\n", i);
		initialValues[i] = 5;
		//initialValues[i] = i+1;
	}
	initialValues[config.iters-1] = 2;
	seenValues.push_back(5);
	seenValues.push_back(2);

	int total_size = cuckooHashSet.populate(initialValues);
	printf("size after populate: %d\n", total_size);
	vector<int> workDistribution(config.iters);
	for (int i = 0; i < config.iters; i++) {
		int rand_value = rand() % 100;
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
	}

	vector<int> randomValues(config.iters);
	for (int i = 0; i < config.iters; i++) {
		int randomValue = i+1;
		randomValues[i] = randomValue;
	}

	int minBucketIndex = 0;
	int maxBucketIndex = config.iters / config.numThreads;
	for (int i = 0; i < config.numThreads; i++) {
		vector<int> workDistrib(config.iters / config.numThreads);
		vector<int> randValues(config.iters / config.numThreads);
		for (int j = minBucketIndex, k = 0; j < maxBucketIndex; j++, k++) {
			workDistrib[k] = workDistribution[j];
			randValues[k] = randomValues[j];
		}
		minBucketIndex += config.iters / config.numThreads;
		maxBucketIndex += config.iters / config.numThreads;
		workBreakdown.insert( pair<int, pair<vector<int>, vector<int>>>(i, std::make_pair(workDistrib, randValues))); 

	}


	printf("made random variables\n");


	int index = 0;
	int size = total_size;

  	for (auto it = workDistribution.begin(); it != workDistribution.end(); ++it) {
  		auto seenValIter = find(seenValues.begin(), seenValues.end(), randomValues[index]);
		if (*it == 1 && seenValIter == seenValues.end()) {
			size++;
			seenValues.push_back(randomValues[index]);
		}
		else if (*it == 2 && seenValIter != seenValues.end()) {
			seenValues.erase(seenValIter);
			size--;
		}
		index++;
	}
	printf("seen values size: %d\n", size);


	std::thread threads[config.numThreads];
	vector<double> loop_times(config.numThreads);
	vector<int> loop_sizes(config.numThreads);

	printf("made threads\n");

	for (int i = 0; i < config.numThreads; i++) {

		std::future<pair<double, int>> timeSizePairFuture = std::async(std::launch::async, [&] {
			return do_work(
				cuckooHashSet,
				workBreakdown[i].first,
				workBreakdown[i].second
			);
		});
		pair<double, int> timeSizePair = timeSizePairFuture.get();
		loop_times[i] = timeSizePair.first;
		loop_sizes[i] = timeSizePair.second;
	}

	double total_time = 0;
	int sizeFromFutures = 0;
	for (int i = 0; i < config.numThreads; i++) {
		printf("do_work time for thread %d: %.7lf\n", i, loop_times[i]);
		total_time += loop_times[i];
		sizeFromFutures += loop_sizes[i];
	}

	printf("Total Time: %.7lf\n", total_time);

	printf("Expected Size 1.0: %d\n", sizeFromFutures);
	printf("Expected Size 2.0: %d\n", size);
	printf("Ending Size: %d\n", cuckooHashSet.size());

}