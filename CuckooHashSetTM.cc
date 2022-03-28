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
#include <algorithm>    
#include <future>

using namespace std;



template<class T>
class CuckooHashSetTM {

private:
	const static int LOCKS_SIZE = 1000;
	const static int LIMIT = 10000;
	const static int PROBE_SIZE = 4;
	const static int THRESHOLD = 2;

	//std::recursive_mutex locks[2][LOCKS_SIZE];
	vector<vector<T>> table[2];
	int capacity;
	int currTable;
	int hash0Carry, hash1Carry;

	int hash0(T value) {
		//printf("%d\n", (int)std::hash<T>()(value));
		int hash = std::hash<T>()(value) + hash0Carry;
		//printf("hash 2: %d\n", hash);
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
	CuckooHashSetTM(int size) {
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

	__attribute__ ((transaction_pure))
	bool add(T value) {
		atomic_noexcept {

			if (contains(value)) {
				return false;
			}

			int i = -1, h = -1;
			bool mustResize = false;

			vector<T> set0 = table[0][hash0(value)];
			vector<T> set1 = table[1][hash1(value)];


			int set0Size = probeSetSize(set0);
			int set1Size = probeSetSize(set1);
			if (set0Size < THRESHOLD) {
				table[0][hash0(value)][set0Size] = value;
				return true;
			}
			else if (set1Size < THRESHOLD) {
				table[1][hash1(value)][set1Size] = value;
				return true;
			}
			else if (set0Size < PROBE_SIZE) {
				table[0][hash0(value)][set0Size] = value;
				i = 0;
				h = hash0(value);
			}
			else if (set1Size < PROBE_SIZE) {
				table[1][hash1(value)][set1Size] = value;
				i = 1;
				h = hash1(value);
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


	}

	__attribute__ ((transaction_pure))
	bool relocate(int i, int hi) {
		atomic_noexcept {
		int hj = 0;
		int j = 1 - i;
		for (int round = 0; round < LIMIT; round++) {
			vector<T> iSet = table[i][hi];
			T y = iSet[0];

			if (i == 0) {
				hj = hash1(y);
			} else {
				hj = hash0(y);
			}
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
	}

	__attribute__ ((transaction_pure))
	bool contains(T value) {
		//printf("got into contains\n");

		atomic_noexcept {
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
	}

	__attribute__ ((transaction_pure))
	void resize() {
		atomic_noexcept {
			int oldCapacity = capacity;
			if (capacity != oldCapacity) {
				return;
			}

			vector<vector<T>> oldTable[2];
			oldTable[0].resize(oldCapacity);
			oldTable[1].resize(oldCapacity);

			for (int i = 0; i < 2; i++) {
				for (int j = 0; j < capacity; j++) {
					oldTable[i][j] = table[i][j];
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

	}

	__attribute__ ((transaction_pure))
	bool remove(T value) {
		atomic_noexcept {


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
	CuckooHashSetTM<int> &set,
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

	CuckooHashSetTM<int> cuckooHashSet(100000);
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