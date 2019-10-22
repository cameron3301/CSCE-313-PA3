#ifndef BoundedBuffer_h
#define BoundedBuffer_h

#include <iostream>
#include <queue>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
using namespace std;

class BoundedBuffer
{
private:
  	int cap;
  	queue<vector<char>> q;

	/* mutexto protect the queue from simultaneous producer accesses
	or simultaneous consumer accesses */
	mutex mtx;
	
	/* condition that tells the consumers that some data is there */
	condition_variable data_available;
	/* condition that tells the producers that there is some slot available */
	condition_variable slot_available;

public:
	BoundedBuffer(int _cap):cap(_cap){
		cap = _cap;
	}
	~BoundedBuffer(){

	}

	queue<vector<char>> getQueue() {
		return q;
	}

	void push(vector<char> data){
		unique_lock<mutex> l(mtx);
		slot_available.wait (l, [this]{ return q.size() < cap; });
		q.push(data);
		data_available.notify_one();
		l.unlock();
	}

	vector<char> pop(){
		unique_lock<mutex> l(mtx);
		// keep waiting as long as q.size() == 0
		data_available.wait (l, [this]{ return q.size() > 0; });
		// pop from the queue
		vector<char> temp = q.front();
		q.pop();
		// notify any potential producer(s)
		slot_available.notify_one();
		// unlock the mutex so that others can go in
		l.unlock();
		return temp;  
	}
};

#endif /* BoundedBuffer_ */
