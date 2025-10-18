#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <vector>
#include <chrono>
#include <fstream>

using namespace std;

queue<int> buffer;
mutex mtx;
condition_variable cv;

const int TOTAL_ITEMS = 20; 
int produced_count = 0;
int consumed_count = 0;
bool production_done = false; 

void producer(int id, int num_producers) {
    while (true) {
        unique_lock<mutex> lock(mtx);

        if (produced_count >= TOTAL_ITEMS) {
            break;
        }

        int item = ++produced_count;
        buffer.push(item);

        cout << "Producer " << id << " produced item " << item << endl;

        lock.unlock();
        cv.notify_all(); 

        this_thread::sleep_for(chrono::milliseconds(100));
    }

    unique_lock<mutex> lock(mtx);
    if (produced_count >= TOTAL_ITEMS)
        production_done = true;
    lock.unlock();
    cv.notify_all();
}

void consumer(int id) {
    while (true) {
        unique_lock<mutex> lock(mtx);

        cv.wait(lock, [] { return !buffer.empty() || production_done; });

        if (!buffer.empty()) {
            int item = buffer.front();
            buffer.pop();
            consumed_count++;
            cout << "Consumer " << id << " consumed item " << item << endl;
        } 
        else if (production_done && buffer.empty()) {
            break;
        }

        lock.unlock();
        cv.notify_all(); 

        this_thread::sleep_for(chrono::milliseconds(150));

        if (consumed_count >= TOTAL_ITEMS)
            break;
    }
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        cerr << "Usage: " << argv[0] << " <num_producers> <num_consumers>\n";
        return 1;
    }

    int num_producers = stoi(argv[1]);
    int num_consumers = stoi(argv[2]);

    if (num_producers < 4 || num_consumers < 3) {
        cerr << "Error: At least 4 producers and 3 consumers are required.\n";
        return 1;
    }

    cout << "Starting with " << num_producers << " producers and "
         << num_consumers << " consumers..." << endl;

    vector<thread> producers, consumers;

    for (int i = 0; i < num_producers; ++i)
        producers.emplace_back(producer, i + 1, num_producers);

    for (int i = 0; i < num_consumers; ++i)
        consumers.emplace_back(consumer, i + 1);

    for (auto& p : producers) p.join();
    for (auto& c : consumers) c.join();

    cout << "\nAll production and consumption completed successfully!" << endl;
    cout << "Total items produced: " << produced_count << endl;
    cout << "Total items consumed: " << consumed_count << endl;

    ofstream out("output.txt");
    out << "Producer-Consumer Simulation Results\n";
    out << "------------------------------------\n";
    out << "Producers: " << num_producers << "\n";
    out << "Consumers: " << num_consumers << "\n";
    out << "Total items produced: " << produced_count << "\n";
    out << "Total items consumed: " << consumed_count << "\n";
    out << "Simulation completed successfully.\n";
    out.close();

    return 0;
}
