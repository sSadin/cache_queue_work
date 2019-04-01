#include "safe_ptr.h"

//#include <chrono>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>


using namespace std;

//constexpr size_t MAX_CACHE_WR_COUNT  = 40;
//constexpr size_t MAX_CACHE_WR_TASKS  = 4;
template<typename T>
using w_lock      = std::lock_guard<T>;


using cache_wr_mem_t    = char*;
auto cache_wr_mem_allocator = [](const size_t &size){return new char[size];};
auto cache_wr_mem_deleter = [](cache_wr_mem_t &cache_wr_mem){if(cache_wr_mem!=nullptr)delete []cache_wr_mem; cache_wr_mem=nullptr;};
using Hash              = string;


struct FileInfo{
    Hash        hash;
    size_t      size;       // file size
    size_t      offset;     // offset from the beginning of the file from which the buffer starts
    size_t      length;     // buffer length
    static constexpr uint raw_size() { return sizeof(FileInfo); }
};


class CacheWrite{
public:
    CacheWrite()                    = default;
    CacheWrite(CacheWrite const&)   = delete;
    CacheWrite(CacheWrite &&)       = delete;
    CacheWrite& operator=(CacheWrite const&)   = delete;
    CacheWrite& operator=(CacheWrite &&)       = delete;
    ~CacheWrite()                    = default;

    struct cache_obj_t{
        FileInfo        fi;
        cache_wr_mem_t  data;
        //size_t          timestamp = 0;//std::chrono::high_resolution_clock::now();
        //~cache_obj_t() {cache_wr_mem_deleter(data);}
    };

    cache_wr_mem_t new_obj(const size_t &length);       // get memory allocated in NVDIMM
//    friend cache_wr_mem_t cache_wr_new(const size_t &);
    void add(const FileInfo &fi, cache_wr_mem_t mem);   // add object desc to list of object in NVDIMM
//    friend void cache_wr_add(const FileInfo &, cache_wr_mem_t);

    cache_obj_t get();                                  // get the object struct with locking thread (if no objects in queue)
    static void del_obj(cache_obj_t &obj);              // delete from cache object (if task completed successfully)
    void clear();                                       // clears object queue

    operator bool() {//TODO: not need this function?
        return !cache->empty();
    }

protected:
    bool whait_for_objects(); //TODO: not needed - thread locks in get()?

private:
    safe_ptr<std::queue<cache_obj_t>> cache;
    mutex               lock;
    condition_variable  cv;
    bool                ready = false;
    bool                processed = false;
};


cache_wr_mem_t CacheWrite::new_obj(const size_t &length){
    return move(cache_wr_mem_allocator(length));
}

void CacheWrite::add(const FileInfo &fi, cache_wr_mem_t mem){
                                                                            cout << "Emplace object to CacheWrite" << endl;
    cache->push(
        cache_obj_t{.fi         = fi,
                    .data       = mem,
                    //.timestamp  = 0
        } );
                                                                            cout << "Notifying cache_wr_thread" << endl;
    cv.notify_one();
    return;
}


void CacheWrite::del_obj(CacheWrite::cache_obj_t &obj) {
    cache_wr_mem_deleter(obj.data);
}



bool CacheWrite::whait_for_objects() {
                                                                            cout << "Wait for objects" << endl;
    unique_lock<mutex> lk(lock);
#if 1
    cv.wait(lk, [this]{return cache->size() > 0;});
                                                                            cout << "CacheWrite has " << cache->size() << " objects" << endl;
    return true;
#else
    while (true) {
        if (cv.wait_for(lk, 1000ms, [this]{return cache->size() >= MAX_CACHE_WR_COUNT;})) {
                                                                            cout << "CacheWrite has over/equal " << MAX_CACHE_WR_COUNT << " objects" << endl;
            continue;
        }
        if (cache->size() > 0) {
            // timeout and has objects
                                                                            cout << "CacheWrite has " << cache->size() << " objects" << endl;
            return;
        }
    }
#endif
}

CacheWrite::cache_obj_t CacheWrite::get() {
    unique_lock<mutex> lk(lock);
    cv.wait(lk, [this]{return cache->size() > 0;});
//        if (cache->empty())
//            return {};
//        lock_timed_any_infinity lock_all(cache);
    cache_obj_t obj = cache->front();
    cache->pop();
    return obj;
}


void CacheWrite::clear() {
    lock_timed_any_infinity lock_all(cache);
    while(cache->empty() != true) {
        del_obj(cache->front());
        cache->pop();
    }
}


struct cache_wr_task_t{
    mutex               lock;
    condition_variable  cond_task;
    enum {
        NON,
        NEW,
        COMPLETED,
        FAILED,
    }                   status = NON;

};


constexpr int MIN_WROKERS_COUNT = 10;


struct cache_wr_job_t {
    cache_wr_task_t         task;
    CacheWrite::cache_obj_t obj;
};

using shptr_cache_wr_job_t = shared_ptr<cache_wr_job_t>;
using shptr_cond_var = shared_ptr<condition_variable>;
using worker_func_t = function<void(shptr_cache_wr_job_t job)>;


void cache_wr_thread(CacheWrite &cache_wr) {
    //
    condition_variable  cond_broker;
    //TODO: threads count may be changed if count of jobs match more then count of workers?
    //vector<shptr_cache_wr_job_t> jobs(MIN_WROKERS_COUNT);
    safe_ptr<queue<shptr_cache_wr_job_t>> free_jobs_queue;

    function<void(shptr_cache_wr_job_t)> post_job_hook =
            [&free_jobs_queue, &cond_broker/*, &cache_wr*/](shptr_cache_wr_job_t job){
                if (job->task.status == cache_wr_task_t::COMPLETED) {
                    /*cache_wr.*/CacheWrite::del_obj(job->obj);
                    //TODO: check for free_jobs_queue don't have his job
                    free_jobs_queue->push(job);
                    cond_broker.notify_one();
                }
                else if (job->task.status == cache_wr_task_t::FAILED)
                    static_assert(true);
            };

    worker_func_t worker = [&post_job_hook](shptr_cache_wr_job_t job) {
        while (true) {
            unique_lock<mutex> lock(job->task.lock);
            job->task.cond_task.wait(lock, [&job]{return job->task.status == cache_wr_task_t::NEW;});

            auto &obj = job->obj;
            stringstream ss;
            ss << "----------------------------------"  << endl <<
                  "hash:   \t" << obj.fi.hash           << endl <<
                  "size:   \t" << obj.fi.size           << endl <<
                  "offset: \t" << obj.fi.offset         << endl <<
                  "length: \t" << obj.fi.length         << endl <<
                  "data:   \t" << obj.data              << endl;
            cout << ss.str();

            this_thread::sleep_for(10ms);

            job->task.status = cache_wr_task_t::COMPLETED;
            post_job_hook(job);
        }
        return;
    };
    //for(shptr_cache_wr_job_t &job : jobs) {
        //job = shptr_cache_wr_job_t{new cache_wr_job_t};
    for( size_t i = 0; i < MIN_WROKERS_COUNT; ++i) {
        shptr_cache_wr_job_t job = shptr_cache_wr_job_t{new cache_wr_job_t};
        thread {worker, job}.detach();
        free_jobs_queue->push(job);
    }

    while (true) { //TODO: make interruptable thread
        mutex cache_wr_mtx;
        unique_lock<mutex> cache_wr_lock(cache_wr_mtx);
        cond_broker.wait(cache_wr_lock,[&free_jobs_queue]{return free_jobs_queue->empty() != true;});
        while(free_jobs_queue->empty() != true) {
            //cache_wr.whait_for_objects();//noone takes object, expect this thread!
            shptr_cache_wr_job_t job;
            {
                lock_timed_any_infinity queue_lock(free_jobs_queue);
                job = free_jobs_queue->front();
                free_jobs_queue->pop();
            }
            job->obj = cache_wr.get();
            //TODO: if(!job->obj) ???;
            job->task.status = cache_wr_task_t::NEW;
            job->task.cond_task.notify_one();
        }
    }
    return;
}


//========================================================================================================================


constexpr ssize_t threads_count = 15;
int main()
{
    cout << "Hello World!" << endl;
    CacheWrite cw;

    thread{cache_wr_thread, ref(cw)}.detach();

    vector<thread> threads(threads_count);
    for(size_t i=0; i < threads_count; ++i) {
        threads.emplace_back([&cw,i]()
        {
            stringstream ss;
            ss << "From thread. Index:\t" << i << "\t|\tThis thread: " << this_thread::get_id() << endl;
            cout << ss.str();
            string str(ss.str());
            size_t cw_cell_size = str.size()+1;

            cache_wr_mem_t buf = cw.new_obj(cw_cell_size);
            copy(str.c_str(),str.c_str() + cw_cell_size, buf);

            cw.add( { .hash = {},
                      .size = 1000+i,//cw_cell_size,
                      .offset = 0xDeadBeeF,
                      .length = cw_cell_size },
                    buf );
        });
    }

    getchar();
    return 0;
}
