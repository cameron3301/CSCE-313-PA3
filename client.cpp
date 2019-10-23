#include "common.h"
#include "BoundedBuffer.h"
#include "Histogram.h"
#include "common.h"
#include "HistogramCollection.h"
#include "FIFOreqchannel.h"
#include <mutex>
using namespace std;

int charsToInt(char* c) {
  string s(c);
  stringstream foo(s);

  int out = 0;
  foo >> out;

  return out;
}


void* file_request_function(BoundedBuffer *buffer, char* _filename, __int64_t file_length)
{
    // create file messages
    int offset = 0;
    const int size = sizeof(filemsg) + strlen(_filename) + 1;
    while (true)
    {
        cout << "fl - o = " << (file_length - offset) << endl;
        if (file_length - offset > MAX_MESSAGE)
        {
            filemsg fm(offset, MAX_MESSAGE);
            char* buf = new char[size];
            memcpy(buf, &fm, sizeof(filemsg));
            memcpy(buf + sizeof(filemsg), _filename, strlen(_filename) + 1);

            vector<char> v(buf, buf + size);
            buffer->push(v);
        }
        else
        {
            int final_chunk = file_length - offset;

            filemsg fm(offset, final_chunk);
            char* buf = new char[size];
            memcpy(buf, &fm, sizeof(filemsg));
            memcpy(buf + sizeof(filemsg), _filename, strlen(_filename) + 1);

            vector<char> v(buf, buf + size);
            buffer->push(v);

            break;
        }
        offset += MAX_MESSAGE;
    }
}


void* patient_function(BoundedBuffer *buf, int person, int n)
{
    double sec = 0;
    for(int i = 0; i < n; i++){
        datamsg dm(person, sec, 1);
        char* req = new char[sizeof(datamsg)];
        memcpy(req, &dm, sizeof(datamsg));
        vector<char> v(req, req + sizeof(datamsg));
        buf->push(v);
        sec += 0.004;
    }
}
    

void* worker_function(BoundedBuffer* buffer, FIFORequestChannel* chan, HistogramCollection* hc, char* _filename)
{   
    // Create the output file
    string out_file;
    if (_filename) {
        string file = _filename;
        out_file = "received/" + file;
    } else {
        out_file = "received/0.csv";
    }    
    ofstream wf(out_file, ios::binary);

    while(true) {
        vector<char> req_v = buffer->pop();
        char* req_c = req_v.data();
        MESSAGE_TYPE m = *(MESSAGE_TYPE*) req_c;

        // datamsg functionality
        if(m == DATA_MSG) {
            chan->cwrite(req_c, sizeof(datamsg));
            char* value = chan->cread();

            datamsg dm = *(datamsg*)req_c;
            hc->updateHist(dm.person, (*(double*)value));   // add data point to histogram

            cout << "value = " << *(double*) value << endl;
        }        
        
        // filemsg functionality
        else if(m == FILE_MSG) {
            filemsg* f = (filemsg*) req_c;

            const int size = sizeof(filemsg) + strlen(_filename) + 1;

            chan->cwrite(req_c, size);
            char* file_data = chan->cread();

            wf.seekp( f->offset);
            wf.write(file_data, f->length);
        }

        // quitmsg functinality
        else if(m == QUIT_MSG) {
            chan->cwrite((char*)&m, sizeof(MESSAGE_TYPE));
            delete chan;
            break;
        }
    }
}


int main(int argc, char *argv[])
{
    /* ----- getopt() functionality ----- */
    int opt;

    char* nChars = NULL;
    char* pChars = NULL;
    char* wChars = NULL;
    char* bChars = NULL;
    char* f = NULL;

    // get input arguments from the user in the command line
    while ((opt = getopt(argc, argv, ":n:p:w:b:f:")) != -1) {
        switch(opt)
        {
            case 'n':
                nChars = optarg;
                break;
            case 'p':
                pChars = optarg;
                break;
            case 'w':
                wChars = optarg;
                break;
            case 'b':
                bChars = optarg;
                break;
            case 'f':
                f = optarg;
                break;
            case ':':
                throw invalid_argument("Missing input value");
                break;
            case '?':
                throw invalid_argument("Unknown tag");
                break;
        }
    }

    int n, p, w, b;

    // validate data request
    if (!f) {
        if (nChars) { n = charsToInt(nChars); }                                    
        else { n = 100; }

        if (pChars) { p = charsToInt(pChars); }
        else { p = 10; }

        if (wChars) { w = charsToInt(wChars); }
        else { w = 100; }

        if (bChars) { b = charsToInt(bChars); }
        else { b = 20; } 

    }
    // validate file requests
    else {
        if (wChars) { w = charsToInt(wChars); }
        else { w = 100; }

        if (bChars) { b = charsToInt(bChars); }
        else { b = 20; }  
    }


    /* ----- begin main functionality ----- */
    
    int m = MAX_MESSAGE; 	// default capacity of the file buffer
    srand(time_t(NULL));
    
    
    int pid = fork();
    if (pid == 0) {
        string m_str = to_string(m);
        const char* m_chars = m_str.c_str();
        execl("./dataserver", m_chars, (char*)NULL);
    }

    
	FIFORequestChannel* chan = new FIFORequestChannel("control", FIFORequestChannel::CLIENT_SIDE);
    BoundedBuffer request_buffer(b);
	HistogramCollection hc;
	
    struct timeval start, end;
    gettimeofday (&start, 0);


    /* ======= DATA_MSG Section ======= */
    if (!f) {
        /* ----- Start all threads here ----- */
        vector<thread> p_threads;
        vector<thread> w_threads;
        vector<FIFORequestChannel*> channels;
        vector<Histogram*> histograms;

        double hist_start_vals[15] = {-0.94, -2.205, -1.875, -1.505, -2.315, -1.06, -1.705, -1.31, -2.79, -0.795, -5.295, -1.08, -1.715, -1.92, -7.825};
        double hist_end_vals[15] = {1.35, 1.11, 0.465, 0.32, 1.495, 0.595, 0.255, 0.44, 3.58, 1.44, 3.81, 0.435, 2.24, 1.235, 7.27};

        // create `p` histograms
        double num_bins = sqrt(((double) n));
        for (int i = 0; i < p; i++) {
            Histogram* h = new Histogram(num_bins, hist_start_vals[i], hist_end_vals[i]);
            histograms.push_back(h);
            hc.add(h); 
        }

        // create `p` patient threads
        for (int i = 1; i <= p; i++) {
            p_threads.push_back(move(thread(patient_function, &request_buffer, i, n)));
        }    
        
        // create `w` new channels
        MESSAGE_TYPE nc_msg = NEWCHANNEL_MSG;
        for (int i = 0 ; i < w ; i++) {
            char* nc_buf = new char [sizeof(MESSAGE_TYPE)];
            memcpy(nc_buf, &nc_msg, sizeof(MESSAGE_TYPE));
            chan->cwrite(nc_buf, sizeof(MESSAGE_TYPE));

            string chan_name = chan->cread();
            FIFORequestChannel *c = new FIFORequestChannel(chan_name, FIFORequestChannel::CLIENT_SIDE);
            channels.push_back(c);
        }

        // create `w` worker threads
        for(int i = 0; i < w; i++) {
            w_threads.push_back(move(thread(worker_function, &request_buffer, channels[i], &hc, f)));
        }


        /* ----- Join all threads here ----- */   

        // join the patient threads
        for (int i = 0; i < p; i++) {
            p_threads[i].join();
        }
        
        // send QUIT_MSG's for each worker thread
        MESSAGE_TYPE q_msg = QUIT_MSG;
        for (int i = 0; i < w; i++) {
            char* qBuf = new char[sizeof(MESSAGE_TYPE)];
            memcpy(qBuf, &q_msg, sizeof(MESSAGE_TYPE));

            vector<char> v(qBuf, qBuf+sizeof(MESSAGE_TYPE));
            request_buffer.push(v);
        } 
        
        // join the worker threads
        for (int i = 0; i < w; i++) {
            w_threads[i].join();
        }


    }
    /* ======= FILE_MSG Section ======= */
    else {
        
        // find the size of the file
        filemsg size_fm(0, 0);
        const int size = sizeof(filemsg) + strlen(f) + 1;
        char* buff = new char[size];
        memcpy(buff, &size_fm, sizeof(filemsg));
        memcpy(buff + sizeof(filemsg), f, strlen(f) + 1);

        int w = chan->cwrite(buff, size);
        __int64_t file_length = *(__int64_t*) chan->cread();

        vector<thread> w_threads;
        vector<FIFORequestChannel*> channels;

        // create patient thread
        thread fq_thread(file_request_function, &request_buffer, f, file_length);

        // create `w` new channels
        MESSAGE_TYPE nc_msg = NEWCHANNEL_MSG;
        for (int i = 0 ; i < w ; i++) {
            char* nc_buf = new char [sizeof(nc_msg)];
            memcpy(nc_buf, &nc_msg, sizeof(nc_msg));
            chan->cwrite(nc_buf, sizeof(nc_msg));

            string chan_name = chan->cread();
            FIFORequestChannel *c = new FIFORequestChannel(chan_name, FIFORequestChannel::CLIENT_SIDE);
            channels.push_back(c);
        }

        // create `w` threads
        for(int i = 0; i < w; i++) {
            w_threads.push_back(move(thread(worker_function, &request_buffer, channels[i], &hc, f)));
        }

        // join the patient thread
        fq_thread.join();

        // send QUIT_MSG's for each worker thread
        MESSAGE_TYPE q_msg = QUIT_MSG;
        for (int i = 0; i < w; i++) {
            char* qBuf = new char[sizeof(q_msg)];
            memcpy(qBuf, &q_msg, sizeof(q_msg));

            vector<char> v(qBuf, qBuf+sizeof(q_msg));
            request_buffer.push(v);
        } 

        // join the worker threads
        for (int i = 0; i < w; i++) {
            w_threads[i].join();
        }
    }

        





    /* ----- Print time taken and close original client channel ----- */
    gettimeofday (&end, 0);
    if (!f) {
        hc.print ();
    }
    int secs = (end.tv_sec * 1e6 + end.tv_usec - start.tv_sec * 1e6 - start.tv_usec)/(int) 1e6;
    int usecs = (int)(end.tv_sec * 1e6 + end.tv_usec - start.tv_sec * 1e6 - start.tv_usec)%((int) 1e6);
    cout << "Took " << secs << " seconds and " << usecs << " micor seconds" << endl;

    MESSAGE_TYPE q = QUIT_MSG;
    chan->cwrite ((char *) &q, sizeof (MESSAGE_TYPE));
    cout << "All Done!!!" << endl;
    delete chan;
    
}
