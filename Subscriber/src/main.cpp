#include <Python.h>
#include <thread>
#include <chrono>
#include <vector>
#include <fstream>
#include <jsoncpp/json/json.h>
#include <iostream>
#include <string>
#include <unordered_map>
#include <iostream>
#include <mutex>
#include <ctime>
#include <signal.h>
#include "msg/KeyValuePubSubTypes.h"
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/DataReaderListener.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>
#include <fastdds/rtps/transport/TCPv4TransportDescriptor.h>
#include <fastrtps/utils/IPLocator.h>

using IPLocator = eprosima::fastrtps::rtps::IPLocator;
using namespace eprosima::fastdds::dds;
using namespace eprosima::fastdds::rtps;
using namespace eprosima::fastdds::dds;

volatile sig_atomic_t stop;
std::string path;
std::mutex mtx;
void inthand(int signum) {stop = 1;}

class Singleton {
public:
    static Singleton& getInstance() {
        static Singleton instance;
        return instance;
    }

    void add(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_map[key] = value;
    }

    std::unordered_map<std::string, std::string> getMap() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_map;
    }

private:
    Singleton() = default;
    Singleton(const Singleton&) = delete;
    Singleton& operator=(const Singleton&) = delete;

    std::unordered_map<std::string, std::string> m_map;
    mutable std::mutex m_mutex;
};


struct initialize // Python interpreterini initialize eden yapı
{
    initialize()
    {
        Py_InitializeEx(1);
        PyEval_InitThreads();
    }

    ~initialize()
    {
        Py_Finalize();
    }
};
// Çoklu işlem için Python'u initialize etmemizi sağlıyor.
class enable_threads
{
public:
    enable_threads()
    {
        _state = PyEval_SaveThread();
    }

    ~enable_threads()
    {
        PyEval_RestoreThread(_state);
    }

private:
    PyThreadState* _state;
};

void setvalue(std::string key, std::string value) {
    PyGILState_STATE _state;
    _state = PyGILState_Ensure();
    //PyRun_SimpleString("import os");
    std::string filePath = "sys.path.append(\"./";
    filePath += path;
    filePath += "\")";
    //PyRun_SimpleString(filePath.c_str());
    //dosya giriyoruz ve kodu 
    PyObject* main_module = PyImport_ImportModule("libpy");//[ ] Buraya nasıl ulaşıyor anlamadım
    PyObject* main_dict = PyModule_GetDict(main_module); 
    PyObject* func = PyDict_GetItemString(main_dict, "setvalue");
    Py_DECREF(main_module);
    //
    PyObject_CallObject(func, (PyObject*)Py_BuildValue("(ss)", key.c_str(), value.c_str()));
    //Py_DECREF(func); FIXME Bu garbage collectoru silince hata veriyor 
    PyGILState_Release(_state);// GIL'i salıyoruz
    //Py_DECREF(main_dict); / // FIXME Bu garbage collectoru silince hata veriyor
    
}


class KeyValueSubscriber
{
private:

    DomainParticipant* participant_;

    Subscriber* subscriber_;

    DataReader* reader_;

    Topic* topic_;

    TypeSupport type_;
    class SubListener : public DataReaderListener
    {
    public:

        SubListener()
            : samples_(0)
        {
        }

        ~SubListener() override
        {
        }

        void on_subscription_matched(
                DataReader*,
                const SubscriptionMatchedStatus& info) override
        {
            if (info.current_count_change == 1)
            {
                std::cout << "Subscriber matched." << std::endl;
            }
            else if (info.current_count_change == -1)
            {
                std::cout << "Subscriber unmatched." << std::endl;
            }
            else
            {
                std::cout << info.current_count_change
                        << " is not a valid value for SubscriptionMatchedStatus current count change" << std::endl;
            }
        }

        void on_data_available(
                DataReader* reader) override
        {
            SampleInfo info;
            if (reader->take_next_sample(&hello_, &info) == ReturnCode_t::RETCODE_OK)
            {
                if (info.valid_data)
                {
                    samples_++;
/*                     std::cout << "Message: " << hello_.key() << " with index: " << hello_.value()
                                << " RECEIVED." << std::endl; */
                    Singleton::getInstance().add(hello_.key(), hello_.value());
                    setvalue(hello_.key(), hello_.value());
                    
                }
            }
        }

        KeyValue hello_;

        std::atomic_int samples_;

    } listener_;

public:

    KeyValueSubscriber()
        : participant_(nullptr)
        , subscriber_(nullptr)
        , topic_(nullptr)
        , reader_(nullptr)
        , type_(new KeyValuePubSubType())
    {
    }

    virtual ~KeyValueSubscriber()
    {
        if (reader_ != nullptr)
        {
            subscriber_->delete_datareader(reader_);
        }
        if (topic_ != nullptr)
        {
            participant_->delete_topic(topic_);
        }
        if (subscriber_ != nullptr)
        {
            participant_->delete_subscriber(subscriber_);
        }
        DomainParticipantFactory::get_instance()->delete_participant(participant_);
    }

    //!Initialize the subscriber
    bool init(const std::string& wan_ip,
        unsigned short port)
    {
    DomainParticipantQos pqos;
    int32_t kind = LOCATOR_KIND_TCPv4;

    Locator initial_peer_locator;
    initial_peer_locator.kind = kind;

    std::shared_ptr<TCPv4TransportDescriptor> descriptor = std::make_shared<TCPv4TransportDescriptor>();

    IPLocator::setIPv4(initial_peer_locator, wan_ip);
    std::cout << wan_ip << ":" << port << std::endl;

    initial_peer_locator.port = port;
    pqos.wire_protocol().builtin.initialPeersList.push_back(initial_peer_locator); // Publisher's meta channel

    pqos.wire_protocol().builtin.discovery_config.leaseDuration = eprosima::fastrtps::c_TimeInfinite;
    pqos.wire_protocol().builtin.discovery_config.leaseDuration_announcementperiod = Duration_t(5, 0);
    pqos.name("Participant_sub");

    pqos.transport().use_builtin_transports = false;

    pqos.transport().user_transports.push_back(descriptor);

    participant_ = DomainParticipantFactory::get_instance()->create_participant(0, pqos);

        if (participant_ == nullptr)
        {
            return false;
        }

        // Register the Type
        type_.register_type(participant_);

        // Create the subscriptions Topic
        topic_ = participant_->create_topic("Topic", "KeyValue", TOPIC_QOS_DEFAULT);

        if (topic_ == nullptr)
        {
            return false;
        }

        // Create the Subscriber
        subscriber_ = participant_->create_subscriber(SUBSCRIBER_QOS_DEFAULT, nullptr);

        if (subscriber_ == nullptr)
        {
            return false;
        }

        // Create the DataReader
        DataReaderQos rqos;
        rqos.history().kind = eprosima::fastdds::dds::KEEP_LAST_HISTORY_QOS;
        rqos.history().depth = 30;
        rqos.resource_limits().max_samples = 50;
        rqos.resource_limits().allocated_samples = 20;
        rqos.reliability().kind = eprosima::fastdds::dds::RELIABLE_RELIABILITY_QOS;
        rqos.durability().kind = eprosima::fastdds::dds::TRANSIENT_LOCAL_DURABILITY_QOS;
        reader_ = subscriber_->create_datareader(topic_, rqos, &listener_);

        if (reader_ == nullptr)
        {
            return false;
        }

        return true;
    }

    //!Run the Subscriber
    void run()
    {
        while(true)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
};


void printSingletonMap() {
    while (!stop)
    {

    Singleton& singleton = Singleton::getInstance();
    std::unordered_map<std::string, std::string> map = singleton.getMap();
    for (const auto& [key, value] : map) {
        std::cout << key << ": " << value << std::endl;
    }
    std::cout << "----------------" << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

void subscriber()
{
    std::cout << "Starting subscriber." << std::endl;

    KeyValueSubscriber* mysub = new KeyValueSubscriber();
    if(mysub->init("127.0.0.1", 5100))
    {
        mysub->run();
    }

    delete mysub;
}


void python_scripts(const char* script_name,int delay,std::string path,PyObject* func_get_x)
{   
    PyGILState_STATE _state;
    _state = PyGILState_Ensure();
    std::unordered_map<std::string, char*> x_map;
    PyObject *moduleName = PyUnicode_FromString(script_name);
    PyObject *module = PyImport_Import(moduleName);
    PyObject *func = PyObject_GetAttrString(module, "main");
    Py_DECREF(moduleName);
    Py_DECREF(module);
    PyGILState_Release(_state);// GIL'i salıyoruz
    std::string key;
    std::string value;
    Singleton& singleton = Singleton::getInstance();
    while (!stop)
    {   
        clock_t baslangic = clock(), bitis;
        auto rest = std::chrono::steady_clock::now() + std::chrono::milliseconds(delay);
        _state = PyGILState_Ensure();
        PyObject_CallObject(func, NULL);
        PyObject* result_dict = PyObject_CallObject(func_get_x, NULL);
        PyGILState_Release(_state);// GIL'i salıyoruz
        PyObject *pKey, *pValue;
        Py_ssize_t pos = 0;
        while (PyDict_Next(result_dict, &pos, &pKey, &pValue)) {
            key = PyUnicode_AsUTF8(pKey);
            value = PyUnicode_AsUTF8(pValue);
            singleton.add(key,value);
        }
        //for (const auto &x : x_map) {std::cout << x.first << ": " << x.second << std::endl;}
        std::this_thread::sleep_until(rest);
        //std::cout << (float)(clock() - baslangic) / CLOCKS_PER_SEC<< std::endl;
    }
    PyGILState_Release(_state);// GIL'i salıyoruz
    Py_DECREF(func);
}


int main()
{
    initialize init;
    signal(SIGINT, inthand);
    //JSON dosyasını giriyoruz ve dosyadaki script isimleriin çekiyoruz
    Json::Value script_list;
    std::ifstream json_scripts_file("config.json", std::ifstream::binary);
    json_scripts_file >> script_list;

    // Threadlere & ile direk adresi vermek programı daha da hızlandırır.
    std::vector<std::thread>threads;
    
    int N = script_list["scripts_list"].size();//JSON içerisindeki kod sayılarını çekiyoruz

    //script isimlerini  bir vectore çekiyoruz
    path = script_list["path"].asString();
    std::vector<std::string> scriptsarray;
    int timeouts[N];
    //std::thread show_thread(printSingletonMap);
    std::thread sub_thread(subscriber);


    for (int i = 0; i < N; i++)
    {
        scriptsarray.push_back(script_list["scripts_list"][i][0].asString());
        timeouts[i] =  script_list["scripts_list"][i][1].asInt(); 
    }


    PyRun_SimpleString("import sys");
    std::string filePath = "sys.path.append(\"./";
    filePath += path;
    filePath += "\")";
    PyRun_SimpleString(filePath.c_str());
    //dosya giriyoruz ve kodu 

    PyObject* main_module = PyImport_ImportModule("libpy");
    PyObject* main_dict = PyModule_GetDict(main_module);   
    std::unordered_map<std::string, char*> x_map;
	PyObject* func_get_x = PyDict_GetItemString(main_dict, "getaddedvalues");


    for(int i=0;i<N;i++)
    {
        threads.push_back(std::thread(&python_scripts,scriptsarray[i].c_str(),timeouts[i],path,func_get_x));
        threads[i].detach();
    }
    //show_thread.detach();
    sub_thread.detach();
    enable_threads enable_threads_scope;

    while (!stop){}
    printf("exiting safely\n");
    system("pause");
    return 0;
}