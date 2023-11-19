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
#include <iostream>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/DataWriterListener.hpp>
#include <fastdds/rtps/transport/TCPv4TransportDescriptor.h>
#include <fastrtps/utils/IPLocator.h>
#include <string>

using namespace eprosima::fastdds::dds;
using namespace eprosima::fastdds::rtps;

volatile sig_atomic_t stop;

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


class KeyValuePublisher
{
private:

    KeyValue hello_;

    DomainParticipant* participant_;

    Publisher* publisher_;

    Topic* topic_;

    DataWriter* writer_;

    TypeSupport type_;

    class PubListener : public DataWriterListener
    {
    public:
        PubListener()
            : matched_(0)
        {
        }

        ~PubListener() override
        {
        }

        void on_publication_matched(
                DataWriter*,
                const PublicationMatchedStatus& info) override
        {
            if (info.current_count_change == 1)
            {
                matched_ = info.total_count;
                std::cout << "Publisher matched." << std::endl;
            }
            else if (info.current_count_change == -1)
            {
                matched_ = info.total_count;
                std::cout << "Publisher unmatched." << std::endl;
            }
            else
            {
                std::cout << info.current_count_change
                        << " is not a valid value for PublicationMatchedStatus current count change." << std::endl;
            }
        }

        std::atomic_int matched_;

    } listener_;

public:

    KeyValuePublisher()
        : participant_(nullptr)
        , publisher_(nullptr)
        , topic_(nullptr)
        , writer_(nullptr)
        , type_(new KeyValuePubSubType())
    {
    }

    virtual ~KeyValuePublisher()
    {
        if (writer_ != nullptr)
        {
            publisher_->delete_datawriter(writer_);
        }
        if (publisher_ != nullptr)
        {
            participant_->delete_publisher(publisher_);
        }
        if (topic_ != nullptr)
        {
            participant_->delete_topic(topic_);
        }
        DomainParticipantFactory::get_instance()->delete_participant(participant_);
    }

    //!Initialize the publisher
    bool init(const std::string& wan_ip,
        unsigned short port)
    {

        DomainParticipantQos pqos;
        pqos.wire_protocol().builtin.discovery_config.leaseDuration = eprosima::fastrtps::c_TimeInfinite;
        pqos.wire_protocol().builtin.discovery_config.leaseDuration_announcementperiod =
                eprosima::fastrtps::Duration_t(5, 0);
        pqos.name("Participant_pub");

        pqos.transport().use_builtin_transports = false;

        std::shared_ptr<TCPv4TransportDescriptor> descriptor = std::make_shared<TCPv4TransportDescriptor>();


        descriptor->sendBufferSize = 0;
        descriptor->receiveBufferSize = 0;
        descriptor->set_WAN_address(wan_ip);
        std::cout << wan_ip << ":" << port << std::endl;
        descriptor->add_listener_port(port);
        pqos.transport().user_transports.push_back(descriptor);

        participant_ = DomainParticipantFactory::get_instance()->create_participant(0, pqos);
        
        if (participant_ == nullptr)
        {
            return false;
        }

        // Register the Type
        type_.register_type(participant_);

        // Create the publications Topic
        topic_ = participant_->create_topic("Machine2", "KeyValue", TOPIC_QOS_DEFAULT);

        if (topic_ == nullptr)
        {
            return false;
        }

        // Create the Publisher
        publisher_ = participant_->create_publisher(PUBLISHER_QOS_DEFAULT, nullptr);

        if (publisher_ == nullptr)
        {
            return false;
        }

        // Create the DataWriter
        DataWriterQos wqos;
        wqos.history().kind = KEEP_LAST_HISTORY_QOS;
        wqos.history().depth = 30;
        wqos.resource_limits().max_samples = 50;
        wqos.resource_limits().allocated_samples = 20;
        wqos.reliable_writer_qos().times.heartbeatPeriod.seconds = 2;
        wqos.reliable_writer_qos().times.heartbeatPeriod.nanosec = 200 * 1000 * 1000;
        wqos.reliability().kind = RELIABLE_RELIABILITY_QOS;
        writer_ = publisher_->create_datawriter(topic_, wqos, &listener_);

        if (writer_ == nullptr)
        {
            return false;
        }
        return true;
    }

    //!Send a publication
    bool publish()
    {
        if (listener_.matched_ > 0)
        {
            std::cout << "Matched" << std::endl;
            return true;
        }
    
        return false;
    }

    //!Run the Publisher
    void run(std::string key,std::string value)
    {
        std::cout << "Sending message..." << std::endl;
        hello_.value(value);
        hello_.key(key);
        std::cout << "Message: " << hello_.value() << " with key: " << hello_.key()
                    << " SENT" << std::endl;
        writer_->write(&hello_);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
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

void publishSingletonMMap()
{
    Singleton& singleton = Singleton::getInstance();
    KeyValuePublisher* mypub = new KeyValuePublisher();
    mypub->init("127.0.0.1", 5100);
    std::unordered_map<std::string, std::string>map_sended;
    while (mypub->publish() < 1)
    {   
        std::cout << "Waiting for subscriber." << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    std::cout << "Subscriber matched." << std::endl;
    while (!stop)
    {
        std::unordered_map<std::string, std::string> map = singleton.getMap();
        for (const auto& [key, value] : map) {
            if (map_sended.count(key) == 0) {
                mypub->run(key, value);
                map_sended[key] = value;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        std::cout << "----------------" << std::endl;
    }
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

        //std::cout << key << " " << value << std::endl;
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
    std::string path = script_list["path"].asString();
    std::vector<std::string> scriptsarray;
    int timeouts[N];



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
    std::thread show_thread(publishSingletonMMap);

    for(int i=0;i<N;i++)
    {
        threads.push_back(std::thread(&python_scripts,scriptsarray[i].c_str(),timeouts[i],path,func_get_x));
        threads[i].detach();
    }
    show_thread.detach();
    enable_threads enable_threads_scope;

    while (!stop){}
    printf("exiting safely\n");
    system("pause");
    return 0;
}