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
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/DataWriterListener.hpp>
#include <fastdds/rtps/transport/UDPv4TransportDescriptor.h>
#include <fastrtps/utils/IPLocator.h>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/DataReaderListener.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>

using namespace eprosima::fastdds::dds;
using namespace eprosima::fastdds::rtps;
using IPLocator = eprosima::fastrtps::rtps::IPLocator;

// Programı sonlandırmak için ctrl+c
volatile sig_atomic_t stop;
void inthand(int signum) {stop = 1;} 

std::string path; // Python dosyalarının bulunduğu path

/*
 Thread Safe Meyers Singleton
İçerisinde bir tane unordered map tanımlı
add-> map'e eleman ekler
getmap -> map'i geri döndürür 

ref = https://www.modernescpp.com/index.php/thread-safe-initialization-of-a-singleton 
*/

class Singleton {
public:
    static Singleton& getInstance() {
        static Singleton instance; // [ ] bu kısımda lock yok sıkıntı olur mu?
        return instance;
    }

    void add(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(m_mutex);
        global_map[key] = value;
    }

    std::unordered_map<std::string, std::string> getMap() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return global_map;
    }

private:
    Singleton() = default;
    ~Singleton() = default;
    Singleton(const Singleton&) = delete;
    Singleton& operator=(const Singleton&) = delete;

    std::unordered_map<std::string, std::string> global_map;
    mutable std::mutex m_mutex;
};
/*
    Python interpreterini initialize eden yapı
    Sadece bir kere main fonksiyonunda çağırılması yeterli
 */

struct initialize 
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

/*
    "libpy.py" adlı kütüphaneden "setvalue" fonksiyonunu çalıştırıyor.
    Böylece global_map'imiz python tarafında da güncelleniyor.
 */
void setvalue(std::string key, std::string value) {
    //GIL'i alıyoruz
    PyGILState_STATE _state;
    _state = PyGILState_Ensure();

    //PyRun_SimpleString("import os");
    std::string filePath = "sys.path.append(\"./";
    filePath += path;
    filePath += "\")";
    //PyRun_SimpleString(filePath.c_str());


    PyObject* main_module = PyImport_ImportModule("libpy");//[ ] Buraya nasıl ulaşıyor anlamadım

    if (main_module == NULL) {
        std::cout << "Error importing main module" << std::endl;
        return;
    }

    PyObject* main_dict = PyModule_GetDict(main_module); 

    if (main_dict == NULL) {
        std::cout << "Error getting main module dictionary" << std::endl;
        return;
    }

    PyObject* func = PyDict_GetItemString(main_dict, "setvalue");

    if (func == NULL) {
        std::cout << "Error getting function" << std::endl;
        return;
    }

    Py_DECREF(main_module);

    PyObject_CallObject(func, (PyObject*)Py_BuildValue("(ss)", key.c_str(), value.c_str()));
    if (PyErr_Occurred()) {
        PyErr_Print();
        return;
    }
    //Py_DECREF(func); FIXME Bu garbage collectoru silince hata veriyor 
    PyGILState_Release(_state);// GIL'i salıyoruz
    //Py_DECREF(main_dict); / // FIXME Bu garbage collectoru silince hata veriyor
    
}

/*
    FastDDS kullanılarak yazılan publisher sınıfı
 */
class KeyValuePublisher
{
private:

    KeyValue keyvalue_; // Kullanılacak veri tipi , msg/KeyValuePubSubTypes.h içerisinde tanımlı

    DomainParticipant* participant_; 

    Publisher* publisher_; 

    Topic* topic_;

    DataWriter* writer_;

    TypeSupport type_;

/*     
 Publisher  bağlantı durumunu takip eden listener
ref = fastdds examples
https://github.com/eProsima/Fast-DDS/tree/master/examples/cpp/dds
  */
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
                std::cout << "[RTPS]Publisher matched." << std::endl;
            }
            else if (info.current_count_change == -1)
            {
                matched_ = info.total_count;
                std::cout << "[RTPS]Publisher unmatched." << std::endl;
            }
            else
            {
                std::cout << info.current_count_change
                        << " is not a valid value for Publication MatchedStatus current count change." << std::endl;
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
    bool init(const std::string& topic_name)
    {   
     DomainParticipantQos qos;
        auto udp_transport = std::make_shared<UDPv4TransportDescriptor>();
/*         udp_transport->sendBufferSize = 9216;
        udp_transport->receiveBufferSize = 9216;
        udp_transport->non_blocking_send = true; */

        // Link the Transport Layer to the Participant.
        qos.transport().user_transports.push_back(udp_transport);

        // Avoid using the default transport
        qos.transport().use_builtin_transports = false;

        participant_ = DomainParticipantFactory::get_instance()->create_participant(0, qos);
        
        if (participant_ == nullptr)
        {
            return false;
        }

        // Register the Type
        type_.register_type(participant_);

        // Topic oluşturma
        topic_ = participant_->create_topic(topic_name, "KeyValue", TOPIC_QOS_DEFAULT);

        if (topic_ == nullptr)
        {
            return false;
        }

        // Publisher oluşturma
        publisher_ = participant_->create_publisher(PUBLISHER_QOS_DEFAULT, nullptr);

        if (publisher_ == nullptr)
        {
            return false;
        }
        
        // DataWriter oluşturma ve ayarlarını yapılandırma
        DataWriterQos wqos;
        wqos.history().kind = KEEP_LAST_HISTORY_QOS; // Son verileri sakla, yeni verileri son verilerin üzerine yaz
        wqos.history().depth = 1;// Saklanacak veri sayısı
        wqos.resource_limits().max_samples = 1; // aynı anda gönderilebilecek veri sayısı
        wqos.resource_limits().allocated_samples = 20; // aynı anda bellekte tutulabilecek veri sayısı
        wqos.reliable_writer_qos().times.heartbeatPeriod.seconds = 2; // heartbeat süresi saniye cinsinden
        wqos.reliable_writer_qos().times.heartbeatPeriod.nanosec = 200 * 1000 * 1000; // heartbeat süresi nanosaniye cinsinden
        wqos.reliability().kind = RELIABLE_RELIABILITY_QOS; // güvenilir veri gönderimi
        writer_ = publisher_->create_datawriter(topic_, wqos, &listener_);
        // Daha fazla özellik için: https://fast-dds.docs.eprosima.com/en/latest/fastdds/api_reference/dds_pim/publisher/datawriterqos.html
        // Bu data writer veri yapısı alınıdığı github adresi:https://github.com/eProsima/Fast-DDS/tree/master/examples/cpp/dds/HelloWorldExampleTCP

        if (writer_ == nullptr)
        {
            return false;
        }
        return true;
    }

    //!Send a publication
    // Eşleşme durumunu kontrol eder
    bool publish()
    {
        if (listener_.matched_ > 0)
        {
            std::cout << "[RTPS]Publisher matched" << std::endl;
            return true;
        }
    
        return false;
    }

    //!Run the Publisher
    //publisher'ın çalıştığı fonksiyon
    void run(std::string key,std::string value)
    {
        keyvalue_.value(value);
        keyvalue_.key(key);
        std::cout << "[RTPS] SENT-> KEY: " << keyvalue_.key() 
                    <<"    VALUE:   " << keyvalue_.value()<< std::endl;
        writer_->write(&keyvalue_);
        //std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
};

/*
    FastDDS kullanılarak yazılan subscriber sınıfı    
 */
class KeyValueSubscriber
{
private:

    DomainParticipant* participant_;

    Subscriber* subscriber_;

    DataReader* reader_;

    Topic* topic_;

    TypeSupport type_;

    /* 
    Subscriber bağlantı durumunu kontrol eden listener sınıfı
    on_subscription_matched-> ilk bağlantı durumunda çalışır
    on_data_available-> veri geldiğinde çalışır

    ref = fastdds examples
    https://github.com/eProsima/Fast-DDS/tree/master/examples/cpp/dds
     */
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
                std::cout << "[RTPS]Subscriber matched." << std::endl;
            }
            else if (info.current_count_change == -1)
            {
                std::cout << "[RTPS]Subscriber unmatched." << std::endl;
            }
            else
            {
                std::cout << info.current_count_change
                        << " is not a valid value for Subscription MatchedStatus current count change" << std::endl;
            }
        }

        void on_data_available(
                DataReader* reader) override
        {
            SampleInfo info;
            if (reader->take_next_sample(&keyvalue_, &info) == ReturnCode_t::RETCODE_OK)
            {
                if (info.valid_data)
                {
                    samples_++;
                    std::cout << "[RTPS] RECEIVED-> Key: " << keyvalue_.key() 
                                << "   Value:  " << keyvalue_.value() << std::endl;
                    Singleton::getInstance().add(keyvalue_.key(), keyvalue_.value()); // Singleton sınıfına verileri ekler
                    setvalue(keyvalue_.key(), keyvalue_.value());// verileri setvalue fonksiyonuna gönderir
                    
                }
            }
        }

        KeyValue keyvalue_;

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
        /* 
        Subscriber'ın initialize edildiği fonksiyon
        */
        bool init(const std::string& topic_name)
        {

        // UDPv4 transport konfigürasyonu
        DomainParticipantQos pqos;
        std::shared_ptr<UDPv4TransportDescriptor> descriptor = std::make_shared<UDPv4TransportDescriptor>();
        eprosima::fastrtps::rtps::Locator_t locator;
        IPLocator::setIPv4(locator, 239, 255, 0, 1);
        locator.port = 22224;
        std::cout << "[RTPS]Subscriber waiting."<<std::endl;

        pqos.wire_protocol().default_multicast_locator_list.push_back(locator); // Publisher's meta channel

        //
        pqos.wire_protocol().builtin.discovery_config.leaseDuration = eprosima::fastrtps::c_TimeInfinite;// keşif süresi    
        pqos.wire_protocol().builtin.discovery_config.leaseDuration_announcementperiod = Duration_t(5, 0);//keşif duyurusu süresi
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
        topic_ = participant_->create_topic(topic_name, "KeyValue", TOPIC_QOS_DEFAULT);

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

        // DaraReader konfigürasyonu
        DataReaderQos rqos;
        rqos.history().kind = eprosima::fastdds::dds::KEEP_LAST_HISTORY_QOS; // Son verileri sakla, yeni verileri son verilerin üzerine yaz
        rqos.history().depth = 1;// Saklanacak veri sayısı
        rqos.resource_limits().max_samples = 1;// aynı anda gönderilebilecek veri sayısı
        rqos.resource_limits().allocated_samples = 1;// aynı anda gönderilebilecek veri sayısı
        rqos.reliability().kind = eprosima::fastdds::dds::RELIABLE_RELIABILITY_QOS;// güvenilir veri gönderimi
        rqos.durability().kind = eprosima::fastdds::dds::TRANSIENT_LOCAL_DURABILITY_QOS; // verilerin saklanma süresi,
        //veriler sadece publisher çalıştığı süre boyunca kalır
        reader_ = subscriber_->create_datareader(topic_, rqos, &listener_);
        // Daha fazla özellik için: https://fast-dds.docs.eprosima.com/en/latest/fastdds/api_reference/dds_pim/subscriber/datareaderqos.html
        // Bu data reader veri yapısı alınıdığı github adresi:https://github.com/eProsima/Fast-DDS/tree/master/examples/cpp/dds/HelloWorldExampleTCP


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
            //std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
};


// Çoklu işlem için Python'u initialize etmemizi sağlıyor.
class enable_threads
{
public:
    enable_threads()
    {
        _state = PyEval_SaveThread();// GIL kaydeder
    }

    ~enable_threads()
    {
        PyEval_RestoreThread(_state); // Kaydedilen GIL'i geri yükler
    }

private:
    PyThreadState* _state;
};


void subscriber(const std::string& topic_name)
{
    std::cout << "[SYS]Starting subscriber thread" << std::endl;
    std::cout << "[RTPS]Starting subscriber." << std::endl;

    KeyValueSubscriber* mysub = new KeyValueSubscriber();
    if(mysub->init( topic_name))
    {
        mysub->run();
    }

    delete mysub;
}

void publisher(const std::string& topic_name)
{
    std::cout << "[SYS]Starting publisher thread" << std::endl;
    Singleton& singleton = Singleton::getInstance();

    KeyValuePublisher* mypub = new KeyValuePublisher();
    mypub->init(topic_name);
    std::unordered_map<std::string, std::string>map_sended;// Gönderilen verileri tutar

    while (mypub->publish() < 1)
    {   
        std::cout << "[RTPS]Waiting for subscriber." << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    }
    std::cout << "[RTPS]Subscriber matched." << std::endl;
    while (!stop)
    {
        std::unordered_map<std::string, std::string> map = singleton.getMap();
        for (const auto& [key, value] : map)
        {
            if (map_sended.count(key) == 0 || map_sended[key] != value)// Daha önceden gönderilmemiş ya da update edilmiş verileri filtreler
            {
                mypub->run(key, value);
                map_sended[key] = value;
            }
        }
        //std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        //std::cout << "----------------" << std::endl;
    }
    delete mypub;
    std::cout << "[RTPS]Publisher stopped." << std::endl;
    std::cout << "[SYS]Thread Publisher Stopped." << std::endl;

}

void python_scripts(const char* script_name,
                int delay,std::string path,PyObject* func_get_x)
{   
    std::cout << "[SYS]Starting Python script:" << script_name <<std::endl;
    PyGILState_STATE _state;
    _state = PyGILState_Ensure();
    
    //dosyadaki "main" fonksiyonunu tutuyoruz
    PyObject *moduleName = PyUnicode_FromString(script_name);
    if (moduleName == NULL)
    {
        std::cout << "[SYS]Error: Python script"<< script_name <<"is not valid." << std::endl;
        return;
    }

    PyObject *module = PyImport_Import(moduleName);

    if (module == NULL)
    {
        std::cout << "[SYS]Error: Python script is not valid." << std::endl;
        return;
    }

    PyObject *func = PyObject_GetAttrString(module, "main");

    if (func == NULL)
    {
        std::cout << "[SYS]Error: Python script is not valid." << std::endl;
        return;
    }

    //gereksiz değişkenleri sil
    Py_DECREF(moduleName);
    Py_DECREF(module);

    PyGILState_Release(_state);// GIL'i salıyoruz

    std::string key; 
    std::string value;
    Singleton& singleton = Singleton::getInstance();

    while (!stop)
    {   
        //clock_t baslangic = clock(), bitis; // işlem başlangıç ve bitiş zamanları gösterilmek istenise açılır
        auto rest = std::chrono::steady_clock::now() + std::chrono::milliseconds(delay);// işlemin süresi hesaplanır

        _state = PyGILState_Ensure();// GIL'i alıyoruz

        PyObject_CallObject(func, NULL);// main fonksiyonunu çağırıyoruz
        PyObject* result_dict = PyObject_CallObject(func_get_x, NULL);//lib.py'deki get_x fonksiyonunu çağırıyoruz ve eklenen değerleri çekiyoruz

        if (result_dict == NULL)
        {
            std::cout << "[SYS]Error: Python script is not valid." << std::endl;
            return;
        }

        PyGILState_Release(_state);// GIL'i salıyoruz

        PyObject *pKey, *pValue; // key ve value değerlerini tutmak için
        Py_ssize_t pos = 0;

        while (PyDict_Next(result_dict, &pos, &pKey, &pValue))// get_x fonksiyonundan gelen değerleri alıyoruz
        {
            key = PyUnicode_AsUTF8(pKey);
            value = PyUnicode_AsUTF8(pValue);
            singleton.add(key,value);
        }
        std::this_thread::sleep_until(rest);
        //std::cout << (float)(clock() - baslangic) / CLOCKS_PER_SEC<< std::endl;
    }
    std::cout << "[SYS]Thread " << script_name << " Stopped." << std::endl;
    Py_DECREF(func);

}


int main()
{
    std::string Publisher_topic_name = "Machine1";
    std::string Subscriber_topic_name = "Machine2";

    initialize init;
    signal(SIGINT, inthand);

    //JSON dosyasını giriyoruz ve dosyadaki script isimleriin çekiyoruz
    Json::Value script_list;
    std::ifstream json_scripts_file("config.json", std::ifstream::binary);
    json_scripts_file >> script_list;

    std::vector<std::thread>threads;
    
    int N = script_list["scripts_list"].size();//JSON içerisindeki kod sayılarını çekiyoruz

    //script isimlerini  bir vectore çekiyoruz
    std::string path = script_list["path"].asString();
    std::vector<std::string> scriptsarray;
    int timeouts[N];
    //json dosyasındaki verilerin isimleri ve timeout değerlerini çekiyoruz
    for (int i = 0; i < N; i++)
    {
        scriptsarray.push_back(script_list["scripts_list"][i][0].asString());
        timeouts[i] =  script_list["scripts_list"][i][1].asInt(); 
    }

    //python içerisinde path ayarlıyoruz
    PyRun_SimpleString("import sys");
    std::string filePath = "sys.path.append(\"./";
    filePath += path;
    filePath += "\")";
    PyRun_SimpleString(filePath.c_str());

    PyObject* main_module = PyImport_ImportModule("libpy");
    PyObject* main_dict = PyModule_GetDict(main_module); 
	PyObject* func_get_x = PyDict_GetItemString(main_dict, "getaddedvalues");

    //publisher ve subscriber threadlerini ayarlıyoruz
    std::thread pub_thread(&publisher,Publisher_topic_name);
    std::thread sub_thread(&subscriber,Subscriber_topic_name);

    for(int i=0;i<N;i++)
    {
        threads.push_back(std::thread(&python_scripts,scriptsarray[i].c_str(),timeouts[i],path,func_get_x));
        threads[i].detach();
    }
    pub_thread.detach();
    sub_thread.detach();
    enable_threads enable_threads_scope;

    while (!stop){}
    printf("exiting safely\n");
    system("pause");
    return 0;
}