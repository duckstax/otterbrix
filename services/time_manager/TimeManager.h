#ifndef ROCKETJOE_SERVICES_TIME_MANAGER_TIMEMANAGER_H_
#define ROCKETJOE_SERVICES_TIME_MANAGER_TIMEMANAGER_H_

#include <boost/asio.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include <iostream>
#include <vector>

namespace rocketjoe
{
namespace services
{


// Скрывает реализацию времени отправления, избавляясь от нужды использовать только posix_time
// То есть идея в том, что нужно создать лишь  подходящий API для этого класса, а внутрення
// реализая остается скрытой
// TODO нужно ли?
class SendingTime {
 public:
  SendingTime() : time_(0) {}
  SendingTime(int64_t seconds) : time_(seconds) {}

  // TODO Другие конструкторы с более сложной логикой? Часы? Дни?
  // ...

  boost::posix_time::seconds GetPosixTimeS() const {
    return time_;
  }

 private:
  boost::posix_time::seconds time_;
};

// Класс описывающий тип сообщения, который принимается time_manager'ом
// Сделать его friend для TimeManager?
template<typename AnotherMessageType>
class Message {
  friend class TimeManager;
 public:

  Message() : delay_time_(0) {}
  Message(const AnotherMessageType& message, const SendingTime& time, std::ostream& stream) : delay_time_(time.GetPosixTimeS()), output_(stream) {
    raw_message_ = message;
  }

 private:
  // Сообщение для отправки
  AnotherMessageType raw_message_;
  // Время ожидания для отправки
  boost::posix_time::seconds delay_time_;
  // TODO как хранить то, куда нам нужно отправлять?
  // Для теста храню io stream
  std::ostream& output_;
};

class TimeManager {
  struct Event {
    boost::posix_time::seconds time;
    std::function<void(const boost::system::error_code&)> handler;
  };

 public:

  // Возможно стоит перенести запуск io сразу в конструктор, но я не знаю как будет работать
  // async_await в таком случае
  // TODO прочитать про асинхронную разработку
  TimeManager(boost::asio::io_service& io) : io_(io) {}

  template <typename AnotherMessageType>
  void AddTask(const Message<AnotherMessageType>& msg) {

    /*
    // Возможносто стоит делать io_service_.stop() перед добавлением сообщения?
    if (is_started) {
      //io_service_.stop();
    }
    else {
      is_started = true;
    }
    */

    std::function<void(const boost::system::error_code&)> handler =
        [msg](const boost::system::error_code& error_code)
        {
          msg.output_ << msg.raw_message_;
        };
    Event event_to_add = {msg.delay_time_, handler};
  }

  void Run() {
    for (const auto& event : events_) {
      boost::asio::deadline_timer timer(io_, event.time);
      timer.async_wait(event.handler);
    }
    io_.run();
  }

 private:
  std::vector<Event> events_;
  boost::asio::io_service& io_;
  bool is_started = false;
};

} // services namespace
} // rocketjoe namespace

#endif //ROCKETJOE_SERVICES_TIME_MANAGER_TIMEMANAGER_H_
