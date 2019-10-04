//
// Created by ilya on 29.09.2019.
//

#include "TimeManager.h"
#include <iostream>

int main() {
  boost::asio::io_service io;
  rocketjoe::services::TimeManager tm(io);
  std::string text = "I have time manager!";
  rocketjoe::services::Message message(text, rocketjoe::services::SendingTime(5), std::cout);
  tm.AddTask(message);
  tm.Run();
  return 0;
}