#pragma once

#ifdef CCAPI_ENABLE_SERVICE_FIX
#ifdef CCAPI_ENABLE_EXCHANGE_BINANCE
#include <deque>
#include <boost/asio/ssl/host_name_verification.hpp>
#include "ccapi_cpp/ccapi_hmac.h"
#include "ccapi_cpp/service/ccapi_binance_fix_sbe_generated.h"
#include "ccapi_cpp/service/ccapi_fix_service.h"

namespace ccapi {

class FixServiceBinance : public FixService {
 public:
  FixServiceBinance(std::function<void(Event&, Queue<Event>*)> eventHandler, SessionOptions sessionOptions, SessionConfigs sessionConfigs,
                    ServiceContextPtr serviceContextPtr)
      : FixService(eventHandler, sessionOptions, sessionConfigs, serviceContextPtr) {
    this->exchangeName = CCAPI_EXCHANGE_NAME_BINANCE;
    this->baseUrlFix = this->sessionConfigs.getUrlFixBase().at(this->exchangeName);
    this->baseUrlFixMarketData = sessionConfigs.getUrlFixMarketDataBase().at(this->exchangeName);
    this->setHostFixFromUrlFix(this->hostFix, this->portFix, this->baseUrlFix);
    this->setHostFixFromUrlFix(this->hostFixMarketData, this->portFixMarketData, this->baseUrlFixMarketData);
    this->fixApiKeyName = CCAPI_BINANCE_FIX_API_KEY;
    this->fixApiPrivateKeyPathName = CCAPI_BINANCE_FIX_API_PRIVATE_KEY_PATH;
    this->fixApiPrivateKeyPasswordName = CCAPI_BINANCE_FIX_API_PRIVATE_KEY_PASSWORD;
    this->setupCredential({this->fixApiKeyName, this->fixApiPrivateKeyPathName, this->fixApiPrivateKeyPasswordName});
    this->protocolVersion = CCAPI_FIX_PROTOCOL_VERSION_BINANCE;
    this->targetCompId = "SPOT";
  }

  virtual ~FixServiceBinance() {}
#ifndef CCAPI_EXPOSE_INTERNAL

 protected:
#endif
  std::string createSenderCompId() {
    static std::atomic<uint64_t> nextSenderCompId{0};
    this->senderCompId = std::to_string(++nextSenderCompId);
    return this->senderCompId;
  }

  std::vector<std::pair<int, std::string>> createCommonParam(std::shared_ptr<FixConnection> fixConnectionPtr, const std::string& nowFixTimeStr) override {
    return {
        {hff::tag::SenderCompID, this->senderCompIdByConnection.at(fixConnectionPtr->id)},
        {hff::tag::TargetCompID, this->targetCompId},
        {hff::tag::MsgSeqNum, std::to_string(++this->fixMsgSeqNumByConnectionIdMap[fixConnectionPtr->id])},
        {hff::tag::SendingTime, nowFixTimeStr},
    };
  }

  std::vector<std::pair<int, std::string>> createLogonParam(std::shared_ptr<FixConnection> fixConnectionPtr, const std::string& nowFixTimeStr,
                                                            const std::map<int, std::string> logonOptionMap = {}) override {
    std::vector<std::pair<int, std::string>> param;
    std::string msgType = "A";
    param.push_back({hff::tag::MsgType, msgType});
    param.push_back({hff::tag::EncryptMethod, "0"});
    param.push_back({hff::tag::HeartBtInt, std::to_string(this->sessionOptions.heartbeatFixIntervalMilliseconds / 1000)});
    const auto& credential = fixConnectionPtr->credential;
    const auto& msgSeqNum = std::to_string(1);
    this->senderCompId = this->createSenderCompId();
    this->senderCompIdByConnection[fixConnectionPtr->id] = this->senderCompId;
    std::vector<std::string> prehashFieldList{msgType, this->senderCompId, this->targetCompId, msgSeqNum, nowFixTimeStr};
    const auto& payload = UtilString::join(prehashFieldList, "\x01");

    auto it = credential.find(this->fixApiPrivateKeyPathName);
    if (it == credential.end()) {
      throw std::runtime_error("Missing credential: " + this->fixApiPrivateKeyPathName);
    }
    std::string password;
    if (auto it = credential.find(this->fixApiPrivateKeyPasswordName); it != credential.end()) {
      password = it->second;
    }
    std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> pkey(UtilAlgorithm::loadPrivateKey(UtilAlgorithm::readFile(it->second), password), EVP_PKEY_free);
    if (!pkey || EVP_PKEY_base_id(pkey.get()) != EVP_PKEY_ED25519) throw std::runtime_error("Binance FIX requires an Ed25519 private key");
    std::string signature = UtilAlgorithm::signPayload(pkey.get(), payload);
    param.push_back({hff::tag::RawDataLength, std::to_string(signature.length())});
    param.push_back({hff::tag::RawData, signature});
    param.push_back({hff::tag::ResetSeqNumFlag, "Y"});
    param.push_back({hff::tag::Username, credential.at(this->fixApiKeyName)});
    param.push_back({kMessageHandlingTag, "1"});
    for (const auto& x : logonOptionMap) {
      auto existing = std::find_if(param.begin(), param.end(), [&](const auto& field) { return field.first == x.first; });
      if (existing == param.end()) param.push_back({x.first, x.second});
      else existing->second = x.second;
    }
    return param;
  }

  struct SbeState {
    std::array<char, CCAPI_FIX_READ_BUFFER_SIZE> readBuffer{};
    size_t readSize = 0;
    uint32_t incomingSequence = 0;
    bool authorized = false;
    std::shared_ptr<boost::asio::steady_timer> logonTimer;
    std::deque<std::shared_ptr<std::string>> writes;
  };
  std::map<std::string, std::shared_ptr<SbeState>> sbeStateByConnection;
  std::map<std::string, std::string> senderCompIdByConnection;

  static bool usesSbe(const Subscription& subscription) {
    auto it = subscription.getOptionMap().find(CCAPI_BINANCE_FIX_ENCODING);
    return it != subscription.getOptionMap().end() && it->second == "SBE";
  }
  void connect(std::shared_ptr<FixConnection> connection) override {
    auto it = connection->subscription.getOptionMap().find(CCAPI_BINANCE_FIX_ENCODING);
    if (it != connection->subscription.getOptionMap().end() && it->second != "FIX" && it->second != "SBE") {
      this->onError(Event::Type::FIX_STATUS, Message::Type::FIX_FAILURE, "Unsupported Binance FIX encoding", {connection->subscription.getCorrelationId()});
      return;
    }
    if (usesSbe(connection->subscription)) {
      if (connection->host == "fix-oe.binance.com" || connection->host == "fix-md.binance.com") {
        connection->setUrl("tcp+tls://" + connection->host + ":9002");
      }
      this->sbeStateByConnection[connection->id] = std::make_shared<SbeState>();
      try {
        if (connection->isSecure) {
          this->serviceContextPtr->sslContextPtr->set_default_verify_paths();
          auto stream = std::get<std::shared_ptr<beast::ssl_stream<beast::tcp_stream>>>(connection->streamPtr);
          stream->set_verify_mode(boost::asio::ssl::verify_peer);
          stream->set_verify_callback(boost::asio::ssl::host_name_verification(connection->host));
        }
      } catch (const std::exception& error) {
        this->sbeStateByConnection.erase(connection->id);
        this->onError(Event::Type::FIX_STATUS, Message::Type::FIX_FAILURE, error.what(), {connection->subscription.getCorrelationId()});
        return;
      }
    }
    FixService::connect(connection);
  }

  void onOpen(std::shared_ptr<FixConnection> connection) override {
    if (!usesSbe(connection->subscription)) return FixService::onOpen(connection);
    try {
      FixService::onOpen(connection);
      auto state = this->sbeStateByConnection.at(connection->id);
      state->logonTimer = std::make_shared<boost::asio::steady_timer>(*this->serviceContextPtr->ioContextPtr, std::chrono::seconds(10));
      state->logonTimer->async_wait([that = shared_from_base<FixServiceBinance>(), connection, state](const ErrorCode& ec) {
        auto it = that->sbeStateByConnection.find(connection->id);
        if (!ec && it != that->sbeStateByConnection.end() && it->second == state && !state->authorized) {
          that->onError(Event::Type::AUTHORIZATION_STATUS, Message::Type::AUTHORIZATION_FAILURE,
                        "FIX SBE logon timed out", {connection->subscription.getCorrelationId()});
          that->onClose(connection, boost::asio::error::timed_out);
        }
      });
    } catch (const std::exception& error) {
      this->onError(Event::Type::AUTHORIZATION_STATUS, Message::Type::AUTHORIZATION_FAILURE, error.what(), {connection->subscription.getCorrelationId()});
      this->onClose(connection, boost::asio::error::invalid_argument);
    }
  }

  static void flattenSbe(const rapidjson::Value& value, Element& element) {
    for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it) {
      if (it->name.GetString()[0] == '_') continue;
      int tag = std::stoi(it->name.GetString());
      if (it->value.IsArray()) {
        element.insert(tag, std::to_string(it->value.Size()));
        for (const auto& entry : it->value.GetArray()) flattenSbe(entry, element);
      } else if (it->value.IsString()) element.insert(tag, std::string(it->value.GetString(), it->value.GetStringLength()));
    }
  }

  void processSbeFrame(std::shared_ptr<FixConnection> connection, std::string_view frame, const TimePoint& received) {
    namespace codec = BinanceProtocolSbe;
    rapidjson::Document document;
    auto decoded = codec::Fix::decode(frame, document.GetAllocator());
    auto state = this->sbeStateByConnection.at(connection->id);
    uint32_t sequence = codec::integer<uint32_t>(decoded["34"].GetString());
    if (state->incomingSequence == UINT32_MAX || sequence != state->incomingSequence + 1) throw std::runtime_error("FIX SBE incoming sequence gap");
    state->incomingSequence = sequence;
    unsigned tid = codec::integer<unsigned>(decoded["_templateId"].GetString());
    const auto type = std::string(codec::Fix::messageType(tid));
    auto nowFix = UtilTime::convertTimePointToFIXTime(received);
    if (type == "0") return;
    if (type == "1") {
      this->writeMessage(connection, nowFix, {{{35, "0"}, {112, decoded["112"].GetString()}}});
      return;
    }
    if (!state->authorized && tid != 20009 && type != "3" && type != "5") throw std::runtime_error("FIX SBE data before logon acknowledgement");
    Event event;
    event.setType(Event::Type::FIX);
    Message message;
    message.setType(Message::Type::FIX);
    message.setTimeReceived(received);
    message.setTime(TimePoint(std::chrono::microseconds(codec::timestampUs(decoded["52"].GetString()))));
    message.setCorrelationIdList({connection->subscription.getCorrelationId()});
    Element element(true);
    element.insert(35, type);
    element.insert("BINANCE_SBE_TEMPLATE_ID", std::to_string(tid));
    flattenSbe(decoded, element);
    if (tid == 20009) {
      if (state->authorized || sequence != 1) throw std::runtime_error("Unexpected FIX SBE logon acknowledgement");
      state->authorized = true;
      if (state->logonTimer) state->logonTimer->cancel();
      event.setType(Event::Type::AUTHORIZATION_STATUS);
      message.setType(Message::Type::AUTHORIZATION_SUCCESS);
      this->setPingPongTimer(PingPongMethod::FIX_PROTOCOL_LEVEL, connection,
          [that = shared_from_base<FixServiceBinance>()](std::shared_ptr<FixConnection> c) {
            auto now = UtilTime::now();
            that->writeMessage(c, UtilTime::convertTimePointToFIXTime(now),
                {{{35, "0"}}, {{35, "1"}, {112, std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count())}}});
          });
    } else if (!state->authorized) {
      event.setType(Event::Type::AUTHORIZATION_STATUS);
      message.setType(Message::Type::AUTHORIZATION_FAILURE);
    }
    message.setElementList({element});
    event.setMessageList({message});
    this->eventHandler(event, nullptr);
    if (!state->authorized) throw std::runtime_error("Binance FIX SBE authorization failed");
    if (type == "5") throw std::runtime_error("Binance FIX SBE logout");
  }

  void startReadFix(std::shared_ptr<FixConnection> connection, void* data, size_t size) override {
    if (!usesSbe(connection->subscription)) return FixService::startReadFix(connection, data, size);
    auto state = this->sbeStateByConnection.at(connection->id);
    std::visit([&](auto& stream) {
      stream->async_read_some(boost::asio::buffer(state->readBuffer.data() + state->readSize,
          std::min(state->readBuffer.size() - state->readSize, this->readMessageChunkSize)),
          [that = shared_from_base<FixServiceBinance>(), connection, state, stream](const ErrorCode& ec, size_t n) {
            auto it = that->sbeStateByConnection.find(connection->id);
            if (it != that->sbeStateByConnection.end() && it->second == state) that->onReadFix(connection, ec, n);
          });
    }, connection->streamPtr);
  }

  void onReadFix(std::shared_ptr<FixConnection> connection, const ErrorCode& ec, std::size_t n) override {
    if (!usesSbe(connection->subscription)) return FixService::onReadFix(connection, ec, n);
    const auto received = UtilTime::now();
    if (ec) { this->onClose(connection, ec); return; }
    if (connection->status != FixConnection::Status::OPEN) return;
    auto state = this->sbeStateByConnection.at(connection->id);
    auto& buffer = state->readBuffer;
    auto& size = state->readSize;
    size += n;
    size_t consumed = 0;
    try {
      while (size - consumed >= 6) {
        BinanceProtocolSbe::Reader reader(std::string_view(buffer.data() + consumed, size - consumed));
        auto length = reader.read<uint32_t>();
        if (length < 26 || length > buffer.size() || reader.read<uint16_t>() != 0xeb50) throw std::runtime_error("Invalid FIX SBE frame length or encoding");
        if (length > size - consumed) break;
        processSbeFrame(connection, std::string_view(buffer.data() + consumed, length), received);
        consumed += length;
        auto current = this->sbeStateByConnection.find(connection->id);
        if (current == this->sbeStateByConnection.end() || current->second != state) return;
      }
      size -= consumed;
      if (size) std::memmove(buffer.data(), buffer.data() + consumed, size);
      if (size == buffer.size()) throw std::runtime_error("FIX SBE read buffer exhausted");
    } catch (const std::exception& error) {
      this->onError(Event::Type::FIX_STATUS, Message::Type::FIX_FAILURE, error.what(), {connection->subscription.getCorrelationId()});
      this->onClose(connection, boost::asio::error::invalid_argument);
      return;
    }
    this->onPongByMethod(PingPongMethod::FIX_PROTOCOL_LEVEL, connection, received);
    this->startReadFix(connection, buffer.data() + size, std::min(buffer.size() - size, this->readMessageChunkSize));
  }

  void writeSbeQueue(std::shared_ptr<FixConnection> connection, std::shared_ptr<SbeState> state) {
    auto payload = state->writes.front();
    std::visit([&](auto& stream) {
      boost::asio::async_write(*stream, boost::asio::buffer(*payload),
          [that = shared_from_base<FixServiceBinance>(), connection, state, payload, stream](const ErrorCode& ec, size_t) {
            auto it = that->sbeStateByConnection.find(connection->id);
            if (it == that->sbeStateByConnection.end() || it->second != state) return;
            if (ec) { that->onClose(connection, ec); return; }
            state->writes.pop_front();
            if (connection->status == FixConnection::Status::CLOSING) { that->onClose(connection, {}); return; }
            if (!state->writes.empty()) that->writeSbeQueue(connection, state);
          });
    }, connection->streamPtr);
  }

  void writeMessage(std::shared_ptr<FixConnection> connection, const std::string& nowFixTimeStr,
                    const std::vector<std::vector<std::pair<int, std::string>>>& paramList) override {
    if (!usesSbe(connection->subscription)) return FixService::writeMessage(connection, nowFixTimeStr, paramList);
    if (connection->status != FixConnection::Status::OPEN) return;
    auto state = this->sbeStateByConnection.at(connection->id);
    auto sequence = this->fixMsgSeqNumByConnectionIdMap[connection->id];
    try {
      std::vector<std::shared_ptr<std::string>> encoded;
      auto time = std::chrono::duration_cast<std::chrono::microseconds>(UtilTime::parse(UtilTime::convertFIXTimeToISO(nowFixTimeStr)).time_since_epoch()).count();
      for (auto params : paramList) {
        auto type = BinanceProtocolSbe::Parameters(params).string(35, false);
        if (!state->authorized && type != "A" && type != "0" && type != "1" && type != "5") throw std::runtime_error("FIX SBE session is not authorized");
        if (type == "A") {
          params.emplace_back(49, this->senderCompIdByConnection.at(connection->id));
          params.emplace_back(56, this->targetCompId);
        }
        if (sequence == UINT32_MAX) throw std::runtime_error("FIX SBE outgoing sequence exhausted");
        encoded.push_back(std::make_shared<std::string>(BinanceProtocolSbe::Fix::encode(params, ++sequence, time)));
      }
      size_t queued = 0;
      for (const auto& payload : state->writes) queued += payload->size();
      for (const auto& payload : encoded) queued += payload->size();
      if (queued > CCAPI_FIX_WRITE_BUFFER_SIZE) throw std::runtime_error("FIX SBE write queue exhausted");
      bool idle = state->writes.empty();
      for (auto& payload : encoded) state->writes.push_back(std::move(payload));
      this->fixMsgSeqNumByConnectionIdMap[connection->id] = sequence;
      if (idle && !state->writes.empty()) writeSbeQueue(connection, state);
    } catch (const std::exception& error) {
      this->onError(Event::Type::REQUEST_STATUS, Message::Type::REQUEST_FAILURE, error.what(), {connection->subscription.getCorrelationId()});
    }
  }

  void onClose(std::shared_ptr<FixConnection> connection, ErrorCode ec) override {
    if (connection->status == FixConnection::Status::CLOSED) return;
    if (usesSbe(connection->subscription)) {
      auto state = this->sbeStateByConnection.find(connection->id);
      if (state != this->sbeStateByConnection.end() && state->second->logonTimer) state->second->logonTimer->cancel();
      this->sbeStateByConnection.erase(connection->id);
      for (auto* timers : {&this->pingTimerByMethodByConnectionIdMap, &this->pongTimeOutTimerByMethodByConnectionIdMap}) {
        auto it = timers->find(connection->id);
        if (it != timers->end()) { for (auto& timer : it->second) timer.second->cancel(); timers->erase(it); }
      }
      std::visit([](auto& stream) { ErrorCode ignored; beast::get_lowest_layer(*stream).socket().close(ignored); }, connection->streamPtr);
      if (connection->status == FixConnection::Status::CLOSING) connection->status = FixConnection::Status::OPEN;
    }
    this->senderCompIdByConnection.erase(connection->id);
    FixService::onClose(connection, ec);
  }

#ifndef CCAPI_EXPOSE_INTERNAL
 protected:
#endif
  static constexpr int kMessageHandlingTag = 25035;
};

} /* namespace ccapi */
#endif
#endif
