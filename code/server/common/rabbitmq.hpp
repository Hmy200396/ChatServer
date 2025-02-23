#pragma once
#include <ev.h>
#include <amqpcpp.h>
#include <amqpcpp/libev.h>
#include <openssl/ssl.h>
#include <openssl/opensslv.h>
#include <iostream>
#include <functional>
#include "logger.hpp"

namespace hmy{
class MQClient
{
public:
    using MessageCallback = std::function<void(const char*, size_t)>;
    MQClient(const std::string& user, const std::string& passwd, const std::string& host)
    : _loop(EV_DEFAULT)
    , _handler(_loop)
    , _address("amqp://" + user + ":" + passwd + "@" + host + "/")
    , _connection(&_handler, _address)
    , _channel(&_connection)
    {
        _loop_thread = std::thread([this](){
            ev_run(_loop, 0);
        });
    }

    ~MQClient()
    {
        struct ev_async async_watcher;
        ev_async_init(&async_watcher, watcher_callback);
        ev_async_start(_loop, &async_watcher);
        ev_async_send(_loop, &async_watcher);
        _loop_thread.join();
        // ev_loop_destroy(_loop);
        _loop = nullptr;
    }

    void declareComponents(const std::string &exchange, const std::string &queue, const std::string &routing_key = "routing_key", AMQP::ExchangeType echange_type = AMQP::ExchangeType::direct)
    {
        // 声明交换机
        AMQP::Deferred &exchange_deferred = _channel.declareExchange(exchange, echange_type);
        exchange_deferred.onError([](const char *message){
            LOG_ERROR("声明交换机失败: {}", message);
            exit(0); 
        });
        exchange_deferred.onSuccess([exchange](){ 
            LOG_DEBUG("{} 交换机创建成功", exchange);
        });
        // 声明队列
        AMQP::DeferredQueue &queue_deferred = _channel.declareQueue(queue);
        queue_deferred.onError([](const char *message) {
            LOG_ERROR("声明队列失败: {}", message);
            exit(0); 
        });
        queue_deferred.onSuccess([queue](){ 
            LOG_DEBUG("{} 队列创建成功", queue);
        });
        // 针对交换机和队列进行绑定
        AMQP::Deferred& binding_deferred = _channel.bindQueue(exchange, queue, routing_key);
        binding_deferred.onError([exchange, queue](const char* message){
            LOG_ERROR("{} --- {} 绑定失败: {}", exchange, queue, message);
            exit(0);
        });
        binding_deferred.onSuccess([exchange, queue](){
            LOG_DEBUG("{} --- {} 绑定成功", exchange, queue);
        });
    }

    bool publish(const std::string& exchange, const std::string& msg, const std::string& routing_key = "routing_key", int flags = 0)
    {
        bool ret = _channel.publish(exchange, routing_key, msg, flags);
        if(ret == false)
        {
            LOG_ERROR("{} 发布消息失败: {}", exchange);
            return false;
        }
        return true;
    }

    void consume(const std::string& queue, const std::string& tag, const MessageCallback& cb)
    {
        AMQP::DeferredConsumer& consumer_deferred = _channel.consume(queue, tag);
        consumer_deferred.onError([queue](const char* message){
            LOG_ERROR("订阅 {} 队列消息失败: {}", queue, message);
            exit(0);
        });
        consumer_deferred.onReceived([this, cb](const AMQP::Message& message, uint64_t deliveryTag, bool redelivered){
            cb(message.body(), message.bodySize());
            _channel.ack(deliveryTag);
        });
    }
private:
    static void watcher_callback(struct ev_loop* loop, ev_async* watcher, int32_t revents)
    {
        ev_break(loop, EVBREAK_ALL);
    }

private:
    struct ev_loop* _loop;
    AMQP::LibEvHandler _handler;
    AMQP::Address _address;
    AMQP::TcpConnection _connection;
    AMQP::TcpChannel _channel;
    std::thread _loop_thread;
};
}