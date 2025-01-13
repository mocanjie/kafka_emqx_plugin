-module(kafka_emqx_plugin_brod).

-include_lib("emqx/include/logger.hrl").

-export([on_publish/1, start_kafka/0]).

%% 启动 Kafka 客户端
start_kafka() ->
    % 打印日志
    io:format("hello world~n"),
    ok = brod:start().
    %% 启动 Kafka 客户端并连接到 Kafka 集群
    % ok = brod:start_client([{"localhost", 9092}]).

%% 发布 MQTT 消息到 Kafka
on_publish(Payload) ->
    Topic = "aaa",
    %% 将消息发送到 Kafka 主题 "mqtt-topic"
    kafka_produce(Topic, Payload).

%% 使用 `brod` 发送消息到 Kafka
kafka_produce(Topic, Payload) ->
    %% 发送消息到 Kafka 主题
    {ok, _} = brod:produce_sync(client1,Topic, 0, <<"key1">>,Payload),
    ok.
