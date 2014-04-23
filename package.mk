DEPS:=rabbitmq-server rabbitmq-erlang-client
WITH_BROKER_TEST_COMMANDS:=eunit:test(rabbit_priority_queue_test,[verbose])
