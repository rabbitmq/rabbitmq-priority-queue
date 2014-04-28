DEPS:=rabbitmq-server rabbitmq-erlang-client
WITH_BROKER_TEST_CONFIG:=$(PACKAGE_DIR)/etc/rabbit-test
WITH_BROKER_TEST_COMMANDS:=rabbit_priority_queue_test_all:all_tests()
