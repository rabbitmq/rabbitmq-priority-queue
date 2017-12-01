# Nothing to see here

As of RabbitMQ 3.5.0 the priority queue is integrated into the
broker; the plugin is no longer needed.

Users of the plugin upgrading from previous versions of RabbitMQ should:

* Install the new version of RabbitMQ
* Invoke: `rabbitmq-plugins disable rabbitmq_priority_queue`
* Start the server

## RabbitMQ 3.4.x Plugin

For users of RabbitMQ 3.4.x, you can read the old version
of the README at:

https://github.com/rabbitmq/rabbitmq-priority-queue/tree/fbdcb67af2d190d3974a8133096be2f3f576ee62

and download the plugin from:

http://www.rabbitmq.com/community-plugins/

