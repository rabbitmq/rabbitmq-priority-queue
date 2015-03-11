# Nothing to see here

As of RabbitMQ 3.5.0 the priority queue is integrated into the
broker; the plugin is no longer needed.

Users of the plugin upgrading from previous versions of RabbitMQ should:

* Install the new version of RabbitMQ
* Invoke: `rabbitmq-plugins disable rabbitmq_priority_queue`
* Start the server

For users of RabbitMQ 3.4.x or earlier, you can read the old version
of the README at:

https://github.com/rabbitmq/rabbitmq-priority-queue/tree/3431dc1ef8ea53e9a556c6be8bc1b417ac03b58d

and download the plugin from:

http://www.rabbitmq.com/community-plugins/

