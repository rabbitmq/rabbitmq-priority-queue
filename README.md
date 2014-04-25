# Overview

This plugin adds support for priority queues to RabbitMQ.

# Downloading

For the time being there aren't any binaries, it's not yet trustworthy enough.

# Building

You can build and install it like any other plugin (see
[the plugin development guide](http://www.rabbitmq.com/plugin-development.html)).

# Using

Once the plugin is enabled, you can declare priority queues using the
`x-priorities` argument. This argument should be an array of integers
giving the priorities the queue should support. For example, using the Java
client:

    Channel ch = ...;
    Map<String, Object> args = new HashMap<String, Object>();
    args.put("x-priorities", new Integer[]{0, 5, 10});
    ch.queueDeclare("my-priority-queue", true, false, false, args);

You can then control the priority of messages using the `priority`
field of basic.properties.

There is a simple example using the Java client in the `examples` directory.

## Caution

While this plugin implements priority queues in terms of standard
queues, it does not support converting between a priority queue and a
standard queue, and the on-disc format is somewhat different. This has
the following implications:

* Priority queues can only be defined by arguments, not policies. Queues can
  never change the number of priorities they support.
* *It is dangerous to disable the plugin* when durable priority queues exist;
  the broker will fail to start again. Remember that on broker upgrade
  non-bundled plugins like this one need to be reinstalled.
* It is similarly dangerous to enable the plugin if you have previously
  declared durable queues with an `x-priorities` argument when it was not
  enabled. I have no idea why you'd do that, but it will also lead to broker
  crashes on startup.

# Behaviour

The AMQP spec is a bit vague about how priorities work. It says that
all queues MUST support at least 2 priorities, and MAY support up to
10. Larger numbers indicate higher priorities, but it does not define
how messages without a priority property are treated.

As we know, in RabbitMQ queues by default do not support
priorities. When creating priority queues using this plugin, you can
specify as many priority levels as you like. Note that:

* There is some in-memory and on-disc cost per priority level per
  queue, so you may not wish to create huge numbers of levels.
* The message `priority` field is defined as an unsigned byte, so in
  practice priorities should be between 0 and 255.

Messages without a priority set are treated as if their priority were
0. Messages with a priority which does not match any of the queue's
priority levels have their priority rounded down to the nearest
matching level, unless their priority is lower than any supported by
the queue, in which case they have their priority rounded up to the
lowest level.

## Interaction with other features

In general priority queues have all the features of standard RabbitMQ
queues: they support persistence, paging, mirroring, and so on. There
are a couple of interactions that should be noted though:

* Messages which should expire (as at
  http://www.rabbitmq.com/ttl.html) will still only expire from the
  head of the queue. This means that unlike with normal queues, even
  per-queue TTL can lead to expired lower-priority messages getting
  stuck behind non-expired higher priority ones. These messages will
  never be delivered, but they will appear in queue statistics.

* Queues which have a max-length set (as at
  http://www.rabbitmq.com/maxlength.html) will, as usual, drop
  messages from the head of the queue to enforce the limit. This means
  that higher priority messages might be dropped to make way for lower
  priority ones, which might not be what you would expect.
