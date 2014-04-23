%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_priority_queue_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%% The BQ API is used in all sorts of places in all sorts of
%% ways. Therefore we have to jump through a few different hoops
%% in order to integration-test it.
%%
%% * start/1, stop/0, init/3, terminate/2, delete_and_terminate/2
%%   - starting and stopping rabbit. durable queues / persistent msgs needed
%%     to test recovery
%%
%% * publish/5, drain_confirmed/1, fetch/2, ack/2, is_duplicate/2, msg_rates/1,
%%   needs_timeout/1, timeout/1, invoke/3, resume/1 [0]
%%   - regular publishing and consuming, with confirms and acks and durability
%%
%% * publish_delivered/4    - publish with acks straight through
%% * discard/3              - publish without acks straight through
%% * dropwhile/2            - expire messages without DLX
%% * fetchwhile/4           - expire messages with DLX
%% * ackfold/4              - reject messages with DLX
%% * requeue/2              - kill consumer w unacked msgs
%% * purge/1                - issue AMQP queue.purge
%% * purge_acks/1           - mirror queue explicit sync with unacked msgs
%% * fold/3                 - mirror queue explicit sync
%% * depth/1                - mirror queue implicit sync detection
%% * len/1, is_empty/1      - info items
%% * handle_pre_hibernate/1 - hibernation
%%
%% * set_ram_duration_target/2, ram_duration/1, status/1
%%   - maybe need unit testing?
%%
%% [0] publish enough to get credit flow from msg store

fail_test() ->
    exit(no_tests).
