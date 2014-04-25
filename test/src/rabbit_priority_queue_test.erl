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
%% * requeue/2              - reject messages without DLX
%% * drop/2                 - maxlen messages without DLX
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

recovery_test() ->
    {Conn, Ch} = open(),
    Q = <<"test">>,
    declare(Ch, Q, [1, 2, 3]),
    publish(Ch, Q, [1, 2, 3, 1, 2, 3, 1, 2, 3]),
    amqp_connection:close(Conn),

    %% TODO these break coverage
    rabbit:stop(),
    rabbit:start(),

    {Conn2, Ch2} = open(),
    get_all(Ch2, Q, do_ack, [3, 3, 3, 2, 2, 2, 1, 1, 1]),
    delete(Ch2, Q),
    amqp_connection:close(Conn2),
    passed.

simple_order_test() ->
    {Conn, Ch} = open(),
    Q = <<"test">>,
    declare(Ch, Q, [1, 2, 3]),
    publish(Ch, Q, [1, 2, 3, 1, 2, 3, 1, 2, 3]),
    get_all(Ch, Q, do_ack, [3, 3, 3, 2, 2, 2, 1, 1, 1]),
    publish(Ch, Q, [2, 3, 1, 2, 3, 1, 2, 3, 1]),
    get_all(Ch, Q, no_ack, [3, 3, 3, 2, 2, 2, 1, 1, 1]),
    publish(Ch, Q, [3, 1, 2, 3, 1, 2, 3, 1, 2]),
    get_all(Ch, Q, do_ack, [3, 3, 3, 2, 2, 2, 1, 1, 1]),
    delete(Ch, Q),
    amqp_connection:close(Conn),
    passed.

straight_through_test() ->
    {Conn, Ch} = open(),
    Q = <<"test">>,
    declare(Ch, Q, [1, 2, 3]),
    [begin
         consume(Ch, Q, Ack),
         [begin
              publish1(Ch, Q, P),
              assert_delivered(Ch, Ack, P)
          end || P <- [1, 2, 3]],
         cancel(Ch)
     end || Ack <- [do_ack, no_ack]],
    get_empty(Ch, Q),
    delete(Ch, Q),
    amqp_connection:close(Conn),
    passed.

dropwhile_fetchwhile_test() ->
    {Conn, Ch} = open(),
    Q = <<"test">>,
    [begin
         declare(Ch, Q, Args ++ arguments([1, 2, 3])),
         publish(Ch, Q, [1, 2, 3, 1, 2, 3, 1, 2, 3]),
         timer:sleep(10),
         get_empty(Ch, Q),
         delete(Ch, Q)
     end ||
        Args <- [[{<<"x-message-ttl">>, long, 1}],
                 [{<<"x-message-ttl">>,          long,    1},
                  {<<"x-dead-letter-exchange">>, longstr, <<"amq.fanout">>}]
                ]],
    amqp_connection:close(Conn),
    passed.

ackfold_test() ->
    {Conn, Ch} = open(),
    Q = <<"test">>,
    Q2 = <<"test2">>,
    declare(Ch, Q,
            [{<<"x-dead-letter-exchange">>, longstr, <<>>},
             {<<"x-dead-letter-routing-key">>, longstr, Q2}
             | arguments([1, 2, 3])]),
    declare(Ch, Q2, []),
    publish(Ch, Q, [1, 2, 3]),
    [_, _, DTag] = get_all(Ch, Q, manual_ack, [3, 2, 1]),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DTag,
                                        multiple     = true,
                                        requeue      = false}),
    timer:sleep(100),
    get_all(Ch, Q2, do_ack, [3, 2, 1]),
    delete(Ch, Q),
    delete(Ch, Q2),
    amqp_connection:close(Conn),
    passed.

requeue_test() ->
    {Conn, Ch} = open(),
    Q = <<"test">>,
    declare(Ch, Q, [1, 2, 3]),
    publish(Ch, Q, [1, 2, 3]),
    [_, _, DTag] = get_all(Ch, Q, manual_ack, [3, 2, 1]),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DTag,
                                        multiple     = true,
                                        requeue      = true}),
    get_all(Ch, Q, do_ack, [3, 2, 1]),
    delete(Ch, Q),
    amqp_connection:close(Conn),
    passed.

drop_test() ->
    {Conn, Ch} = open(),
    Q = <<"test">>,
    declare(Ch, Q, [{<<"x-max-length">>, long, 4} | arguments([1, 2, 3])]),
    publish(Ch, Q, [1, 2, 3, 1, 2, 3, 1, 2, 3]),
    %% We drop from the head, so this is according to the "spec" even
    %% if not likely to be what the user wants.
    get_all(Ch, Q, do_ack, [2, 1, 1, 1]),
    delete(Ch, Q),
    amqp_connection:close(Conn),
    passed.

purge_test() ->
    {Conn, Ch} = open(),
    Q = <<"test">>,
    declare(Ch, Q, [1, 2, 3]),
    publish(Ch, Q, [1, 2, 3]),
    amqp_channel:call(Ch, #'queue.purge'{queue = Q}),
    get_empty(Ch, Q),
    delete(Ch, Q),
    amqp_connection:close(Conn),
    passed.

%%----------------------------------------------------------------------------
open() ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    {Conn, Ch}.

declare(Ch, Q, [P | _] = Ps) when is_integer(P) ->
    declare(Ch, Q, arguments(Ps));

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           arguments = Args}).

delete(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.delete'{queue = Q}).

publish(Ch, Q, Ps) ->
    amqp_channel:call(Ch, #'confirm.select'{}),
    [publish1(Ch, Q, P) || P <- Ps],
    amqp_channel:wait_for_confirms(Ch).

publish1(Ch, Q, P) ->
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = Q},
                      #amqp_msg{props   = #'P_basic'{priority      = P,
                                                     delivery_mode = 2},
                                payload = int2bin(P)}).

consume(Ch, Q, Ack) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue        = Q,
                                                no_ack       = Ack =:= no_ack,
                                                consumer_tag = <<"ctag">>},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = <<"ctag">>} ->
             ok
    end.

cancel(Ch) ->
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = <<"ctag">>}).

assert_delivered(Ch, Ack, P) ->
    PBin = int2bin(P),
    receive
        {#'basic.deliver'{delivery_tag = DTag}, #amqp_msg{payload = PBin}} ->
            maybe_ack(Ch, Ack, DTag)
    end.

get_all(Ch, Q, Ack, Ps) ->
    DTags = [get_ok(Ch, Q, true, P) || P <- Ps],
    get_empty(Ch, Q),
    DTags.

get_empty(Ch, Q) ->
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = Q}).

get_ok(Ch, Q, Ack, P) ->
    PBin = int2bin(P),
    {#'basic.get_ok'{delivery_tag = DTag}, #amqp_msg{payload = PBin}} =
        amqp_channel:call(Ch, #'basic.get'{queue  = Q,
                                           no_ack = Ack =:= no_ack}),
    maybe_ack(Ch, Ack, DTag).

maybe_ack(Ch, do_ack, DTag) ->
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DTag}),
    DTag;
maybe_ack(_Ch, _, DTag) ->
    DTag.

arguments(Priorities) ->
    [{<<"x-priorities">>, array, [{byte, P} || P <- Priorities]}].

int2bin(Int) -> list_to_binary(integer_to_list(Int)).
