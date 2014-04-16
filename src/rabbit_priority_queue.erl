%%  The contents of this file are subject to the Mozilla Public License
%%  Version 1.1 (the "License"); you may not use this file except in
%%  compliance with the License. You may obtain a copy of the License
%%  at http://www.mozilla.org/MPL/
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and
%%  limitations under the License.
%%
%%  The Original Code is RabbitMQ.
%%
%%  The Initial Developer of the Original Code is GoPivotal, Inc.
%%  Copyright (c) 2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_priority_queue).

-include_lib("rabbit_common/include/rabbit.hrl").
-behaviour(rabbit_backing_queue).

-rabbit_boot_step({?MODULE,
                   [{description, "priority queue"},
                    {mfa,         {?MODULE, enable, []}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

-export([enable/0]).

-export([start/1, stop/0]).

-export([init/3, terminate/2, delete_and_terminate/2, purge/1, purge_acks/1,
         publish/5, publish_delivered/4, discard/3, drain_confirmed/1,
         dropwhile/2, fetchwhile/4, fetch/2, drop/2, ack/2, requeue/2,
         ackfold/4, fold/3, len/1, is_empty/1, depth/1,
         set_ram_duration_target/2, ram_duration/1, needs_timeout/1, timeout/1,
         handle_pre_hibernate/1, resume/1, msg_rates/1,
         status/1, invoke/3, is_duplicate/2]).

-record(state, {bq, bqs}).

-define(res1(F), State#state{bqs = BQ:F}).
-define(res2(F), {Res, BQS1} = BQ:F, {Res, State#state{bqs = BQS1}}).
-define(res3(F), {Res1, Res2, BQS1} = BQ:F, {Res1, Res2, State#state{bqs = BQS1}}).

enable() ->
    {ok, RealBQ} = application:get_env(rabbit, backing_queue_module),
    application:set_env(rabbitmq_priority_queue, backing_queue_module, RealBQ),
    application:set_env(rabbit, backing_queue_module, ?MODULE).

%%----------------------------------------------------------------------------

start(DurableQueues) ->
    BQ = bq(),
    BQ:start(DurableQueues).

stop() ->
    BQ = bq(),
    BQ:stop().

%%----------------------------------------------------------------------------

init(Q, Recover, AsyncCallback) ->
    BQ = bq(),
    #state{bq = BQ, bqs = BQ:init(Q, Recover, AsyncCallback)}.

terminate(Reason, State = #state{bq = BQ, bqs = BQS}) ->
    ?res1(terminate(Reason, BQS)).

delete_and_terminate(Reason, State = #state{bq = BQ, bqs = BQS}) ->
    ?res1(delete_and_terminate(Reason, BQS)).

purge(State = #state{bq = BQ, bqs = BQS}) ->
    ?res2(purge(BQS)).

purge_acks(State = #state{bq = BQ, bqs = BQS}) ->
    ?res1(purge_acks(BQS)).

publish(Msg, MsgProps, IsDelivered, ChPid,
        State = #state{bq = BQ, bqs = BQS}) ->
    ?res1(publish(Msg, MsgProps, IsDelivered, ChPid, BQS)).

publish_delivered(Msg, MsgProps, ChPid, State = #state{bq = BQ, bqs = BQS}) ->
    ?res2(publish_delivered(Msg, MsgProps, ChPid, BQS)).

discard(MsgId, ChPid, State = #state{bq = BQ, bqs = BQS}) ->
    ?res1(discard(MsgId, ChPid, BQS)).

drain_confirmed(State = #state{bq = BQ, bqs = BQS}) ->
    ?res2(drain_confirmed(BQS)).

dropwhile(Pred, State = #state{bq = BQ, bqs = BQS}) ->
    ?res2(dropwhile(Pred, BQS)).

fetchwhile(Pred, Fun, Acc, State = #state{bq = BQ, bqs = BQS}) ->
    ?res3(fetchwhile(Pred, Fun, Acc, BQS)).

fetch(AckRequired, State = #state{bq = BQ, bqs = BQS}) ->
    ?res2(fetch(AckRequired, BQS)).

drop(AckRequired, State = #state{bq = BQ, bqs = BQS}) ->
    ?res2(drop(AckRequired, BQS)).

ack(AckTags, State = #state{bq = BQ, bqs = BQS}) ->
    ?res2(ack(AckTags, BQS)).

requeue(AckTags, State = #state{bq = BQ, bqs = BQS}) ->
    ?res2(requeue(AckTags, BQS)).

ackfold(MsgFun, Acc, State = #state{bq = BQ, bqs = BQS}, AckTags) ->
    ?res2(ackfold(MsgFun, Acc, BQS, AckTags)).

fold(Fun, Acc, State = #state{bq = BQ, bqs = BQS}) ->
    ?res2(fold(Fun, Acc, BQS)).

len(#state{bq = BQ, bqs = BQS}) ->
    BQ:len(BQS).

is_empty(#state{bq = BQ, bqs = BQS}) ->
    BQ:is_empty(BQS).

depth(#state{bq = BQ, bqs = BQS}) ->
    BQ:depth(BQS).

set_ram_duration_target(DurationTarget, State = #state{bq = BQ, bqs = BQS}) ->
    ?res1(set_ram_duration_target(DurationTarget, BQS)).

ram_duration(State = #state{bq = BQ, bqs = BQS}) ->
    ?res2(ram_duration(BQS)).

needs_timeout(#state{bq = BQ, bqs = BQS}) ->
    BQ:needs_timeout(BQS).

timeout(State = #state{bq = BQ, bqs = BQS}) ->
    ?res1(timeout(BQS)).

handle_pre_hibernate(State = #state{bq = BQ, bqs = BQS}) ->
    ?res1(handle_pre_hibernate(BQS)).

resume(State = #state{bq = BQ, bqs = BQS}) ->
    ?res1(resume(BQS)).

msg_rates(#state{bq = BQ, bqs = BQS}) ->
    BQ:msg_rates(BQS).

status(#state{bq = BQ, bqs = BQS}) ->
    BQ:status(BQS).

invoke(Mod, Fun, State = #state{bq = BQ, bqs = BQS}) ->
    ?res1(invoke(Mod, Fun, BQS)).

is_duplicate(Msg, State = #state{bq = BQ, bqs = BQS}) ->
    ?res2(is_duplicate(Msg, BQS)).

%%----------------------------------------------------------------------------

bq() ->
    {ok, RealBQ} = application:get_env(
                 rabbitmq_priority_queue, backing_queue_module),
    RealBQ.
