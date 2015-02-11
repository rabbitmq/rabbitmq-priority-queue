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

-export([start/1, stop/0]).


start(_DurableQueues) ->
    {ok, []}.

stop() ->
    ok.

init(Queue, Recover, AsyncCallback) ->
    State.


terminate(_Reason, State) ->
    .

delete_and_terminate(_Reason, State) ->
    .

purge(State) ->
    .

purge_acks(State) ->
    .

publish(Msg, MsgProps, IsDelivered, _ChPid, State) ->
    .

publish_delivered(Msg, MsgProps, _ChPid, State) ->
    {[], State}.

discard(_MsgId, _ChPid, State) ->
    State.

drain_confirmed(State) ->
    {[], State}.

dropwhile(Pred, State) ->
    {undefined, State}.

fetchwhile(Pred, Fun, Acc, State) ->
    .

fetch(AckRequired, State) ->
    .

drop(AckRequired, State) ->
    .

ack(AckTags, State) ->
    .

requeue(AckTags, State) ->
    .

ackfold(MsgFun, Acc, State, AckTags) ->
    .

fold(Fun, Acc, State) ->
    .

len(State) ->
    .

is_empty(State) ->
    .

depth(State) ->
    .

set_ram_duration_target(Duration, State) ->
    .

ram_duration(State) ->
    .

needs_timeout(State) ->
    .

timeout(State) ->
    .

handle_pre_hibernate(State) ->
    State.

msg_rates(State) ->
    {0.0, 0.0}.

status(State) ->
    {ok, {}}.

invoke(?MODULE, Fun, State) -> Fun(?MODULE, State);
invoke(      _,   _, State) -> State.

is_duplicate(_Msg, State) -> {false, State}.
