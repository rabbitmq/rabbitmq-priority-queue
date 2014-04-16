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
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-behaviour(rabbit_backing_queue).

-rabbit_boot_step({?MODULE,
                   [{description, "enable priority queue"},
                    {mfa,         {?MODULE, enable, []}},
                    {requires,    pre_boot},
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

-record(state, {bq, bqss}).
-record(null, {bq, bqs}).

-define(res1(F), State#null{bqs = BQ:F}).
-define(res2(F), {Res, BQS1} = BQ:F, {Res, State#null{bqs = BQS1}}).
-define(res3(F), {Res1, Res2, BQS1} = BQ:F, {Res1, Res2, State#null{bqs = BQS1}}).

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

init(Q = #amqqueue{arguments = Args}, Recover, AsyncCallback) ->
    Priorities = case rabbit_misc:table_lookup(Args, <<"x-priorities">>) of
                     {array, Array} -> lists:usort([N || {long, N} <- Array]);
                     _              -> none
                 end,
    BQ = bq(),
    case Priorities of
        none -> #null{bq  = BQ,
                      bqs = BQ:init(Q, Recover, AsyncCallback)};
        _    -> #state{bq  = BQ,
                       bqss = [{P, BQ:init(Q, Recover, AsyncCallback)} ||
                                  P <- Priorities]}
    end.


terminate(Reason, State = #state{bq = BQ}) ->
    fold1(fun (BQSn) -> BQ:terminate(Reason, BQSn) end, State);
terminate(Reason, State = #null{bq = BQ, bqs = BQS}) ->
    ?res1(terminate(Reason, BQS)).

%% delete_and_terminate(Reason, State = #state{bq = BQ}) ->
%%     fold1(fun (BQSn) -> BQ:delete_and_terminate(Reason, BQSn) end, State);
delete_and_terminate(Reason, State = #null{bq = BQ, bqs = BQS}) ->
    ?res1(delete_and_terminate(Reason, BQS)).

purge(State = #null{bq = BQ, bqs = BQS}) ->
    ?res2(purge(BQS)).

purge_acks(State = #null{bq = BQ, bqs = BQS}) ->
    ?res1(purge_acks(BQS)).

publish(Msg, MsgProps, IsDelivered, ChPid, State = #state{bq = BQ}) ->
    pick1(fun (BQSn) ->
                  BQ:publish(Msg, MsgProps, IsDelivered, ChPid, BQSn)
          end, Msg, State);
publish(Msg, MsgProps, IsDelivered, ChPid,
        State = #null{bq = BQ, bqs = BQS}) ->
    ?res1(publish(Msg, MsgProps, IsDelivered, ChPid, BQS)).

publish_delivered(Msg, MsgProps, ChPid, State = #null{bq = BQ, bqs = BQS}) ->
    ?res2(publish_delivered(Msg, MsgProps, ChPid, BQS)).

discard(MsgId, ChPid, State = #null{bq = BQ, bqs = BQS}) ->
    ?res1(discard(MsgId, ChPid, BQS)).

drain_confirmed(State = #state{bq = BQ}) ->
    append2(fun (BQSn) -> BQ:drain_confirmed(BQSn) end, State);
drain_confirmed(State = #null{bq = BQ, bqs = BQS}) ->
    ?res2(drain_confirmed(BQS)).

dropwhile(Pred, State = #null{bq = BQ, bqs = BQS}) ->
    ?res2(dropwhile(Pred, BQS)).

fetchwhile(Pred, Fun, Acc, State = #null{bq = BQ, bqs = BQS}) ->
    ?res3(fetchwhile(Pred, Fun, Acc, BQS)).

fetch(AckRequired, State = #state{bq = BQ}) ->
    find2(fun (BQSn) -> BQ:fetch(AckRequired, BQSn) end, State);
fetch(AckRequired, State = #null{bq = BQ, bqs = BQS}) ->
    ?res2(fetch(AckRequired, BQS)).

drop(AckRequired, State = #null{bq = BQ, bqs = BQS}) ->
    ?res2(drop(AckRequired, BQS)).

ack(AckTags, State = #null{bq = BQ, bqs = BQS}) ->
    ?res2(ack(AckTags, BQS)).

requeue(AckTags, State = #null{bq = BQ, bqs = BQS}) ->
    ?res2(requeue(AckTags, BQS)).

ackfold(MsgFun, Acc, State = #null{bq = BQ, bqs = BQS}, AckTags) ->
    ?res2(ackfold(MsgFun, Acc, BQS, AckTags)).

fold(Fun, Acc, State = #null{bq = BQ, bqs = BQS}) ->
    ?res2(fold(Fun, Acc, BQS)).

len(#state{bq = BQ, bqss = BQSs}) ->
    add0(fun (BQSn) -> BQ:len(BQSn) end, BQSs);
len(#null{bq = BQ, bqs = BQS}) ->
    BQ:len(BQS).

is_empty(#state{bq = BQ, bqss = BQSs}) ->
    any(fun (BQSn) -> BQ:is_empty(BQSn) end, BQSs);
is_empty(#null{bq = BQ, bqs = BQS}) ->
    BQ:is_empty(BQS).

depth(#state{bq = BQ, bqss = BQSs}) ->
    add0(fun (BQSn) -> BQ:depth(BQSn) end, BQSs);
depth(#null{bq = BQ, bqs = BQS}) ->
    BQ:depth(BQS).

set_ram_duration_target(DurationTarget, State = #state{bq = BQ}) ->
    fold1(fun (BQSn) ->
                  BQ:set_ram_duration_target(DurationTarget, BQSn)
          end, State);
set_ram_duration_target(DurationTarget, State = #null{bq = BQ, bqs = BQS}) ->
    ?res1(set_ram_duration_target(DurationTarget, BQS)).

ram_duration(State = #state{bq = BQ}) ->
    add2(fun (BQSn) -> BQ:ram_duration(BQSn) end, State);
ram_duration(State = #null{bq = BQ, bqs = BQS}) ->
    ?res2(ram_duration(BQS)).

needs_timeout(#state{bq = BQ, bqss = BQSs}) ->
    fold0(fun (_BQSn, timed) -> timed;
              (BQSn,  idle)  -> case BQ:needs_timeout(BQSn) of
                                    timed -> timed;
                                    _     -> idle
                                end;
              (BQSn,  false) -> BQ:needs_timeout(BQSn)
          end, false, BQSs);
needs_timeout(#null{bq = BQ, bqs = BQS}) ->
    BQ:needs_timeout(BQS).

timeout(State = #null{bq = BQ, bqs = BQS}) ->
    ?res1(timeout(BQS)).

handle_pre_hibernate(State = #state{bq = BQ}) ->
    fold1(fun (BQSn) ->
                  BQ:handle_pre_hibernate(BQSn)
          end, State);
handle_pre_hibernate(State = #null{bq = BQ, bqs = BQS}) ->
    ?res1(handle_pre_hibernate(BQS)).

resume(State = #null{bq = BQ, bqs = BQS}) ->
    ?res1(resume(BQS)).

msg_rates(#null{bq = BQ, bqs = BQS}) ->
    BQ:msg_rates(BQS).

status(#state{bq = BQ, bqss = BQSs}) ->
    [[{priority, P},
      {status,   BQ:status(BQSn)}] || {P, BQSn} <- BQSs];
status(#null{bq = BQ, bqs = BQS}) ->
    BQ:status(BQS).

invoke(Mod, Fun, State = #null{bq = BQ, bqs = BQS}) ->
    ?res1(invoke(Mod, Fun, BQS)).

is_duplicate(Msg, State = #state{bq = BQ}) ->
    pick2(fun (BQSn) -> BQ:is_duplicate(Msg, BQSn) end, Msg, State);
is_duplicate(Msg, State = #null{bq = BQ, bqs = BQS}) ->
    ?res2(is_duplicate(Msg, BQS)).

%%----------------------------------------------------------------------------

bq() ->
    {ok, RealBQ} = application:get_env(
                     rabbitmq_priority_queue, backing_queue_module),
    RealBQ.

priority(_Msg, [{P, BQSn}]) ->
    {P, BQSn};
priority(Msg = #basic_message{content = #content{properties = Props}}, 
         [{P, BQSn} | Rest]) ->
    #'P_basic'{priority = Priority} = Props,
    case Priority =< P of
        true  -> {P, BQSn};
        false -> priority(Msg, Rest)
    end.

fold0(Fun,  Acc, [{_, BQSn} | Rest]) -> fold0(Fun, Fun(BQSn, Acc), Rest);
fold0(_Fun, Acc, [])                 -> Acc.

any(Fun, BQSs) -> fold0(fun (_BQSn, true)  -> true;
                            (BQSn,  false) -> Fun(BQSn)
                        end, false, BQSs).

add0(Fun, BQSs) -> fold0(fun (BQSn, Acc) -> Acc + Fun(BQSn) end, 0, BQSs).

fold1(Fun, State = #state{bqss = BQSs}) ->
    State#state{bqss = fold1(Fun, BQSs, [])}.

fold1(Fun, [{P, BQSn} | Rest], BQSAcc) ->
    BQSn1 = Fun(BQSn),
    fold1(Fun, Rest, [{P, BQSn1} | BQSAcc]);
fold1(_Fun, [], BQSAcc) ->
    lists:reverse(BQSAcc).


fold2(Fun, AccFun, Acc, State = #state{bqss = BQSs}) ->
    {Res, BQSs1} = fold2(Fun, AccFun, Acc, BQSs, []),
    {Res, State#state{bqss = BQSs1}}.

fold2(Fun, AccFun, Acc, [{P, BQSn} | Rest], BQSAcc) ->
    {Res, BQSn1} = Fun(BQSn),
    Acc1 = AccFun(Res, Acc),
    fold2(Fun, AccFun, Acc1, Rest, [{P, BQSn1} | BQSAcc]);
fold2(_Fun, _AccFun, Acc, [], BQSAcc) ->
    {Acc, lists:reverse(BQSAcc)}.

pick1(Fun, Msg, #state{bqss = BQSs} = State) ->
    {P, BQSn} = priority(Msg, BQSs),
    State#state{bqss = orddict:store(P, Fun(BQSn), BQSs)}.

pick2(Fun, Msg, #state{bqss = BQSs} = State) ->
    {P, BQSn} = priority(Msg, BQSs),
    {Res, BQSn1} = Fun(BQSn),
    {Res, State#state{bqss = orddict:store(P, BQSn1, BQSs)}}.


find2(Fun, State = #state{bqss = BQSs}) ->
    {Res, BQSs1} = find2(Fun, BQSs, []),
    {Res, State#state{bqss = BQSs1}}.

find2(Fun, [{P, BQSn} | Rest], BQSAcc) ->
    case Fun(BQSn) of
        {empty, BQSn1} -> find2(Fun, Rest, [{P, BQSn1} | BQSAcc]);
        {Res, BQSn1}   -> {Res, lists:reverse([{P, BQSn1} | BQSAcc] ++ Rest)}
    end;
find2(_Fun, [], BQSAcc) ->
    {empty, lists:reverse(BQSAcc)}.

append2(Fun, State) ->
    fold2(Fun, fun (Res, Acc) -> Res ++ Acc end, [], State).

add2(Fun, State) ->
    fold2(Fun, fun add_maybe_infinity/2, 0, State).

add_maybe_infinity(infinity, _) -> infinity;
add_maybe_infinity(_, infinity) -> infinity;
add_maybe_infinity(A, B)        -> A + B.
