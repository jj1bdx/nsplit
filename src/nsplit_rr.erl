%% nsplit_rr: nsplit with using reversed list of reversed list, such as
%% [[6,5],[4],[3,2,1]] for [1,2,3,4,5,6] instead of [[5,6],[4],[1,2,3]
%% as in the previous version.

%% This is an experimental and *slower* module of nsplit, by removing
%% lists:split/2 at the lower level and tries to reverse the whole
%% results in npmap/3.  The test result showed this version was actually
%% slower than the previous version, presumably due to the increase of
%% number of the elements of the lists being scanned by check/2.

%% The number of elements handled by check/2 in the previous version is the
%% number of processes in npmap/3 (N) because the processing was done as check/2
%% first, then lists:append/1 next.

%% On the other hand, the number of elements handled by check/2 in this
%% version is the number of whole list elements (L) in npmap/3 because the
%% results were first processed by lists:append/1, then verified by check/2.

%% Tested and evaluated by Kenji Rikitake 24-NOV-2008 and *dumped*. Sigh.

%% nsplit: splitting a list into sublists
%%         and do parallel map() with rpc:parallel_eval()
%% Written by Kenji Rikitake <kenji.rikitake@acm.org>
%% See the end of this file for the license details.

%% Version history:
%%     4-JUN-2008 0.1 alpha release
%%     9-JUN-2008 0.2 alpha release:
%%         pmap and npmap reversed result bug fixed
%%         list_nsplit_reversed/2 added

-module(nsplit_rr).
-export([pmap/2, npmap/3, list_nsplit/2]).

%% pmap(function, list) -> list
%% spawn parallel processes to map(function, list)
%% using rpc:parallel_eval and apply/2
%% note: erlang:apply/2 accepts args of (function, [args])

-spec pmap(function(), list()) -> list().

pmap(F, L) ->
    lists:reverse(check(rpc:parallel_eval(
        [{erlang, apply, [F, [X]]} || X <- L]), [])).

%% npmap(integer, function, list) -> list
%% spawn N processes to map(function, list)
%% note: rpc:parallel_eval wants
%% a list of {module, function, [arg1, arg2,...]} tuples
%% note 2: rpc:parallel_eval arguments reversed; check/2 reverses them back

-spec npmap(non_neg_integer(), function(), list()) -> list().

npmap(N, F, L) ->
    check(lists:append(rpc:parallel_eval(
	    [{lists, map, [F, X]} || X <- list_nsplit_rev_rev(N, L)])), []).

%% check(list, list) -> list
%% if one single call fails, let the whole computation fail.
%% this function is included from Erlang/OTP R12B2 lib/kernel/src/rpc.erl
%% (defined originally as an internal (non-exported) function)
%% WARNING: the result list is REVERSEd, so the result must be reversed back
%% by list:reverse().

-spec check(list(), list()) -> list().

check([{badrpc, _}|_], _) -> exit(badrpc);
check([X|T], Ack) -> check(T, [X|Ack]);
check([], Ack) -> Ack.

%% split a list into N- and (N+1)-element lists
%% list_nsplit(3, [a,b,c,d,e,f,g]) ->
%%    [[a,b,c],[d,e],[f,g]]

-spec list_nsplit(non_neg_integer(), list()) -> list().

list_nsplit(N, L) ->
    lists:reverse(lists:map(fun lists:reverse/1,list_nsplit_rev_rev(N, L))).

%% split a list into N- and (N+1)-element lists
%% but the result list is REVERSEd
%% list_nsplit(3, [a,b,c,d,e,f,g]) ->
%%    [[g,f],[e,d],[c,b,a]]

-spec list_nsplit_rev_rev(non_neg_integer(), list()) -> list().

list_nsplit_rev_rev(N, L) ->
    {R, []} = getnumsandrevsplit(lengthlist(length(L), N), [], L),
    R.

%% getnumsandrevsplit(list_of_integers, list of list, list) -> {list, list}
%% getnumsandrevsplit([1,2,3], [], [a,b,c,d,e,f]) ->
%%    {[[f,e,d],[c,b],[a]], []}

-spec getnumsandrevsplit([integer()], [list()], list()) -> {list(), list()}.

getnumsandrevsplit([], H, T) -> {H, T};
getnumsandrevsplit([N|Ntail], H, T) ->
    {NH, NT} = getrevheadandleft(N, H, T),
    getnumsandrevsplit(Ntail, NH, NT).

%% getrevheadandleft(integer, list of list, list) -> {list, list}
%% getrevheadandleft(4, [[c,b,a]], [1,2,3,4,5,6]) ->
%%    {[[4,3,2,1],[c,b,a]], [5,6]}

-spec getrevheadandleft(non_neg_integer(), [list()], list()) -> {list(), list()}.

getrevheadandleft(_, L, []) -> {L, []};
getrevheadandleft(0, L, R) -> {L, R};
getrevheadandleft(N, L, R) ->
    {TH, TT} = revheadandtail(N, [], R),
    {[TH|L], TT}.

%% The following function is newly added as the substitute for
%% lists:split/2.  This function is not slow, and in fact a slight
%% modification of lists:split/2.

%% revheadandtail(integer, list, list) -> {list, list}
%% revheadandtail(4, [c,b,a], [1,2,3,4,5,6]) ->
%%    {[4,3,2,1,c,b,a], [5,6]}

-spec revheadandtail(non_neg_integer(), list(), list()) -> {list(), list()}.

revheadandtail(0, L, R) ->
    {L, R};
revheadandtail(N, L, [H|T]) ->
    revheadandtail(N-1, [H|L], T);
revheadandtail(_, _, []) ->
    badarg.

%% lengthlist(integer, integer) -> list
%% lengthlist(22, 4) -> [5, 5, 4, 4, 4]

-spec lengthlist(integer(), integer()) -> list().

lengthlist(Len,N) ->
    E = Len div N,
    D = Len rem N,
    lists:append(lists:duplicate(D, E+1), lists:duplicate((N-D), E)).

%% end of module

%% License details:
%%
%% The code written by Kenji Rikitake
%% (of all functions and declarations except for check/2)
%% is subject to the Creative Commons BSD License.
%% http://creativecommons.org/licenses/BSD/
%%
%% This code contains function check/2
%% imported from the Erlang/OTP R12B2.
%% The imported code is subject to the Erlang Public License,
%% notified as follows:
%%
%% ``The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved via the world wide web at http://www.erlang.org/.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%%
%% The Initial Developer of the Original Code is Ericsson Utvecklings AB.
%% Portions created by Ericsson are Copyright 1999, Ericsson Utvecklings
%% AB. All Rights Reserved.''
