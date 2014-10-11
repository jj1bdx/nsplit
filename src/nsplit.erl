%% nsplit: splitting a list into sublists
%%         and do parallel map functions with rpc:async_call/4
%% Written by Kenji Rikitake <kenji.rikitake@acm.org>
%% See the end of this file for the license details.

%% Version history: 

%% 4-JUN-2008 0.1 alpha release
%% 9-JUN-2008 0.2 alpha release:
%%     pmap and npmap reversed result bug fixed
%%     list_nsplit_reversed/2 added
%% 24-NOV-2008 0.3 pre-release version with major change:
%%     rpc module's parallel_eval, map_nodes, yield are
%%     imported and modified for npmap and pmap
%%     list_nsplit_reversed/2 removed
%%     when rpc fails, it will fail inside yield_check/2
%% 29-NOV-2008 0.4 pre-release version with more major changes:
%%     npmap/3 rewritten without pre-generation of various lists
%%     and the argument sublists are now generated on-the-fly
%%     this optimization reduces memory consumption and
%%     makes the code running faster on distributed environment
%%     dialyzer check code added for npmap-related functions

%% TODO:
%%     write manual with edoc
%%     splitting npmap into another module

-module(nsplit).
-export([npmap/3, pmap/2, list_nsplit/2]).

%% npmap/3:
%% spawn N processes to map(F, L)
%% note: npmap/3 only depends on
%% map_sublist_call/7 and yield_check/2

-spec npmap(non_neg_integer(),function(),[T]) -> [T].

npmap(N, F, L) when is_integer(N), N > 0, is_function(F), is_list(L) ->
    Len = length(L),
    Nodes = [node() | nodes()],
    Keys = map_sublist_call(L,F,N,
			 Len div N,
			 Len rem N,
			 Nodes,Nodes),
    lists:append(lists:map(fun yield_check/1,Keys));
npmap(N, F, L) ->
    erlang:error(badarg,[N,F,L]).

%% map_sublist_call/7:
%% splitting ArgList into Rcnt pieces of sublists on-the-fly
%% and feed the sublists into rpc:async_call/4
%% idea of this code imported from
%% Erlang/OTP R12B5 lib/kernel/src/rpc.erl check/3

-spec map_sublist_call([T],function(),
       non_neg_integer(),non_neg_integer(),integer(),
       [T],[T]) -> [pid()].
					    
map_sublist_call([],_,_,_,_,_,_) -> [];
map_sublist_call(ArgList,Fun,Rcnt,Slen,Srem,[],Orignodes) ->
    map_sublist_call(ArgList,Fun,Rcnt,Slen,Srem,Orignodes,Orignodes); 
map_sublist_call(ArgList,Fun,Rcnt,Slen,Srem,[Node|MoreNodes],Orignodes) ->
    SL = case Rcnt > Srem of
	        true -> Slen;
	        false -> Slen + 1
         end,
    {ArgSub, ArgTail} = lists:split(SL, ArgList),
    [rpc:async_call(Node, lists, map, [Fun, ArgSub]) | 
     map_sublist_call(ArgTail,Fun,Rcnt-1,Slen,Srem,MoreNodes,Orignodes)].

%% rpc:yield/1 with checking the result of rpc failure
%% if failed then exit inside here with exception 'badrpc'
%% This allows faster failure and reduces burden of
%% checking through the results of *all* processes
%% idea of checking imported from
%% Erlang/OTP R12B5 lib/kernel/src/rpc.erl check/3

-spec yield_check(rpc:key()) -> any().

yield_check(K) ->
    case rpc:yield(K) of
	{badrpc,_} -> exit(badrpc);
	Other -> Other
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% NOTE: since Version 0.4,
%% the following lines contain deprecated contents
%% and will not be updated further
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% old_npmap(integer, function, list) -> list
%% spawn N processes to map(function, list)
%% (obsolete definition)
%% npmap(N, F, L) ->
%%     lists:append(
%%       parallel_eval_func(lists, map, [[F, X] || 
%%                          X <- list_nsplit(N, L)])).

%% pmap(function, list) -> list
%% spawn parallel processes to map(function, list)

-spec pmap(function(),[T]) -> [T].

pmap(F, L) when is_function(F), is_list(L) ->
    parallel_eval_func(erlang, apply, [[F, [X]] || X <- L]);
pmap(F, L) ->
    erlang:error(badarg,[F,L]).

%% original source: rpc:parallel_eval/1 in Erlang/OTP R12B5

-spec parallel_eval_func(atom(),atom(),[T]) -> [T].

parallel_eval_func(M, F, ArgL) ->
    Nodes = [node() | nodes()],
    Keys = map_nodes_func(ArgL,Nodes,Nodes,M,F),
    lists:map(fun yield_check/1,Keys).

%% called from parallel_eval_func/3
%% round-robin mapping of rpc:async_call/4
%% original source: Erlang/OTP R12B5 lib/kernel/src/rpc.erl

-spec map_nodes_func([T],[T],[T],atom(),atom()) -> [pid()].

map_nodes_func([],_,_,_,_) -> [];
map_nodes_func(ArgL,[],Original,M,F) ->
    map_nodes_func(ArgL,Original,Original,M,F); 
map_nodes_func([A|Tail],[Node|MoreNodes],Original,M,F) ->
    [rpc:async_call(Node, M, F, A) | 
     map_nodes_func(Tail,MoreNodes,Original,M,F)].

%% split a list into N- and (N+1)-element lists
%% list_nsplit(3, [a,b,c,d,e,f,g]) ->
%%    [[a,b,c],[d,e],[f,g]]

-spec list_nsplit(non_neg_integer(),[T]) -> [T].

list_nsplit(N, L) when is_integer(N), N > 0, is_list(L) ->
    lists:reverse(getnumsandsplit(lengthlist(length(L), N), [], L));
list_nsplit(N, L) ->
    erlang:error(badarg,[N,L]).

%% getnumsandsplit(list_of_integers, list, list) -> list
%% getnumsandsplit([1,2,3], [], [a,b,c,d,e,f]) -> [[d,e,f],[b,c],[a]]

-spec getnumsandsplit([T],[T],[T]) -> [T].

getnumsandsplit([], H, []) -> H;
getnumsandsplit([N|Ntail], H, T) ->
    {NH, NT} = getheadandleft(N, H, T),
    getnumsandsplit(Ntail, NH, NT).

%% getheadandleft(integer, list, list) -> {list, list}
%% getheadandleft(4, [[a,b,c]], [1,2,3,4,5,6]) ->
%%    {[[1,2,3,4],[a,b,c]], [5,6]}

-spec getheadandleft(integer(),[T],[T]) -> {[T],[T]}.

getheadandleft(_, H, []) -> {H, []};
getheadandleft(0, H, T) -> {H, T};
getheadandleft(N, H, T) ->
    {TH, TT} = lists:split(N,T),
    {[TH|H], TT}.

%% lengthlist(integer, integer) -> list
%% lengthlist(22, 4) -> [5, 5, 4, 4, 4]

-spec lengthlist(non_neg_integer(),non_neg_integer()) -> [non_neg_integer()].

lengthlist(Len,N) ->
    E = Len div N,
    D = Len rem N,
    lists:append(lists:duplicate(D, E+1), lists:duplicate((N-D), E)).

%% end of module

%% License details:
%%
%% The code written by Kenji Rikitake 
%% (except for the original code imported and modified from Erlang/OTP,
%%  whose license is based on the Erlang Public License)
%% is subject to the Creative Commons BSD License.
%% http://creativecommons.org/licenses/BSD/
%%
%% This code contains functions imported from Erlang/OTP R12B5 rpc module.
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
