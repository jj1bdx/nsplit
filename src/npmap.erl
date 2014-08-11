%% npmap:  do parallel map functions with rpc:async_call/4
%%         with N-process splitting of the job
%% Written by Kenji Rikitake <kenji.rikitake@acm.org>
%% See the end of this file for the licensing details.

%% Version history: 
%% 29-NOV-2008 0.1 alpha release
%% code splitted from nsplit

%% TODO:
%% write manual with edoc

-module(npmap).
-export([npmap/3]).

%% npmap/3: spawn N processes to map(F, L)
%% note: npmap/3 only depends on
%% map_sublist_call/7 and yield_check/2

-spec(npmap/3 :: (non_neg_integer(),function(),[T]) -> [T]).

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

%% splitting ArgList into Rcnt pieces of sublists on-the-fly
%% and feed the sublists into rpc:async_call/4
%% idea of this code imported from
%% Erlang/OTP R12B5 lib/kernel/src/rpc.erl check/3

-spec(map_sublist_call/7 :: 
      ([T],function(),
       non_neg_integer(),non_neg_integer(),integer(),
       [T],[T]) -> [pid()]).
					    
map_sublist_call([],_,_,_,_,_,_) -> [];
map_sublist_call(ArgList,Fun,Rcnt,Slen,Srem,[],Orignodes) ->
    map_sublist_call(ArgList,Fun,Rcnt,Slen,Srem,Orignodes,Orignodes); 
map_sublist_call(ArgList,Fun,Rcnt,Slen,Srem,[Node|MoreNodes],Orignodes) ->
    case Rcnt > Srem of
	true -> SL = Slen;
	false -> SL = Slen + 1
    end,
    {ArgSub, ArgTail} = lists:split(SL, ArgList),
    [rpc:async_call(Node, lists, map, [Fun, ArgSub]) | 
     map_sublist_call(ArgTail,Fun,Rcnt-1,Slen,Srem,MoreNodes,Orignodes)].

%% rpc:yield/1 with checking the result of rpc failure
%% idea of checking imported from
%% Erlang/OTP R12B5 lib/kernel/src/rpc.erl check/3

-spec(yield_check/1 :: (pid()) -> any()).

yield_check(K) ->
    case rpc:yield(K) of
	{badrpc,_} -> exit(badrpc);
	Other -> Other
    end.

%% end of module

%% Version history of nsplit FYI:

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
