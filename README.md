# What is nsplit and npmap?

nsplit is a n-process parallel/concurrent mapping function library based on Erlang/OTP rpc:async_call/4 function.

The module has an external function:

* npmap(N, F, L): spawn N processes to lists:map(F, L)

The npmap/3 function is also available as an independent module npmap.erl from nsplit-0.4.

# deprecated functions

The module has two deprecated external functions:

* list_nsplit(N, L): splits a list L into N-member two-level lists

  list_nsplit(3, [a,b,c,d,e,f,g]) -> [[a,b,c],[d,e],[f,g]]

* pmap(F, L): spawn parallel processes to lists:map(F, L)

