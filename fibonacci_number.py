from functools import lru_cache
@lru_cache(None) # used for memoization in dynamic programming
def fib(n):
    # return nth fibonacci number
    if n == 0:
        return 0
    if n == 1:
        return 1
    return fib(n-1) + fib(n-2)
for i in range(100):
    print(fib(i))