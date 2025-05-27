from time import monotonic_ns


n = 100_000

x = monotonic_ns()
frozenset(range(n)) & frozenset(range(n * 2))
print(monotonic_ns() - x)


# 1 000 000 000
