import benchy, bench_old, bench_new

timeIt "old parser":
  for i in 0 .. 1_000_000:
    testOld()

timeIt "new parser":
  for i in 0 .. 1_000_000:
    testNew()