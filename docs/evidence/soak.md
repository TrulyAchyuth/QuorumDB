# FaultLab soak runs

Each line block is the verbatim output of `./gradlew :simulation:soak -Pruns=N -PmasterSeed=S`.
A run fails if any safety property or linearizability is violated, or if the cluster has not
re-elected a leader and converged after all faults healed. Scenarios are regenerated exactly by
`Soak.generate(masterSeed, index)`.

```
$ ./gradlew :simulation:soak -Pruns=5000 -PmasterSeed=1
soak masterSeed=1 runs=5000 failures=0
  simulated time: 11.1 hours of cluster time in 31.5 s wall clock
  faults injected: 22495, elections: 12769, client operations: 3273432, events audited: 28195022
  headlines: {EXPECTED_UNAVAILABILITY=778, HEALTHY=4222}

$ ./gradlew :simulation:soak -Pruns=20000 -PmasterSeed=2
soak masterSeed=2 runs=20000 failures=0
  simulated time: 44.4 hours of cluster time in 132.6 s wall clock
  faults injected: 90007, elections: 50700, client operations: 13140234, events audited: 118235398
  headlines: {EXPECTED_UNAVAILABILITY=2681, HEALTHY=17319}
```

Machine: Apple M5, 10 cores; openjdk version "21.0.12.1" 2026-08-18 LTS; 2026-11-13 08:58 UTC
