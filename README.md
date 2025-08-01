# pqdocket
[![build-and-test](https://github.com/modfin/pqdocket/actions/workflows/build-and-test.yaml/badge.svg)](https://github.com/modfin/pqdocket/actions/workflows/build-and-test.yaml)


A simple Postgres-powered work queue based on `lib/pq`.

## Design goals
- Support long-running jobs, don't occupy a connection / transaction while a task is running.
- Be able to schedule tasks at a timestamp in the future.
- Fast task claiming based using `SELECT ... FOR UPDATE SKIP LOCKED`
- Uses postgres `LISTEN/NOTIFY` to:
  - Reduce polling while idle.
  - Immediately begin processing tasks on all worker instances when tasks are scheduled, uses `LISTEN/NOTIFY` for this.
- At-Least-Once execution, see notes below.


## Notes on At-Least-Once execution
When used with a correct setup and with well-behaved tasks, it typically will operate at Exactly-Once. But some problems can cause it to not be able to reach Exactly-Once execution. And then we choose At-Least-Once over At-Most-Once.
For example:
- Postgres or the Go worker instances, crashing during task execution.
- Buggy tasks that occasionally block / deadlock forever. The task will then after the claim time (task execution timeout) have expired, be scheduled again. 
- Tasks with too low claim time.
- Tasks that don't extend their claim time if they run longer than expected.
