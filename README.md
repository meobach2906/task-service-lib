This is library to execute task

It will create task in database then cronjob fetch task then execute

Flow:

```
  1. Once first time start cron: reset 'RUNNING' task status to 'TEMPORARILY_FAILED' status
  2. Check amount of RUNNING task:
    + if > task_limit: end => next cron.
    + else: fetch TEMPORARILY_FAILED task in failed_at order.
      + if < task_limit: fetch IDLE task in priority, created_at order, LIMIT by remain slot
  3. Execute task
```

1. Activity

  + Define task setting and execute

  ```
    const { TaskManager } = require('task-service-lib');

    TaskManager.addActivity({
      code: 'TASK_CODE',
      setting: {
        mode: 'PARALLEL' | 'SEQUENCE',
        retryable: <boolean>, // default: true
        priority: <integer>, // default: 0, max: 9
        max_retry_times: <integer>, // default: null
      },
      process: async ({ task }) => {

        // to throw retryable error: TaskManager.throwRetryableError({ ...reason })

        return <result>
      }
    })
  ```
  + setting:
    + mode:
      + PARALLEL: tasks same code may execute in parallel
      + SEQUENCE: tasks same code must execute sequentially
    + retryable:
      + when task may retry: (task.status === 'TEMPORARILY_FAILED')
        + in task process: TaskManager.throwRetryableError({ ...reason })
        + RUNNING task when restart server => retry

      + if false: task cannot retry => task.status === 'FAILED'

    + max_retry_times: if retry_times > max_retry_times => FAILED

    + priority

2. Start Manager
  ```
    TaskManager.start({ storage, task_limit, cron_time, task_expiry_after_finish });

    // task_limit: default 5: only 5 tasks execute at a time

    // cron_time: default '*/5 * * * * *': run cronjob each 5 second

    // task_expiry_after_finish: default: 10 * 24 * 60 * 60 (second): task expire after finish
  ```

  + Storage:
    + MongoDB storage:
    ```
      const mongoose = require('mongoose');
      const { TaskStorage } = require('task-service-lib');

      await mongoose.connect('mongodb://localhost:27017/<db>');

      const storage = await TaskStorage.TaskMongoStorage.init({ mongoose })
    ```

    + SQL storage
    ```
      const knex = require('knex');
      const { TaskStorage } = require('task-service-lib');

      const db = knex({
        client: 'pg',
        connection: {
          host: 'localhost',
          user: <user>,
          password: <pass>,
          database: <db>,
          port: 5433
        }
      })

      const storage = await TaskStorage.TasSQLStorage.init({ knex: db })
    ```

3. Create Task

  ```
    const { TaskService } = require('task-service-lib');

    await TaskService.createTask({ activity_code: <activity_code>, input: <task_input> })
  ```