This is library to execute task

It will create task in storage (Mongodb, SQL, Redis) then cronjob fetch task then execute

Flow:

```
  1. Once first time start cron: reset 'RUNNING' task status to 'TEMPORARILY_FAILED' status
  2. Check amount of RUNNING task:
    + if > task_limit: end => next cron.
    + else: fetch TEMPORARILY_FAILED task in failed_at order.
      + if < task_limit: fetch IDLE task in priority, created_at order, LIMIT by remain slot
  3. Execute task
```

1. Storage

  + Storage: How library interact with database

  + List support storage

    + MongoDB storage:
      + Interact with MongoDB through 'mongoose'
      ```
        const mongoose = require('mongoose');
        const { TaskStorage } = require('task-service-lib');

        await mongoose.connect('mongodb://localhost:27017/<db>');

        const storage = await TaskStorage.TaskMongoStorage.init({ mongoose, expired: <expired> })
      ```

      + Task model
        + Name: 'task'
        ```
          {
            activity_code: { type: String, require: true },
            status: { type: String, default: _CONST.TASK.STATUS.IDLE },
            input: { type: Object, default: null },
            result: { type: Object, default: null },
            error: { type: Object, default: null },
            priority: { type: Number, default: 0 },
            max_retry_times: { type: Number, default: null },
            retry_times: { type: Number, default: 0 },
            running_at: { type: Date, default: null },
            finished_at: { type: Date, default: null },
            failed_at: { type: Date, default: null },
            created_at: { type: Date, default: Date.now },
            updated_at: { type: Date, default: Date.now },
          }
        ```

    + SQL storage
      + Interact with SQL DBMS through 'knex'
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

        const storage = await TaskStorage.TasSQLStorage.init({ knex: db, expired: <expired> })
      ```

      + Task table:
        + Name: 'tasks'
        ```
          CREATE TABLE "tasks" (
            "_id" UUID PRIMARY KEY,
            "activity_code" VARCHAR NOT NULL,
            "status" VARCHAR DEFAULT 'IDLE',
            "input" VARCHAR,
            "result" VARCHAR,
            "error" VARCHAR,
            "priority" INTEGER,
            "max_retry_times" INTEGER DEFAULT NULL,
            "retry_times" INTEGER DEFAULT 0,
            "running_at" TIMESTAMP,
            "finished_at" TIMESTAMP,
            "failed_at" TIMESTAMP,
            "created_at" TIMESTAMP DEFAULT NOW(),
            "updated_at" TIMESTAMP DEFAULT NOW()
          );
        ```

    + Redis storage:
      + Interact with Redis through 'redis'
      ```
        const { createClient } = require('redis');
        const { TaskStorage } = require('task-service-lib');

        const redis = createClient();

        await redis.connect();

        const storage = await TaskStorage.TaskRedisStorage.init({ redis: redis, expired: <expired> })
      ```

    ```
      expired: <expired>
      // auto remove finished task after <expired> second
      // * in MONGODB, must remove finished_at ttl index to reset
    ```

2. TaskManager
  + Manager storage, activities, runtime setting.

  ```
    const { TaskManagerFactory } = require('task-service-lib');

    const TaskManager = TaskManagerFactory({
      storage: <storage>,
      task_limit: <task_limit>,
      cron_time: <cron_time>,
      verbose: <verbose>
    })

  ```

    + task_limit: <number>: default 5: only 5  (each mode) tasks execute at a time: ex: can be 5 parallel task and 5 sequence task execute at a time

    + cron_time: default '*/5 * * * * *': run cronjob each 5 second

    + verbose: <boolean>: default false; true => log each error when process task

3. Activity

  + Define task setting and execute

  ```
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

4. TaskService
  + Service use to create and manager task:
  ```
    const { TaskServiceFactory } = require('task-service-lib');

    const TaskService = TaskServiceFactory({ task_manager: TaskManager })
  ```


5. Task

  + CreateTask
  ```
    await TaskService.createTask({ activity_code: <activity_code>, input: <task_input> })
  ```

6. Start cron process task

  ```
    TaskManager.start({ task_service: TaskService });
  ```