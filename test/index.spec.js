const uuid = require('uuid');
const mongoose = require('mongoose');
const knex = require('knex');
const { createClient } = require('redis');

const _is = require("../src/utils/share/_is.utils.share");
const { TaskManager, TaskService, TaskStorage: { TaskMongoStorage, TaskSQLStorage, TaskRedisStorage } } = require('../index');

const sleep = (ms) => new Promise((res) => setTimeout(res, ms))

const storages = [
  {
    code: 'MONGO',
    storage: null,
    init_storage: async function () {
      await mongoose.connect(`mongodb://localhost:27017/tasks`)
  
      this.storage = await TaskMongoStorage.init({ mongoose, expired: 5 });
    },
    clear_record: async function() {
      await this.storage.task_model.deleteMany({});
    },
    all_task: async function() {
      return await this.storage.task_model.find({}).sort({ 'created_at': 'asc' }).lean(true);
    },
    parse_result_error: function(val) {
      return val;
    },
    insert_record: async function (task) {
      await this.storage.task_model.create(task);
    }
  },
  {
    code: 'SQL',
    storage: null,
    init_storage: async function () {
      const connection = knex({
        client: 'pg',
        connection: {
          host: 'localhost',
          port: 5432,
          user: 'meobach2906',
          password: 'random',
          database: 'tasks'
        }
      })

      this.storage = await TaskSQLStorage.init({ knex: connection, expired: 5 });  
    },
    clear_record: async function() {
      await this.storage.knex('tasks').del({});
    },
    all_task: async function() {
      return await this.storage.knex.select('*').from('tasks').orderBy('created_at', 'asc');
    },
    parse_result_error: function(val) {
      return JSON.parse(val);
    },
    insert_record: async function (task) {
      task._id = uuid.v4();
      await this.storage.knex('tasks').insert(task);
    }
  },
  {
    code: 'REDIS',
    storage: null,
    init_storage: async function () {
      const redis = createClient();

      await redis.connect();

      this.storage = await TaskRedisStorage.init({ redis: redis, expired: 5 });  
    },
    clear_record: async function() {
      await this.storage.redis.flushDb();
    },
    all_task: async function() {
      const tasks = [];
      const task_ids = await this.storage.redis.lRange(`tasks`, 0, -1)
      for (const task_id of task_ids) {
        const task = await this.storage.redis.hGetAll(`task:${task_id}`)
        if (_is.filled_object(task)) {
          tasks.push(task);
        }
      }
      return tasks;
    },
    parse_result_error: function(val) {
      return val ? JSON.parse(val) : null;
    },
    insert_record: async function (task) {
      if (task.failed_at) {
        task.failed_at = task.failed_at.toISOString()
      }


      if (task.created_at) {
        task.created_at = task.created_at.toISOString()
      }

      task._id = uuid.v4();

      await this.storage.redis.rPush(`tasks`, task._id);

      await this.storage.redis.hSet(`task:${task._id}`, task)

      if (task.status === 'TEMPORARILY_FAILED') {
        await this.storage.redis.rPush(`temporarily_failed_tasks`, JSON.stringify({ _id: task._id, activity_code: task.activity_code, priority: '1', created_at: task.created_at }));
      }

      if (task.status === 'RUNNING') {
        await this.storage.redis.rPush(`running_tasks`, JSON.stringify({ _id: task._id, activity_code: task.activity_code, priority: '1', created_at: task.created_at }));
      }
    }
  }
]

const tests = [
  {
    task: { activity_code: 'PARALLEL_LOWEST_PRIORITY', input: { i: 0 } },
    expected: { activity_code: 'PARALLEL_LOWEST_PRIORITY', status: 'IDLE', input: { i: 0 } },
    expected_process_1: { activity_code: 'PARALLEL_LOWEST_PRIORITY', status: 'IDLE', result: null, error: null },
    expected_reset_2: { activity_code: 'PARALLEL_LOWEST_PRIORITY', status: 'IDLE', result: null, error: null },
    expected_process_3: { activity_code: 'PARALLEL_LOWEST_PRIORITY', status: 'IDLE', result: null, error: null },
    expected_process_4: { activity_code: 'PARALLEL_LOWEST_PRIORITY', status: 'RUNNING', result: null, error: null },
    expected_process_5: { activity_code: 'PARALLEL_LOWEST_PRIORITY', status: 'FAILED', result: null, error: { code : "ABC", reactions : ["FIX_DATA"]} },
    expected_process_6: { activity_code: 'PARALLEL_LOWEST_PRIORITY', status: 'FAILED', result: null, error: { code : "ABC", reactions : ["FIX_DATA"]} },
    expected_process_7: { activity_code: 'PARALLEL_LOWEST_PRIORITY', status: 'FAILED', result: null, error: { code : "ABC", reactions : ["FIX_DATA"]} },
  },
  {
    task: { activity_code: 'PARALLEL', input: { i: 2 } },
    expected: { activity_code: 'PARALLEL', status: 'IDLE', input: { i: 2 } },
    expected_process_1: { activity_code: 'PARALLEL', status: 'RUNNING', result: null, error: null },
    expected_reset_2: { activity_code: 'PARALLEL', status: 'TEMPORARY_FAILED', result: null, error: null },
    expected_process_3: { activity_code: 'PARALLEL', status: 'TEMPORARILY_FAILED', result: null, error: { code : "ABC", reactions : ["RETRY"]} },
    expected_process_4: { activity_code: 'PARALLEL', status: 'RUNNING', result: null, error: { code : "ABC", reactions : ["RETRY"]} },
    expected_process_5: { activity_code: 'PARALLEL', status: 'TEMPORARILY_FAILED', result: null, error: { code : "ABC", reactions : ["RETRY"]} },
    expected_process_6: { activity_code: 'PARALLEL', status: 'RUNNING', result: null, error: { code : "ABC", reactions : ["RETRY"]} },
    expected_process_7: { activity_code: 'PARALLEL', status: 'FAILED', result: null, error: { code : "ABC", reactions : ["RETRY"]} },
  },
  {
    task: { activity_code: 'PARALLEL_UNRETRYABLE', input: { i: 2 } },
    expected: { activity_code: 'PARALLEL_UNRETRYABLE', status: 'IDLE', input: { i: 2 } },
    expected_process_1: { activity_code: 'PARALLEL_UNRETRYABLE', status: 'RUNNING', result: null, error: null },
    expected_reset_2: { activity_code: 'PARALLEL_UNRETRYABLE', status: 'TEMPORARY_FAILED', result: null, error: null },
    expected_process_3: { activity_code: 'PARALLEL_UNRETRYABLE', status: 'FAILED', result: null, error: { code : "ABC", reactions : ["RETRY"]} },
    expected_process_4: { activity_code: 'PARALLEL_UNRETRYABLE', status: 'FAILED', result: null, error: { code : "ABC", reactions : ["RETRY"]} },
    expected_process_5: { activity_code: 'PARALLEL_UNRETRYABLE', status: 'FAILED', result: null, error: { code : "ABC", reactions : ["RETRY"]} },
    expected_process_6: { activity_code: 'PARALLEL_UNRETRYABLE', status: 'FAILED', result: null, error: { code : "ABC", reactions : ["RETRY"]} },
    expected_process_7: { activity_code: 'PARALLEL_UNRETRYABLE', status: 'FAILED', result: null, error: { code : "ABC", reactions : ["RETRY"]} }
  },
  {
    task: { activity_code: 'PARALLEL_UNRETRYABLE', input: { i: 1 } },
    expected: { activity_code: 'PARALLEL_UNRETRYABLE', status: 'IDLE', input: { i: 1 } },
    expected_process_1: { activity_code: 'PARALLEL_UNRETRYABLE', status: 'IDLE', result: null, error: null },
    expected_reset_2: { activity_code: 'PARALLEL_UNRETRYABLE', status: 'IDLE', result: null, error: null },
    expected_process_3: { activity_code: 'PARALLEL_UNRETRYABLE', status: 'IDLE', result: null, error: null },
    expected_process_4: { activity_code: 'PARALLEL_UNRETRYABLE', status: 'RUNNING', result: null, error: null },
    expected_process_5: { activity_code: 'PARALLEL_UNRETRYABLE', status: 'FINISHED', result: { ok: true, i: 1 }, error: null },
    expected_process_6: { activity_code: 'PARALLEL_UNRETRYABLE', status: 'FINISHED', result: { ok: true, i: 1 }, error: null },
    expected_process_7: { activity_code: 'PARALLEL_UNRETRYABLE', status: 'FINISHED', result: { ok: true, i: 1 }, error: null },
  },
  {
    task: { activity_code: 'PARALLEL', input: { i: 1 } },
    expected: { activity_code: 'PARALLEL', status: 'IDLE', input: { i: 1 } },
    expected_process_1: { activity_code: 'PARALLEL', status: 'RUNNING', result: null, error: null },
    expected_reset_2: { activity_code: 'PARALLEL', status: 'TEMPORARY_FAILED', result: null, error: null },
    expected_process_3: { activity_code: 'PARALLEL', status: 'FINISHED', result: { ok: true, i: 1 }, error: null },
    expected_process_4: { activity_code: 'PARALLEL', status: 'FINISHED', result: { ok: true, i: 1 }, error: null },
    expected_process_5: { activity_code: 'PARALLEL', status: 'FINISHED', result: { ok: true, i: 1 }, error: null },
    expected_process_6: { activity_code: 'PARALLEL', status: 'FINISHED', result: { ok: true, i: 1 }, error: null },
    expected_process_7: { activity_code: 'PARALLEL', status: 'FINISHED', result: { ok: true, i: 1 }, error: null },
  },
  {
    task: { activity_code: 'SEQUENCE', input: { i: 0 } },
    expected: { activity_code: 'SEQUENCE', status: 'IDLE', input: { i: 0 } },
    expected_process_1: { activity_code: 'SEQUENCE', status: 'RUNNING', result: null, error: null },
    expected_reset_2: { activity_code: 'SEQUENCE', status: 'TEMPORARY_FAILED', result: null, error: null },
    expected_process_3: { activity_code: 'SEQUENCE', status: 'TEMPORARILY_FAILED', result: null, error: { code : "ABC", reactions : ["RETRY"]} },
    expected_process_4: { activity_code: 'SEQUENCE', status: 'RUNNING', result: null, error: { code : "ABC", reactions : ["RETRY"]} },
    expected_process_5: { activity_code: 'SEQUENCE', status: 'FAILED', result: null, error: { code : "ABC", reactions : ["RETRY"]} },
    expected_process_6: { activity_code: 'SEQUENCE', status: 'FAILED', result: null, error: { code : "ABC", reactions : ["RETRY"]} },
    expected_process_7: { activity_code: 'SEQUENCE', status: 'FAILED', result: null, error: { code : "ABC", reactions : ["RETRY"]} },
  },
  {
    task: { activity_code: 'SEQUENCE', input: { i: 1 } },
    expected: { activity_code: 'SEQUENCE', status: 'IDLE', input: { i: 1 } },
    expected_process_1: { activity_code: 'SEQUENCE', status: 'IDLE', result: null, error: null },
    expected_reset_2: { activity_code: 'SEQUENCE', status: 'IDLE', result: null, error: null },
    expected_process_3: { activity_code: 'SEQUENCE', status: 'IDLE', result: null, error: null },
    expected_process_4: { activity_code: 'SEQUENCE', status: 'IDLE', result: null, error: null },
    expected_process_5: { activity_code: 'SEQUENCE', status: 'IDLE', result: null, error: null },
    expected_process_6: { activity_code: 'SEQUENCE', status: 'RUNNING', result: null, error: null },
    expected_process_7: { activity_code: 'SEQUENCE', status: 'FINISHED', result: { ok: true, i: 1 }, error: null },
  },
  {
    task: { activity_code: 'SEQUENCE2', input: { i: 1 } },
    expected: { activity_code: 'SEQUENCE2', status: 'IDLE', input: { i: 1 } },
    expected_process_1: { activity_code: 'SEQUENCE2', status: 'RUNNING', result: null, error: null },
    expected_reset_2: { activity_code: 'SEQUENCE2', status: 'RUNNING', result: null, error: null },
    expected_process_3: { activity_code: 'SEQUENCE2', status: 'FINISHED', result: { ok: true, i: 1 }, error: null },
    expected_process_4: { activity_code: 'SEQUENCE2', status: 'FINISHED', result: { ok: true, i: 1 }, error: null },
    expected_process_5: { activity_code: 'SEQUENCE2', status: 'FINISHED', result: { ok: true, i: 1 }, error: null },
    expected_process_6: { activity_code: 'SEQUENCE2', status: 'FINISHED', result: { ok: true, i: 1 }, error: null },
    expected_process_7: { activity_code: 'SEQUENCE2', status: 'FINISHED', result: { ok: true, i: 1 }, error: null },
  },
]

const reset_tests = [
  {
    task: { activity_code: 'SEQUENCE', status: 'TEMPORARILY_FAILED' },
    expected_reset_1: { activity_code: 'SEQUENCE', status: 'TEMPORARILY_FAILED' },
    expected_process_1: { activity_code: 'SEQUENCE', status: 'TEMPORARILY_FAILED' },
  },
  {
    task: { activity_code: 'SEQUENCE', status: 'RUNNING' },
    expected_reset_1: { activity_code: 'SEQUENCE', status: 'TEMPORARILY_FAILED' },
    expected_process_1: { activity_code: 'SEQUENCE', status: 'RUNNING' },
  },
  {
    task: { activity_code: 'PARALLEL', status: 'RUNNING' },
    expected_reset_1: { activity_code: 'PARALLEL', status: 'TEMPORARILY_FAILED' },
    expected_process_1: { activity_code: 'PARALLEL', status: 'RUNNING' },
  },
  {
    task: { activity_code: 'PARALLEL_UNRETRYABLE', status: 'RUNNING' },
    expected_reset_1: { activity_code: 'PARALLEL_UNRETRYABLE', status: 'FAILED' },
    expected_process_1: { activity_code: 'PARALLEL_UNRETRYABLE', status: 'FAILED' },
  },
  {
    task: { activity_code: 'PARALLEL_LOWEST_PRIORITY', status: 'RUNNING' },
    expected_reset_1: { activity_code: 'PARALLEL_LOWEST_PRIORITY', status: 'TEMPORARILY_FAILED' },
    expected_process_1: { activity_code: 'PARALLEL_LOWEST_PRIORITY', status: 'RUNNING' },
  },
  {
    task: { activity_code: 'PARALLEL_LOWEST_PRIORITY', status: 'RUNNING' },
    expected_reset_1: { activity_code: 'PARALLEL_LOWEST_PRIORITY', status: 'TEMPORARILY_FAILED' },
    expected_process_1: { activity_code: 'PARALLEL_LOWEST_PRIORITY', status: 'RUNNING' },
  },
  {
    task: { activity_code: 'PARALLEL', status: 'TEMPORARILY_FAILED' },
    expected_reset_1: { activity_code: 'PARALLEL', status: 'TEMPORARILY_FAILED' },
    expected_process_1: { activity_code: 'PARALLEL', status: 'TEMPORARILY_FAILED' },
  },
  {
    task: { activity_code: 'PARALLEL', status: 'IDLE' },
    expected_reset_1: { activity_code: 'PARALLEL', status: 'IDLE' },
    expected_process_1: { activity_code: 'PARALLEL', status: 'IDLE' },
  },
]

describe('TASK_SERVICE', () => {

  before(async () => {
    TaskManager.addActivity({
      code: 'PARALLEL',
      setting: { mode: TaskManager.TASK_CONST.MODE.PARALLEL, retryable: true,  priority: 9, max_retry_times: 3 },
      process: async ({ task }) => {
        await sleep(3000);
        if (task.input.i % 2 === 0) {
          TaskManager.throwRetryableError({ code: 'ABC' });
        }
        return { ok: true, ...task.input };
      }
    })

    TaskManager.addActivity({
      code: 'PARALLEL_LOWEST_PRIORITY',
      setting: { mode: TaskManager.TASK_CONST.MODE.PARALLEL, retryable: true,  priority: 0, max_retry_times: 3 },
      process: async ({ task }) => {
        await sleep(3000);
        if (task.input.i % 2 === 0) {
          TaskManager.throwError({ code: 'ABC' });
        }
        return { ok: true, ...task.input };
      }
    })

    TaskManager.addActivity({
      code: 'PARALLEL_UNRETRYABLE',
      setting: { mode: TaskManager.TASK_CONST.MODE.PARALLEL, retryable: false,  priority: 3 },
      process: async ({ task }) => {
        await sleep(3000);
        if (task.input.i % 2 === 0) {
          TaskManager.throwRetryableError({ code: 'ABC' });
        }
        return { ok: true, ...task.input };
      }
    })

    TaskManager.addActivity({
      code: 'SEQUENCE',
      setting: {
        mode: TaskManager.TASK_CONST.MODE.SEQUENCE,
        retryable: true,
        max_retry_times: 2,
      },
      process: async ({ task }) => {
        await sleep(3000);
        if (task.input.i % 2 === 0) {
          TaskManager.throwRetryableError({ code: 'ABC' });
        }
        return { ok: true, ...task.input };
      }
    })

    TaskManager.addActivity({
      code: 'SEQUENCE2',
      setting: {
        mode: TaskManager.TASK_CONST.MODE.SEQUENCE,
        retryable: false,
      },
      process: async ({ task }) => {
        await sleep(3000);
        if (task.input.i % 2 === 0) {
          TaskManager.throwRetryableError({ code: 'ABC' });
        }
        return { ok: true, ...task.input };
      }
    })

  })

  for (const storage of storages) {
  
    describe(`TASK_${storage.code}`, () => {
      let tasks = [];
  
      before(async () => {

        await storage.init_storage()

        await storage.clear_record();
  
        TaskManager.start({
          storage: storage.storage,
          task_limit: 3,
          verbose: false,
          is_test: true
        });
      })

      after(async () => {
        await storage.clear_record();
      })
  
      it('CREATE_TASK', async () => {
        for (const test of tests) {
          await TaskService.createTask(test.task);
        }
  
        tasks = await storage.all_task();
  
        expect(tasks.length, tests.length)
  
        for (const index in tasks) {
          const task = tasks[index];
          const test = tests[index];
  
          expect({ activity_code: task.activity_code, status: task.status, input: storage.parse_result_error(task.input) }).to.deep.equal(test.expected)
        }
      })
  
      it('EXECUTE_TASK', async function() {
        this.timeout(30000);
  
        await TaskService.resetTask();
  
        tasks = await storage.all_task()
  
        for (const index in tasks) {
          const task = tasks[index];
          const test = tests[index];
  
          expect({ activity_code: task.activity_code, status: task.status, input: storage.parse_result_error(task.input) }).to.deep.equal(test.expected)
        }
  
        await TaskService.process();
  
        tasks = await storage.all_task()
  
        for (const index in tasks) {
          const task = tasks[index];
          const test = tests[index];
  
          expect({ activity_code: task.activity_code, status: task.status, result: storage.parse_result_error(task.result), error: storage.parse_result_error(task.error) }).to.deep.equal(test.expected_process_1)
        }
  
        await TaskService.process();
  
        tasks = await storage.all_task()
  
        for (const index in tasks) {
          const task = tasks[index];
          const test = tests[index];
  
          expect({ activity_code: task.activity_code, status: task.status, result: storage.parse_result_error(task.result), error: storage.parse_result_error(task.error) }).to.deep.equal(test.expected_process_1)
        }
  
        await sleep(4000);
  
        tasks = await storage.all_task()
  
        for (const index in tasks) {
          const task = tasks[index];
          const test = tests[index];
  
          expect({ activity_code: task.activity_code, status: task.status, result: storage.parse_result_error(task.result), error: storage.parse_result_error(task.error) }).to.deep.equal(test.expected_process_3)
        }
  
        await TaskService.process();
  
        tasks = await storage.all_task()
  
        for (const index in tasks) {
          const task = tasks[index];
          const test = tests[index];
  
          expect({ activity_code: task.activity_code, status: task.status, result: storage.parse_result_error(task.result), error: storage.parse_result_error(task.error) }).to.deep.equal(test.expected_process_4)
        }
  
        await TaskService.process();
  
        tasks = await storage.all_task()
  
        for (const index in tasks) {
          const task = tasks[index];
          const test = tests[index];
  
          expect({ activity_code: task.activity_code, status: task.status, result: storage.parse_result_error(task.result), error: storage.parse_result_error(task.error) }).to.deep.equal(test.expected_process_4)
        }
  
        await sleep(4000);
  
        tasks = await storage.all_task()
  
        for (const index in tasks) {
          const task = tasks[index];
          const test = tests[index];
  
          expect({ activity_code: task.activity_code, status: task.status, result: storage.parse_result_error(task.result), error: storage.parse_result_error(task.error) }).to.deep.equal(test.expected_process_5)
        }
  
        await sleep(4000);
  
        await TaskService.process();
  
        tasks = await storage.all_task()
  
        for (const index in tasks) {
          const task = tasks[index];
          const test = tests[index];
  
          expect({ activity_code: task.activity_code, status: task.status, result: storage.parse_result_error(task.result), error: storage.parse_result_error(task.error) }).to.deep.equal(test.expected_process_6)
        }
  
        await sleep(4000);
  
        tasks = await storage.all_task()
  
        for (const index in tasks) {
          const task = tasks[index];
          const test = tests[index];
  
          expect({ activity_code: task.activity_code, status: task.status, result: storage.parse_result_error(task.result), error: storage.parse_result_error(task.error) }).to.deep.equal(test.expected_process_7)
        }
  
      })   
      
      it('EXPIRE_TASK', async function() {
        if (storage.code === 'MONGO') {
          await TaskService.expiredTask();
          return
        }
        this.timeout(30000);

        await sleep(5000);
        await TaskService.expiredTask();
      
        tasks = await storage.all_task()

        const not_expire_test = tests.filter(test => test.expected_process_7.status != 'FINISHED')

        expect(tasks.length).to.equal(not_expire_test.length);

        for (const index in not_expire_test) {
          const task = tasks[index];
          const test = not_expire_test[index];

          if (test.expected_process_7.status != 'FINISHED') {
            expect({ activity_code: task.activity_code, status: task.status, result: storage.parse_result_error(task.result), error: storage.parse_result_error(task.error) }).to.deep.equal(test.expected_process_7)
          }
        }
      })

      it('RESET TASK', async () => {
        await storage.clear_record();

        const now = new Date();

        await storage.insert_record({
          activity_code: 'SEQUENCE',
          status: 'TEMPORARILY_FAILED',
          failed_at: new Date(now - 6),
          created_at: new Date(now - 6)
        });

        await storage.insert_record({
          activity_code: 'SEQUENCE',
          status: 'RUNNING',
          created_at: new Date(now - 5)
        });

        await storage.insert_record({
          activity_code: 'PARALLEL',
          status: 'RUNNING',
          created_at: new Date(now - 4)
        });

        await storage.insert_record({
          activity_code: 'PARALLEL_UNRETRYABLE',
          status: 'RUNNING',
          created_at: new Date(now - 3)
        });

        await storage.insert_record({
          activity_code: 'PARALLEL_LOWEST_PRIORITY',
          status: 'RUNNING',
          created_at: new Date(now - 2)
        });

        await storage.insert_record({
          activity_code: 'PARALLEL_LOWEST_PRIORITY',
          status: 'RUNNING',
          created_at: new Date(now - 1)
        });

        await storage.insert_record({
          activity_code: 'PARALLEL',
          status: 'TEMPORARILY_FAILED',
          failed_at: new Date(),
          created_at: new Date(now)
        }); 

        await TaskService.createTask({ activity_code: 'PARALLEL', input: {} })

        let tasks = await storage.all_task()
  
        for (const index in tasks) {
          const task = tasks[index];
          const test = reset_tests[index];
  
          expect({ activity_code: task.activity_code, status: task.status }).to.deep.equal(test.task)
        }

        await TaskService.resetTask();

        tasks = await storage.all_task()
  
        for (const index in tasks) {
          const task = tasks[index];
          const test = reset_tests[index];
  
          expect({ activity_code: task.activity_code, status: task.status }).to.deep.equal(test.expected_reset_1)
        }

        await TaskService.process();

        tasks = await storage.all_task()
  
        for (const index in tasks) {
          const task = tasks[index];
          const test = reset_tests[index];
  
          expect({ activity_code: task.activity_code, status: task.status }).to.deep.equal(test.expected_process_1)
        }
      })

      it('CRON', async function () {
        // start last
        if (storage.code != 'REDIS') {
          return;
        }
        this.timeout(5000)
        TaskManager.start({
          storage: storage.storage,
          task_limit: 3,
          verbose: true,
        });

        await sleep(4000)
      })
    })
  }
})