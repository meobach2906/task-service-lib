const _ = require('lodash');
const uuid = require('uuid');

const _is = require('../../../utils/share/_is.utils.share');
const _ERR = require('../../../utils/share/_ERR.utils.share');

const { TaskManager } = require('../manager/task-manager.core');

class TaskRedisStorage {
  static storage = null;

  CODE = TaskManager.TASK_CONST.STORAGE.REDIS;

  static async init({ redis }) {
    const storage = new TaskRedisStorage({ redis });
    return storage;
  }

  constructor({ redis }) {
    if (!TaskRedisStorage.storage) {
      TaskRedisStorage.storage = this;
      this.redis = redis;
    }

    return TaskRedisStorage.storage;
  }

  async expiredTask() {
    const expired_at = new Date(Date.now() - this.expired);

    let start = 0;
    let expired_index = null;
    const page_size = 100;
    let done = true;

    let script = ``;

    while(done) {
      const items = await this.redis.lRange(`finished_tasks`, start, start + page_size - 1);

      if (!_is.filled_array(items)) {
        done = true;
        break;
      }

      for (const index in items) {
        const item = JSON.parse(items[index]);

        if (new Date(item.finished_at) >= expired_at) {
          done = false;
          break;
        }

        expired_index = start + Number(index);

        script += `
          redis.call('DEL', 'task:${item._id}')
        `;
      }

      start += page_size;
    }

    if (expired_index != null) {
      script += `
        redis.call('LTRIM', 'finished_tasks', 0, ${expired_index})
      `;

      await this.redis.eval(script, { keys: [], arguments: [] });
    }
  }

  async createTask({ activity_code, input = {} }) {
    const result = {
      created_task: null,
    };

    const activity = TaskManager.assertActivity({ code: activity_code });

    const _id = uuid.v4();

    const now = new Date();

    const task = {
      _id: _id,
      activity_code: activity_code,
      status: TaskManager.TASK_CONST.STATUS.IDLE,
      input: JSON.stringify(input),
      priority: String(activity.priority),
      max_retry_times: String(activity.max_retry_times),
      retry_times: String(0),
      created_at: now.toISOString(),
      updated_at: now.toISOString()
    }

    const script = `
      local created_task = redis.call('HSET', KEYS[1], '_id', ARGV[1], 'activity_code', ARGV[2], 'status', ARGV[3], 'input', ARGV[4], 'priority', ARGV[5], 'max_retry_times', ARGV[6], 'retry_times', ARGV[7], 'created_at', ARGV[8], 'updated_at', ARGV[9])

      redis.call('RPUSH', KEYS[2], ARGV[10])

      return created_task
    `

    result.created_task = await this.redis.eval(script, {
      keys: [`task:${task._id}`, `idle_tasks:${task.activity_code}:${task.priority}`],
      arguments: [task._id, task.activity_code, task.status, task.input, task.priority, task.max_retry_times, task.retry_times, task.created_at, task.updated_at, JSON.stringify({ _id: task._id, activity_code: task.activity_code, priority: task.priority, created_at: task.created_at })]
    })

    if (TaskManager.isTest()) {
      await this.redis.rPush(`tasks`, task._id)
    }

    return result;
  }

  async resetTask({ retryable_activity_codes = [] }) {
    const now = new Date();
    const running_tasks = await this.redis.lRange(`running_tasks`, 0, -1);

    const retryable_tasks = [];
    const unretryable_tasks = [];
    for (let running_task of running_tasks) {
      running_task = JSON.parse(running_task);

      if (retryable_activity_codes.includes(running_task.activity_code)) {
        retryable_tasks.push(running_task);
      } else {
        unretryable_tasks.push(running_task);
      }
    }

    let script = `
      redis.call('DEL', 'running_tasks')
    `;

    if (_is.filled_array(unretryable_tasks)) {
      for (const unretryable_task of unretryable_tasks) {
        script += `
          redis.call('HSET', 'task:${unretryable_task._id}', 'status', 'FAILED', 'error', '${JSON.stringify(_ERR.errorWithoutStack({ error: new _ERR.ERR({ message: 'Reset running' }) }))}', 'failed_at', '${now.toISOString()}', 'updated_at', '${now.toISOString()}')
        `;
      }
    }

    if (_is.filled_array(retryable_tasks)) {
      script += `
        redis.call('LPUSH', 'temporarily_failed_tasks', ${retryable_tasks.map((_, index) => `ARGV[${index + 1}]`).join(', ')})
      `;
      for (const retryable_task of retryable_tasks) {
        script += `
          redis.call('HSET', 'task:${retryable_task._id}', 'status', 'TEMPORARILY_FAILED', 'error', '${JSON.stringify(_ERR.errorWithoutStack({ error: new _ERR.TEMPORARILY_ERR({ message: 'Retry when reset running' }) }))}', 'updated_at', '${now.toISOString()}')
        `;
      }
    }

    await this.redis.eval(script, {
      keys: [],
      arguments: [...retryable_tasks.reverse().map(retryable_task => JSON.stringify(retryable_task))]
    })
  }

  async parallelTasks({ parallel_activity_codes = [] }) {
    const result = {
      runnable_tasks: [],
    };

    if (!_is.filled_array(parallel_activity_codes)) {
      return result;
    }

    let remain_slot = TaskManager.getTaskLimit();

    const running_tasks = await this.redis.lRange(`running_tasks`, 0, -1);
    const parallel_running_tasks = [];

    for (let running_task of running_tasks) {
      running_task = JSON.parse(running_task);

      if (parallel_activity_codes.includes(running_task.activity_code)) {
        parallel_running_tasks.push(running_task);
        remain_slot -= 1;
      }

      if (!(remain_slot > 0)) {
        break;
      }
    };

    if (!(remain_slot > 0)) {
      return result;
    }

    const retryable_tasks = await this.redis.lRange(`temporarily_failed_tasks`, 0, -1);

    for (let retryable_task of retryable_tasks) {

      retryable_task = JSON.parse(retryable_task);

      if (parallel_activity_codes.includes(retryable_task.activity_code)) {
        const task = await this.redis.hGetAll(`task:${retryable_task._id}`);
        result.runnable_tasks.push(task);
        remain_slot -= 1;
      }

      if (!(remain_slot > 0)) {
        break;
      }
    };

    if (!(remain_slot > 0)) {
      return result;
    }

    let idle_tasks = [];

    for (const activity_code of parallel_activity_codes) {

      let activity_code_slot = remain_slot;

      const { keys } = await this.redis.scan('0', {
        MATCH: `idle_tasks:${activity_code}:*`,
        COUNT: 100,
      });

      const priorities = keys.map(key => Number(key.split(':')[2])).sort((a, b) => b - a);

      for (const priority of priorities) {
        const idle_activity_code_tasks = await this.redis.lRange(`idle_tasks:${activity_code}:${priority}`, 0, activity_code_slot);

        idle_tasks.push(...idle_activity_code_tasks);

        activity_code_slot -= idle_activity_code_tasks.length;

        if (!(activity_code_slot > 0)) {
          break;
        }
      }
    }

    idle_tasks = idle_tasks.map(idle_task => JSON.parse(idle_task))

    idle_tasks = _.orderBy(idle_tasks, ['priority', idle_task => new Date(idle_task.created_at)], ['desc', 'asc']).splice(0, remain_slot);
 
    for (const idle_task of idle_tasks) {
      const task = await this.redis.hGetAll(`task:${idle_task._id}`);
      result.runnable_tasks.push(task);
    }

    return result;
  }

  async sequenceTasks({ sequence_activity_codes = [] }) {
    const result = {
      runnable_tasks: [],
    };

    let remain_slot = TaskManager.getTaskLimit();

    const running_tasks = await this.redis.lRange(`running_tasks`, 0, -1);
    const sequence_running_tasks = [];

    for (let running_task of running_tasks) {
      running_task = JSON.parse(running_task);

      if (sequence_activity_codes.includes(running_task.activity_code)) {
        sequence_running_tasks.push(running_task);
        remain_slot -= 1;
      }

      if (!(remain_slot > 0)) {
        break;
      }
    };

    if (!(remain_slot > 0)) {
      return result;
    }
    
    sequence_activity_codes = sequence_activity_codes.filter(activity_code => !sequence_running_tasks.find(running_task => running_task.activity_code === activity_code));

    if (!_is.filled_array(sequence_activity_codes)) {
      return result;
    }

    const retryable_tasks = await this.redis.lRange(`temporarily_failed_tasks`, 0, -1);

    for (let retryable_task of retryable_tasks) {

      retryable_task = JSON.parse(retryable_task);

      if (sequence_activity_codes.includes(retryable_task.activity_code)) {
        sequence_activity_codes = sequence_activity_codes.filter(sequence_activity_code => sequence_activity_code != retryable_task.activity_code)
        const task = await this.redis.hGetAll(`task:${retryable_task._id}`);
        result.runnable_tasks.push(task);
        remain_slot -= 1;
      }

      if (!_is.filled_array(sequence_activity_codes)) {
        break;
      }

      if (!(remain_slot > 0)) {
        break;
      }
    };

    if (!(remain_slot > 0)) {
      return result;
    }

    if (!_is.filled_array(sequence_activity_codes)) {
      return result;
    }

    let idle_tasks = [];

    for (const activity_code of sequence_activity_codes) {
      const { keys } = await this.redis.scan('0', {
        MATCH: `idle_tasks:${activity_code}:*`,
        COUNT: 100
      });

      const priorities = keys.map(key => Number(key.split(':')[2])).sort((a, b) => b - a);

      for (const priority of priorities) {
        const idle_activity_code_task = await this.redis.lRange(`idle_tasks:${activity_code}:${priority}`, 0, 0);

        if (idle_activity_code_task[0]) {
          idle_tasks.push(idle_activity_code_task[0]);
          break;
        }
      }
    }

    idle_tasks = idle_tasks.map(idle_task => JSON.parse(idle_task))

    idle_tasks = _.orderBy(idle_tasks, ['priority', idle_task => new Date(idle_task.created_at)], ['desc', 'asc']).splice(0, remain_slot);

    for (const idle_task of idle_tasks) {
      const task = await this.redis.hGetAll(`task:${idle_task._id}`);
      result.runnable_tasks.push(task);
    }

    return result;
  }

  async startTask({ task }) {
    const result = {
      updated_task: null,
    };

    const now = new Date();

    const script = `
      local updated_task = redis.call('HSET', KEYS[1], 'status', ARGV[2], 'running_at', ARGV[3], 'updated_at', ARGV[3])

      if ARGV[4] == '${TaskManager.TASK_CONST.STATUS.IDLE}' then
        redis.call('LREM', KEYS[2], 0, ARGV[5])
      end

      if ARGV[4] == '${TaskManager.TASK_CONST.STATUS.TEMPORARILY_FAILED}' then
        redis.call('LREM', 'temporarily_failed_tasks', 0, ARGV[5])
      end

      redis.call('RPUSH', 'running_tasks', ARGV[5])

      return updated_task
    `;

    result.updated_task = await this.redis.eval(script, {
      keys: [`task:${task._id}`, `idle_tasks:${task.activity_code}:${task.priority}`],
      arguments: [task._id, TaskManager.TASK_CONST.STATUS.RUNNING, now.toISOString(), task.status, JSON.stringify({ _id: task._id, activity_code: task.activity_code, priority: task.priority, created_at: task.created_at })]
    })

    result.updated_task = await this.redis.hGetAll(`task:${task._id}`);

    return result;
  }

  async finishTask({ task, result: output }) {
    const result = {
      updated_task: null,
    };

    const activity = TaskManager.assertActivity({ code: task.activity_code })

    const now = new Date();

    const script = `
      local updated_task = redis.call('HSET', KEYS[1], 'status', ARGV[2], 'result', ARGV[3],'finished_at', ARGV[4], 'updated_at', ARGV[4])

      redis.call('LREM', 'running_tasks', 0, ARGV[5])

      redis.call('RPUSH', 'finished_tasks', ARGV[6])

      return updated_task
    `;

    await this.redis.eval(script, {
      keys: [`task:${task._id}`],
      arguments: [task._id, TaskManager.TASK_CONST.STATUS.FINISHED, output ? JSON.stringify(output) : '', now.toISOString(), JSON.stringify({ _id: task._id, activity_code: task.activity_code, priority: task.priority, created_at: task.created_at }), JSON.stringify({ _id: task._id, activity_code: task.activity_code, finished_at: now.toISOString() })]
    })

    result.updated_task = await this.redis.hGetAll(`task:${task._id}`);

    return result;
  }

  async failTask({ task, error }) {
    const result = {
      updated_task: null,
    };

    const activity = TaskManager.assertActivity({ code: task.activity_code });

    const now = new Date();

    const status = _is.retry({ error }) && (_is.activity.retryable({ activity }) && (!Number(task.max_retry_times) || Number(task.retry_times) + 1 < Number(task.max_retry_times))) ? TaskManager.TASK_CONST.STATUS.TEMPORARILY_FAILED : TaskManager.TASK_CONST.STATUS.FAILED;

    const script = `
      local updated_task = redis.call('HSET', KEYS[1], 'status', ARGV[2], 'error', ARGV[3], 'failed_at', ARGV[4], 'updated_at', ARGV[4], 'retry_times', ARGV[5])

      redis.call('LREM', 'running_tasks', 0, ARGV[6])

      if ARGV[2] == '${TaskManager.TASK_CONST.STATUS.TEMPORARILY_FAILED}' then
        redis.call('RPUSH', 'temporarily_failed_tasks', ARGV[6])
      end

      return updated_task
    `;

    await this.redis.eval(script, {
      keys: [`task:${task._id}`],
      arguments: [task._id, status, error ? JSON.stringify(_ERR.errorWithoutStack({ error })) : '', now.toISOString(), String(Number(task.retry_times) + 1), JSON.stringify({ _id: task._id, activity_code: task.activity_code, priority: task.priority, created_at: task.created_at })]
    })

    result.updated_task = await this.redis.hGetAll(`task:${task._id}`);

    return result;

  }

  async process({ task, is_waiting = false }) {
    const { updated_task } = await this.startTask({ task });

    updated_task.input = JSON.parse(updated_task.input);

    const promise = TaskManager.processTask({ task: updated_task })
      .then(async result => {
        TaskManager.log(`[FINISHED] [PROCESS_TASK] [ID: ${String(task._id)}] [RESULT: ${JSON.stringify(result)}]`);
        return await this.finishTask({ task: updated_task, result });
      })
      .catch(async error => {
        TaskManager.log(`[FAILED] [PROCESS_TASK] [ID: ${String(task._id)}] [ERROR: ${_ERR.stringify({ error })}]`);
        return await this.failTask({ task: updated_task, error });
      })

    if (is_waiting) {
      return await promise;
    }

    return { updated_task };
  }

}

module.exports = {
  TaskRedisStorage: TaskRedisStorage
};