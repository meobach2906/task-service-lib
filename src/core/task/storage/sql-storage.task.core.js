const _ = require('lodash');
const uuid = require('uuid');

const _is = require('../../../utils/share/_is.utils.share');
const _ERR = require('../../../utils/share/_ERR.utils.share');

const { TaskManager } = require('../manager/task-manager.core');

class TaskSQLStorage {
  static storage = null;

  static #table_name = 'tasks'

  CODE = TaskManager.TASK_CONST.STORAGE.SQL;

  static async init({ knex }) {
    const storage = new TaskSQLStorage({ knex });
    await storage.#initTable();
    return storage;
  }

  constructor({ knex }) {
    if (!TaskSQLStorage.storage) {
      TaskSQLStorage.storage = this;
      this.knex = knex;
    }

    return TaskSQLStorage.storage;
  }

  async #initTable() {
    if (!(await this.knex.schema.hasTable(TaskSQLStorage.#table_name))) {
      await this.knex.schema.createTable(TaskSQLStorage.#table_name, (table) => {  
        table.uuid('_id').primary();
        table.string('activity_code').notNullable();
        table.string('status').defaultTo(TaskManager.TASK_CONST.STATUS.IDLE);
        table.string('input').nullable();
        table.string('result').nullable();
        table.string('error').nullable();
        table.integer('priority').nullable();
        table.integer('max_retry_times').nullable().defaultTo(null);
        table.integer('retry_times').defaultTo(0);
        table.timestamp('running_at').nullable();
        table.timestamp('finished_at').nullable();
        table.timestamp('failed_at').nullable();
        table.timestamp('created_at').defaultTo(this.knex.fn.now());
        table.timestamp('updated_at').defaultTo(this.knex.fn.now());

        table.index(['status', 'created_at']);
        table.index(['status', 'failed_at', 'created_at']);
        table.index(['status', 'priority', 'created_at']);
      });
    }
  }

  async createTask({ activity_code, input = {} }) {

    const result = {
      created_task: null,
    };

    const _id = uuid.v4();
    const activity = TaskManager.assertActivity({ code: activity_code });

    await this.knex(TaskSQLStorage.#table_name).insert({
      _id: _id,
      activity_code: activity_code,
      input: input,
      priority: activity.priority,
      max_retry_times: activity.max_retry_times,
    });

    result.created_task = await this.knex.select('*').from(TaskSQLStorage.#table_name).where({ _id: _id }).first();

    return result;
  }

  async resetTask({ retryable_activity_codes = [] }) {
    const now = new Date();
    await this.knex(TaskSQLStorage.#table_name).where({ status:TaskManager.TASK_CONST.STATUS.RUNNING }).whereIn('activity_code', retryable_activity_codes).update({ status:TaskManager.TASK_CONST.STATUS.TEMPORARILY_FAILED, error: JSON.stringify(_ERR.errorWithoutStack({ error: new _ERR.TEMPORARILY_ERR({ message: 'Retry when reset running' }) })), failed_at: now, updated_at: now })
    await this.knex(TaskSQLStorage.#table_name).where({ status:TaskManager.TASK_CONST.STATUS.RUNNING }).whereNotIn('activity_code', retryable_activity_codes).update({ status:TaskManager.TASK_CONST.STATUS.FAILED,  error: JSON.stringify(_ERR.errorWithoutStack({ error: new _ERR.ERR({ message: 'Reset running' }) })), failed_at: now, updated_at: now })
  }

  async parallelTasks({ parallel_activity_codes = [] }) {
    const result = {
      runnable_tasks: [],
    };

    if (!_is.filled_array(parallel_activity_codes)) {
      return result;
    }

    const limit = TaskManager.getTaskLimit();

    const [{ running_slot }] = await this.knex.count('* AS running_slot').from(TaskSQLStorage.#table_name).where({ status:TaskManager.TASK_CONST.STATUS.RUNNING }).whereIn('activity_code', parallel_activity_codes);

    let remain_slot = limit - Number(running_slot);

    if (!(remain_slot > 0)) {
      return result;
    }

    const retryable_tasks = await this.knex.select('*').from(TaskSQLStorage.#table_name).where({ status:TaskManager.TASK_CONST.STATUS.TEMPORARILY_FAILED }).whereIn('activity_code', parallel_activity_codes).orderBy('failed_at', 'asc').orderBy('created_at', 'asc').limit(remain_slot);

    result.runnable_tasks.push(...retryable_tasks);

    remain_slot -= retryable_tasks.length;

    if (!(remain_slot > 0)) {
      return result;
    }

    const idle_tasks = await this.knex.select('*').from(TaskSQLStorage.#table_name).where({ status:TaskManager.TASK_CONST.STATUS.IDLE }).whereIn('activity_code', parallel_activity_codes).orderBy('priority', 'desc').orderBy('created_at', 'asc').limit(remain_slot);

    result.runnable_tasks.push(...idle_tasks);

    return result;
  }

  async sequenceTasks({ sequence_activity_codes = [] }) {
    const result = {
      runnable_tasks: [],
    };

    if (!_is.filled_array(sequence_activity_codes)) {
      return result;
    }

    const limit = TaskManager.getTaskLimit();

    const running_tasks = await this.knex.select('*').from(TaskSQLStorage.#table_name).where({ status:TaskManager.TASK_CONST.STATUS.RUNNING }).whereIn('activity_code', sequence_activity_codes);

    let remain_slot = limit - running_tasks.length;

    if (!(remain_slot > 0)) {
      return result;
    }

    sequence_activity_codes = sequence_activity_codes.filter(activity_code => !running_tasks.find(running_task => running_task.activity_code === activity_code));

    if (!_is.filled_array(sequence_activity_codes)) {
      return result;
    }

    const retryable_tasks = await this.knex.select('*').from(TaskSQLStorage.#table_name).where({ status:TaskManager.TASK_CONST.STATUS.TEMPORARILY_FAILED }).whereIn('activity_code', sequence_activity_codes).orderBy('failed_at', 'asc').orderBy('created_at', 'asc');

    for (const retryable_task of retryable_tasks) {
      if (!result.runnable_tasks.find(runnable_task => runnable_task.activity_code === retryable_task.activity_code)) {
        result.runnable_tasks.push(retryable_task);

        remain_slot -= 1;
      }

      if (!(remain_slot > 0)) {
        break;
      }
    }

    sequence_activity_codes = sequence_activity_codes.filter(activity_code => !result.runnable_tasks.find(runnable_task => runnable_task.activity_code === activity_code));

    if (!_is.filled_array(sequence_activity_codes)) {
      return result;
    }

    const idle_tasks = [];
    for (const activity_code of sequence_activity_codes) {
      const idle_task = await this.knex.select('*').from(TaskSQLStorage.#table_name).where({ status:TaskManager.TASK_CONST.STATUS.IDLE, activity_code: activity_code }).orderBy('priority', 'desc').orderBy('created_at', 'asc').limit(1).first();
      if (idle_task) {
        idle_tasks.push(idle_task);
      }
    }

    const runnable_idle_tasks = _.orderBy(idle_tasks, ['priority', 'created_at'], ['desc', 'asc']).slice(0, remain_slot);

    result.runnable_tasks.push(...runnable_idle_tasks);

    return result;
  }

  async startTask({ task }) {
    const result = {
      updated_task: null,
    };

    const now = new Date();

    await this.knex.transaction(async (trx) => {
      await trx(TaskSQLStorage.#table_name).where({ _id: task._id }).update({ status: TaskManager.TASK_CONST.STATUS.RUNNING, running_at: now, updated_at: now });
      const updated_task = await trx.select('*').from(TaskSQLStorage.#table_name).where({ _id: task._id, updated_at: now }).first();
      result.updated_task = updated_task;
    });

    return result;
  }

  async finishTask({ task, result: output }) {
    const result = {
      updated_task: null,
    };

    const now = new Date();

    await this.knex.transaction(async (trx) => {
      await trx(TaskSQLStorage.#table_name).where({ _id: task._id }).update({ status:TaskManager.TASK_CONST.STATUS.FINISHED, result: JSON.stringify(output), finished_at: now, updated_at: now });
      const updated_task = await trx.select('*').from(TaskSQLStorage.#table_name).where({ _id: task._id }).first();
      result.updated_task = updated_task;
    });

    return result;
  }

  async failTask({ task, error }) {
    const result = {
      updated_task: null,
    };

    const now = new Date();

    const activity = TaskManager.assertActivity({ code: task.activity_code })

    const status = _is.retry({ error }) && (_is.activity.retryable({ activity }) && (!task.max_retry_times || task.retry_times + 1 < task.max_retry_times)) ? TaskManager.TASK_CONST.STATUS.TEMPORARILY_FAILED : TaskManager.TASK_CONST.STATUS.FAILED;

    await this.knex.transaction(async (trx) => {
      await trx(TaskSQLStorage.#table_name).where({ _id: task._id }).update({ status: status, error: JSON.stringify(_ERR.errorWithoutStack({ error })), failed_at: now, updated_at: now }).increment('retry_times', 1);
      const updated_task = await trx.select('*').from(TaskSQLStorage.#table_name).where({ _id: task._id }).first();
      result.updated_task = updated_task;
    });

    return result;
  }

  async process({ task }) {
    const { updated_task } = await this.startTask({ task });

    updated_task.input = JSON.parse(updated_task.input);

    const promise = TaskManager.processTask({ task: updated_task })
      .then(async result => {
        console.log(`[FINISHED] [PROCESS_TASK] [ID: ${String(task._id)}] [RESULT: ${JSON.stringify(result)}]`);
        return await this.finishTask({ task: updated_task, result });
      })
      .catch(async error => {
        console.log(`[FAILED] [PROCESS_TASK] [ID: ${String(task._id)}] [ERROR: ${_ERR.stringify({ error })}]`);
        return await this.failTask({ task: updated_task, error });
      })

    return promise;
  }

}

module.exports = {
  TaskSQLStorage: TaskSQLStorage
};