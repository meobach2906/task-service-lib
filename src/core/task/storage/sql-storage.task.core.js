const _ = require('lodash');
const uuid = require('uuid');

const _is = require('../../../utils/share/_is.utils.share');
const _ERR = require('../../../utils/share/_ERR.utils.share');
const _CONST = require("../../../utils/share/_CONST.utils.share");

class TaskSQLStorage {
  static storage = null;

  static #table_name = 'tasks'

  CODE = _CONST.TASK.STORAGE.SQL;

  static async init({ knex, expired = 10 * 24 * 60 * 60 }) {
    const storage = new TaskSQLStorage({ knex, expired });
    await storage.#initTable();
    return storage;
  }

  constructor({ knex, expired = 10 * 24 * 60 * 60 }) {
    if (!TaskSQLStorage.storage) {
      TaskSQLStorage.storage = this;
      this.knex = knex;
      this.expired = expired;
    }

    return TaskSQLStorage.storage;
  }

  async #initTable() {
    if (!(await this.knex.schema.hasTable(TaskSQLStorage.#table_name))) {
      await this.knex.schema.createTable(TaskSQLStorage.#table_name, (table) => {  
        table.uuid('_id').primary();
        table.string('activity_code').notNullable();
        table.string('status').defaultTo(_CONST.TASK.STATUS.IDLE);
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

  async expiredTask() {
    await this.knex(TaskSQLStorage.#table_name).del().where('finished_at', '<=', new Date(Date.now() - this.expired))
  }

  async createTask({ activity, input = {} }) {

    const result = {
      created_task: null,
    };

    const _id = uuid.v4();

    await this.knex(TaskSQLStorage.#table_name).insert({
      _id: _id,
      activity_code: activity.code,
      input: input,
      priority: activity.priority,
      max_retry_times: activity.max_retry_times,
    });

    result.created_task = await this.knex.select('*').from(TaskSQLStorage.#table_name).where({ _id: _id }).first();

    return result;
  }

  async resetTask({ retryable_activity_codes = [] }) {
    const now = new Date();
    await this.knex(TaskSQLStorage.#table_name).where({ status: _CONST.TASK.STATUS.RUNNING }).whereIn('activity_code', retryable_activity_codes).update({ status: _CONST.TASK.STATUS.TEMPORARILY_FAILED, error: JSON.stringify(_ERR.errorWithoutStack({ error: new _ERR.TEMPORARILY_ERR({ message: 'Retry when reset running' }) })), updated_at: now })
    await this.knex(TaskSQLStorage.#table_name).where({ status: _CONST.TASK.STATUS.RUNNING }).whereNotIn('activity_code', retryable_activity_codes).update({ status: _CONST.TASK.STATUS.FAILED,  error: JSON.stringify(_ERR.errorWithoutStack({ error: new _ERR.ERR({ message: 'Reset running' }) })), failed_at: now, updated_at: now })
  }

  async parallelTasks({ parallel_activity_codes = [], limit }) {
    const result = {
      runnable_tasks: [],
    };

    if (!_is.filled_array(parallel_activity_codes)) {
      return result;
    }

    const [{ running_slot }] = await this.knex.count('* AS running_slot').from(TaskSQLStorage.#table_name).where({ status: _CONST.TASK.STATUS.RUNNING }).whereIn('activity_code', parallel_activity_codes);

    let remain_slot = limit - Number(running_slot);

    if (!(remain_slot > 0)) {
      return result;
    }

    const reset_running_tasks = await this.knex.select('*').from(TaskSQLStorage.#table_name).where({ status: _CONST.TASK.STATUS.TEMPORARILY_FAILED }).whereIn('activity_code', parallel_activity_codes).whereNull('failed_at').orderBy('created_at', 'asc').limit(remain_slot);

    result.runnable_tasks.push(...reset_running_tasks);

    remain_slot -= reset_running_tasks.length;

    if (!(remain_slot > 0)) {
      return result;
    }

    const temporarily_fail_task = await this.knex.select('*').from(TaskSQLStorage.#table_name).where({ status: _CONST.TASK.STATUS.TEMPORARILY_FAILED }).whereIn('activity_code', parallel_activity_codes).whereNotNull('failed_at').orderBy('failed_at', 'asc').orderBy('created_at', 'asc').limit(remain_slot);

    result.runnable_tasks.push(...temporarily_fail_task);

    remain_slot -= temporarily_fail_task.length;


    if (!(remain_slot > 0)) {
      return result;
    }

    const idle_tasks = await this.knex.select('*').from(TaskSQLStorage.#table_name).where({ status: _CONST.TASK.STATUS.IDLE }).whereIn('activity_code', parallel_activity_codes).orderBy('priority', 'desc').orderBy('created_at', 'asc').limit(remain_slot);

    result.runnable_tasks.push(...idle_tasks);

    return result;
  }

  async sequenceTasks({ sequence_activity_codes = [], limit }) {
    const result = {
      runnable_tasks: [],
    };

    if (!_is.filled_array(sequence_activity_codes)) {
      return result;
    }

    const running_tasks = await this.knex.select('*').from(TaskSQLStorage.#table_name).where({ status: _CONST.TASK.STATUS.RUNNING }).whereIn('activity_code', sequence_activity_codes);

    let remain_slot = limit - running_tasks.length;

    if (!(remain_slot > 0)) {
      return result;
    }

    sequence_activity_codes = sequence_activity_codes.filter(activity_code => !running_tasks.find(running_task => running_task.activity_code === activity_code));

    if (!_is.filled_array(sequence_activity_codes)) {
      return result;
    }

    const reset_running_tasks = await this.knex.select('*').from(TaskSQLStorage.#table_name).where({ status: _CONST.TASK.STATUS.TEMPORARILY_FAILED }).whereIn('activity_code', sequence_activity_codes).whereNull('failed_at').orderBy('created_at', 'asc');

    for (const retryable_task of reset_running_tasks) {
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

    const temporarily_fail_task = await this.knex.select('*').from(TaskSQLStorage.#table_name).where({ status: _CONST.TASK.STATUS.TEMPORARILY_FAILED }).whereIn('activity_code', sequence_activity_codes).whereNotNull('failed_at').orderBy('failed_at', 'asc').orderBy('created_at', 'asc');

    for (const retryable_task of temporarily_fail_task) {
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
      const idle_task = await this.knex.select('*').from(TaskSQLStorage.#table_name).where({ status: _CONST.TASK.STATUS.IDLE, activity_code: activity_code }).orderBy('priority', 'desc').orderBy('created_at', 'asc').limit(1).first();
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
      await trx(TaskSQLStorage.#table_name).where({ _id: task._id }).update({ status: _CONST.TASK.STATUS.RUNNING, running_at: now, updated_at: now });
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
      await trx(TaskSQLStorage.#table_name).where({ _id: task._id }).update({ status: _CONST.TASK.STATUS.FINISHED, result: JSON.stringify(output), finished_at: now, updated_at: now });
      const updated_task = await trx.select('*').from(TaskSQLStorage.#table_name).where({ _id: task._id }).first();
      result.updated_task = updated_task;
    });

    return result;
  }

  async failTask({ task, activity, error }) {
    const result = {
      updated_task: null,
    };

    const now = new Date();

    const status = _is.retry({ error }) && (_is.activity.retryable({ activity }) && (!task.max_retry_times || task.retry_times + 1 < task.max_retry_times)) ? _CONST.TASK.STATUS.TEMPORARILY_FAILED : _CONST.TASK.STATUS.FAILED;

    await this.knex.transaction(async (trx) => {
      await trx(TaskSQLStorage.#table_name).where({ _id: task._id }).update({ status: status, error: JSON.stringify(_ERR.errorWithoutStack({ error })), failed_at: now, updated_at: now }).increment('retry_times', 1);
      const updated_task = await trx.select('*').from(TaskSQLStorage.#table_name).where({ _id: task._id }).first();
      result.updated_task = updated_task;
    });

    return result;
  }

  parseStorageObjectField(data) {
    return data ? JSON.parse(data) : null;
  }
}

module.exports = {
  TaskSQLStorage: TaskSQLStorage
};