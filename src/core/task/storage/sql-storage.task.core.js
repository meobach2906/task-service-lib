const _ = require('lodash');

const _is = require('../../../utils/share/_is.utils.share');
const _ERR = require('../../../utils/share/_ERR.utils.share');
const _CONST = require('../../../utils/share/_CONST.utils.share');

const TaskManager = require('../manager/task-manager.core');

class TaskSQLStorage {
  static storage = null;

  static #table_name = 'tasks'

  CODE = _CONST.TASK.STORAGE.SQL;

  static async init({ knex }) {
    const storage = new TaskSQLStorage({ knex });
    await storage.#initTable();
  }

  constructor({ knex }) {
    if (!TaskMongoStorage.storage) {
      TaskMongoStorage.storage = this;
      this.knex = knex;
    }

    return TaskSQLStorage.storage;
  }

  async #initTable() {
    if (!(await this.knex.schema.hasTable(TaskSQLStorage.#table_name))) {
      await this.knex.schema.createTable(TaskSQLStorage.#table_name, (table) => {  
        table.uuid('_id').primary().defaultTo(this.knex.raw('uuid_generate_v4()'));
        table.string('activity_code').notNullable();
        table.string('status').defaultTo(_CONST.TASK.STATUS.IDLE);
        table.string('input').nullable();
        table.string('result').nullable();
        table.string('error').nullable();
        table.integer('priority').nullable();
        table.timestamp('running_at').nullable();
        table.timestamp('finished_at').nullable();
        table.timestamp('failed_at').nullable();
        table.timestamp('created_at').defaultTo(knex.fn.now());
        table.timestamp('updated_at').defaultTo(knex.fn.now());
      });
    }
  }

  async createTask({ task }) {
    await this.knex.insert(task).into(TaskSQLStorage.#table_name);
  }

  async resetTask({ unresetable_activity_codes = [] }) {
    if (_is.filled_array(unresetable_activity_codes)) {
      await this.knex(TaskSQLStorage.#table_name).where({ status: _CONST.TASK.STATUS.RUNNING }).whereIn('activity_code', unresetable_activity_codes).update({ status: _CONST.TASK.STATUS.FAILED })
    }
    await this.knex(TaskSQLStorage.#table_name).where({ status: _CONST.TASK.STATUS.RUNNING }).update({ status: _CONST.TASK.STATUS.TEMPORARILY_FAILED })
  }

  async parallelTasks({ parallel_activity_codes = [] }) {
    const result = {
      runnable_tasks: [],
    };

    const limit = TaskManager.getTaskLimit();

    const [{ running_slot }] = await this.knex.count('* AS running_slot').from(TaskSQLStorage.#table_name).where({ status: _CONST.TASK.STATUS.RUNNING }).whereIn('activity_code', parallel_activity_codes);

    let remain_slot = limit - running_slot;

    if (!(remain_slot > 0)) {
      return result;
    }

    const retryable_tasks = await this.knex.select('*').from(TaskSQLStorage.#table_name).where({ status: _CONST.TASK.STATUS.TEMPORARILY_FAILED }).whereIn('activity_code', parallel_activity_codes).orderBy('failed_at', 'asc').orderBy('created_at', 'asc').limit(remain_slot);

    result.runnable_tasks.push(...retryable_tasks);

    remain_slot -= retryable_tasks.length;

    if (!(remain_slot > 0)) {
      return result;
    }

    const idle_tasks = await this.knex.select('*').from(TaskSQLStorage.#table_name).where({ status: _CONST.TASK.STATUS.IDLE }).whereIn('activity_code', parallel_activity_codes).orderBy('priority', 'desc').orderBy('created_at', 'asc');limit(remain_slot);

    result.runnable_tasks.push(...idle_tasks);

    return result;
  }

  async sequenceTasks({ sequence_activity_codes = [] }) {
    const result = {
      runnable_tasks: [],
    };

    const limit = TaskManager.getTaskLimit();

    const running_tasks = await this.knex.select('*').from(TaskSQLStorage.#table_name).where({ status: _CONST.TASK.STATUS.RUNNING }).whereIn('activity_code', sequence_activity_codes);

    let remain_slot = limit - running_tasks.length;

    if (!(remain_slot > 0)) {
      return result;
    }

    sequence_activity_codes = sequence_activity_codes.filter(activity_code => !running_tasks.find(running_task => running_task.activity_code === activity_code));

    const retryable_tasks = await this.knex.select('*').from(TaskSQLStorage.#table_name).where({ status: _CONST.TASK.STATUS.TEMPORARILY_FAILED }).whereIn('activity_code', sequence_activity_codes).orderBy('failed_at', 'asc').orderBy('created_at', 'asc');

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

    const idle_tasks = [];
    for (const activity_code in sequence_activity_codes) {
      const [idle_task] = await this.knex.select('*').from(TaskSQLStorage.#table_name).where({ status: _CONST.TASK.STATUS.IDLE, activity_code: activity_code }).orderBy('priority', 'desc').orderBy('created_at', 'asc').limit(1);
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
      started_task: null,
    };

    const [started_task] = await this.knex(TaskSQLStorage.#table_name).where({ _id: task._id, updated_at: task.updated_at }).update({ running_at: new Date(), status: _CONST.TASK.STATUS.RUNNING }).returning('*');
    result.started_task = started_task;

    if (!result.started_task) {
      throw new Error(`Cannot start task because task updated before`);
    }

    return result;
  }

  async finishTask({ task, result }) {
    const result = {
      finished_task: null,
    };

    const [finished_task] = await this.knex(TaskSQLStorage.#table_name).where({ _id: task._id }).update({ finished_at: new Date(), status: _CONST.TASK.STATUS.FINISHED, result: JSON.stringify(result) }).returning('*');
    result.finished_task = finished_task;

    return result;
  }

  async failTask({ task, error }) {
    const result = {
      failed_task: null,
    };

    const activity = TaskManager.assertActivity({ activity_code: task.activity_code })

    const status = _is.retry({ error }) && _is.activity.retryable({ activity })  ? _CONST.TASK.STATUS.TEMPORARILY_FAILED : _CONST.TASK.STATUS.FAILED;

    const [failed_task] = await this.knex(TaskSQLStorage.#table_name).where({ _id: task._id }).update({ failed_at: new Date(), status: status, error: JSON.stringify(_ERR.errorWithoutStack({ error })) }).returning('*');
    result.failed_task = failed_task;

    return result;
  }

  async process({ task, option: { is_not_waiting = true } }) {
    await this.startTask({ task });

    task.input = JSON.parse(task.input);

    const promise = TaskManager.processTask({ task })
      .then(async result => {
        console.log(`[FINISHED] [PROCESS_TASK] [ID: ${String(task._id)}] [RESULT: ${JSON.stringify(result)}]`);
        await this.finishTask({ task, result });
      })
      .catch(async error => {
        console.log(`[FAILED] [PROCESS_TASK] [ID: ${String(task._id)}] [ERROR: ${_ERR.stringify({ error })}]`);
        await this.failTask({ task, error });
      })

    if (!is_not_waiting) {
      await promise;
    }
  }

}

module.exports = {
  TaskMongoStorage: TaskMongoStorage
};