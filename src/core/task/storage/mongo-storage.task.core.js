const _is = require('../../../utils/share/_is.utils.share');
const _ERR = require('../../../utils/share/_ERR.utils.share');
const _CONST = require('../../../utils/share/_CONST.utils.share');

const TaskManager = require('../manager/task-manager.core');

class TaskMongoStorage {
  static storage = null;

  static #schema_name = 'task';

  CODE = _CONST.TASK.STORAGE.MONGO;

  static async init({ knex }) {
    const storage = new TaskMongoStorage({ knex });
    await storage.#initModel();
  }

  constructor({ mongoose }) {
    if (!TaskMongoStorage.storage) {
      TaskMongoStorage.storage = this;
      this.mongoose = mongoose;
    }

    return TaskMongoStorage.storage;
  }

  async #initModel() {
    this.task_schema = new this.mongoose.Schema({
      activity_code: { type: String, require: true },
      status: { type: String, default: _CONST.TASK.STATUS.IDLE },
      input: { type: Object, default: null },
      result: { type: Object, default: null },
      error: { type: Object, default: null },
      priority: { type: Number, default: 0 },
      running_at: { type: Date, default: null },
      finished_at: { type: Date, default: null },
      failed_at: { type: Date, default: null },
      created_at: { type: Date, default: Date.now },
      updated_at: { type: Date, default: Date.now },
    });

    this.task_model = this.mongoose.model(TaskMongoStorage.#schema_name, this.task_schema);
  }

  async createTask({ task }) {
    await this.task_model.insert(task);
  }

  async resetTask({ unresetable_activity_codes = [] }) {
    if (_is.filled_array(unresetable_activity_codes)) {
      await this.task_model.updateMany({ status: { $in: [_CONST.TASK.STATUS.RUNNING], activity_code: { $in: unresetable_activity_codes } } }, { $set: { status: _CONST.TASK.STATUS.FAILED, failed_at: new Date() } })
    }
    await this.task_model.updateMany({ status: { $in: [_CONST.TASK.STATUS.RUNNING] } }, { $set: { status: _CONST.TASK.STATUS.TEMPORARILY_FAILED, failed_at: new Date() } })
  }

  async parallelTasks({ parallel_activity_codes = [] }) {
    const result = {
      runnable_tasks: [],
    };

    const limit = TaskManager.getTaskLimit();

    const running_slot = await this.task_model.count({ activity_code:  { $in: parallel_activity_codes }, status: _CONST.TASK.STATUS.RUNNING });

    let remain_slot = limit - running_slot;

    if (!(remain_slot > 0)) {
      return result;
    }

    const retryable_tasks = await this.task_model.find({ activity_code:  { $in: parallel_activity_codes }, status: _CONST.TASK.STATUS.TEMPORARILY_FAILED }).sort({ failed_at: 'asc', created_at: 'asc' }).limit(remain_slot);

    result.runnable_tasks.push(...retryable_tasks);

    remain_slot -= retryable_tasks.length;

    if (!(remain_slot > 0)) {
      return result;
    }

    const idle_tasks = await this.task_model.find({ activity_code:  { $in: parallel_activity_codes }, status: _CONST.TASK.STATUS.IDLE }).sort({ priority: 'desc', created_at: 'asc' }).limit(remain_slot);

    result.runnable_tasks.push(...idle_tasks);

    return result;
  }

  async sequenceTasks({ sequence_activity_codes = [] }) {
    const result = {
      runnable_tasks: [],
    };

    const limit = TaskManager.getTaskLimit();

    const running_tasks = await this.task_model.find({ activity_code:  { $in: sequence_activity_codes }, status: _CONST.TASK.STATUS.RUNNING });

    let remain_slot = limit - running_tasks.length;

    if (!(remain_slot > 0)) {
      return result;
    }

    sequence_activity_codes = sequence_activity_codes.filter(activity_code => !running_tasks.find(running_task => running_task.activity_code === activity_code));

    const retryable_tasks = await this.task_model.find({ activity_code:  { $in: parallel_activity_codes }, status: _CONST.TASK.STATUS.TEMPORARILY_FAILED }).sort({ failed_at: 'asc', created_at: 'asc' });

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

    if (!(remain_slot > 0)) {
      return result;
    }

    const idle_tasks = await this.task_model.aggregate([
      { $match: { activity_code:  { $in: sequence_activity_codes }, status: _CONST.TASK.STATUS.IDLE } },
      { $sort: { priority: 'desc', created_at: 'asc' } },
      { $group: { _id: '$activity_code', task: { $first: '$$ROOT' } } },
      { $replaceRoot: { newRoot: "$task" } },
      { $sort: { priority: 'desc', created_at: 'asc' } },
      { $limit: remain_slot }
    ])

    result.runnable_tasks.push(...idle_tasks);

    return result;
  }

  async startTask({ task }) {
    const result = {
      started_task: null,
    };

    result.started_task = await this.task_model.findOneAndUpdate({ _id: task._id, updated_at: task.updated_at }, { $set: { running_at: new Date(), status: _CONST.TASK.STATUS.RUNNING } }, { new: true });

    if (!result.started_task) {
      throw new Error(`Cannot start task because task updated before`);
    }

    return result;
  }

  async finishTask({ task, result }) {
    const result = {
      finished_task: null,
    };

    result.finished_task = await this.task_model.findOneAndUpdate({ _id: task._id }, { $set: { finished_at: new Date(), status: _CONST.TASK.STATUS.FINISHED, result: result } }, { new: true });

    return result;
  }

  async failTask({ task, error }) {
    const result = {
      failed_task: null,
    };

    const activity = TaskManager.assertActivity({ activity_code: task.activity_code })

    const status = _is.retry({ error }) && _is.activity.retryable({ activity }) ? _CONST.TASK.STATUS.TEMPORARILY_FAILED : _CONST.TASK.STATUS.FAILED;

    result.failed_task = await this.task_model.findOneAndUpdate({ _id: task._id }, { $set: { failed_at: new Date(), status: status, error: _ERR.errorWithoutStack({ error }) } }, { new: true });

    return result;
  }

  async process({ task, option: { is_not_waiting = true } }) {
    await this.startTask({ task });

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