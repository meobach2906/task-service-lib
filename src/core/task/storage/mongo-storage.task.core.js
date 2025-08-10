const _is = require('../../../utils/share/_is.utils.share');
const _ERR = require('../../../utils/share/_ERR.utils.share');

const { TaskManager } = require('../manager/task-manager.core');

class TaskMongoStorage {
  static storage = null;

  static #schema_name = 'task';

  CODE = TaskManager.TASK_CONST.STORAGE.MONGO;

  static async init({ mongoose }) {
    const storage = new TaskMongoStorage({ mongoose });
    await storage.#initModel();
    return storage;
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
      status: { type: String, default: TaskManager.TASK_CONST.STATUS.IDLE },
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
    });

    this.task_schema.index({ status: 1, created_at: 1 });
    this.task_schema.index({ status: 1, failed_at: 1, created_at: 1 });
    this.task_schema.index({ status: 1, priority: 1, created_at: 1 });

    this.task_model = this.mongoose.model(TaskMongoStorage.#schema_name, this.task_schema);
  }

  async createTask({ activity_code, input = {} }) {
    const result = {
      created_task: null,
    };

    const activity = TaskManager.assertActivity({ code: activity_code });

    result.created_task = await this.task_model.create({
      activity_code: activity_code,
      input: input,
      priority: activity.priority,
      max_retry_times: activity.max_retry_times,
    });

    return result;
  }

  async resetTask({ retryable_activity_codes = [] }) {
    const now = new Date();
    await this.task_model.updateMany({ status:TaskManager.TASK_CONST.STATUS.RUNNING, activity_code: { $in: retryable_activity_codes } }, { $set: { status:TaskManager.TASK_CONST.STATUS.TEMPORARILY_FAILED, error: _ERR.errorWithoutStack({ error: new _ERR.TEMPORARILY_ERR({ message: 'Retry when reset running' }) }), failed_at: now, updated_at: now } })
    await this.task_model.updateMany({ status:TaskManager.TASK_CONST.STATUS.RUNNING, activity_code: { $nin: retryable_activity_codes } }, { $set: { status:TaskManager.TASK_CONST.STATUS.FAILED, error: _ERR.errorWithoutStack({ error: new _ERR.ERR({ message: 'Reset running' }) }), failed_at: now, updated_at: now  } })
  }

  async parallelTasks({ parallel_activity_codes = [] }) {
    const result = {
      runnable_tasks: [],
    };

    if (!_is.filled_array(parallel_activity_codes)) {
      return result;
    }

    const limit = TaskManager.getTaskLimit();

    const running_slot = await this.task_model.count({ activity_code: { $in: parallel_activity_codes }, status:TaskManager.TASK_CONST.STATUS.RUNNING });

    let remain_slot = limit - running_slot;

    if (!(remain_slot > 0)) {
      return result;
    }

    const retryable_tasks = await this.task_model.find({ activity_code:  { $in: parallel_activity_codes }, status:TaskManager.TASK_CONST.STATUS.TEMPORARILY_FAILED }).sort({ failed_at: 'asc', created_at: 'asc' }).limit(remain_slot).lean(true);

    result.runnable_tasks.push(...retryable_tasks);

    remain_slot -= retryable_tasks.length;

    if (!(remain_slot > 0)) {
      return result;
    }

    const idle_tasks = await this.task_model.find({ activity_code: { $in: parallel_activity_codes }, status:TaskManager.TASK_CONST.STATUS.IDLE }).sort({ priority: 'desc', created_at: 'asc' }).limit(remain_slot).lean(true);

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

    const running_tasks = await this.task_model.find({ activity_code: { $in: sequence_activity_codes }, status:TaskManager.TASK_CONST.STATUS.RUNNING }).lean(true);

    let remain_slot = limit - running_tasks.length;

    if (!(remain_slot > 0)) {
      return result;
    }

    sequence_activity_codes = sequence_activity_codes.filter(activity_code => !running_tasks.find(running_task => running_task.activity_code === activity_code));

    if (!_is.filled_array(sequence_activity_codes)) {
      return result;
    }

    const retryable_tasks = await this.task_model.find({ activity_code: { $in: sequence_activity_codes }, status:TaskManager.TASK_CONST.STATUS.TEMPORARILY_FAILED }).sort({ failed_at: 'asc', created_at: 'asc' }).lean(true);

    for (const retryable_task of retryable_tasks) {
      if (!result.runnable_tasks.find(runnable_task => runnable_task.activity_code === retryable_task.activity_code)) {
        result.runnable_tasks.push(retryable_task);

        remain_slot -= 1;
      }

      if (!(remain_slot > 0)) {
        break;
      }
    }

    if (!(remain_slot > 0)) {
      return result;
    }

    sequence_activity_codes = sequence_activity_codes.filter(activity_code => !result.runnable_tasks.find(runnable_task => runnable_task.activity_code === activity_code));

    if (!_is.filled_array(sequence_activity_codes)) {
      return result;
    }

    const idle_tasks = await this.task_model.aggregate([
      { $match: { activity_code:  { $in: sequence_activity_codes }, status:TaskManager.TASK_CONST.STATUS.IDLE } },
      { $sort: { priority: -1, created_at: 1 } },
      { $group: { _id: '$activity_code', task: { $first: '$$ROOT' } } },
      { $replaceRoot: { newRoot: "$task" } },
      { $sort: { priority: -1, created_at: 1 } },
      { $limit: remain_slot }
    ])

    result.runnable_tasks.push(...idle_tasks);

    return result;
  }

  async startTask({ task }) {
    const result = {
      updated_task: null,
    };

    result.updated_task = await this.task_model.findOneAndUpdate({ _id: task._id, updated_at: task.updated_at }, { $set: { running_at: new Date(), status:TaskManager.TASK_CONST.STATUS.RUNNING } }, { new: true });

    if (!result.updated_task) {
      throw new Error(`Cannot start task because task updated before`);
    }

    return result;
  }

  async finishTask({ task, result: output }) {
    const result = {
      updated_task: null,
    };

    const now = new Date();

    result.updated_task = await this.task_model.findOneAndUpdate({ _id: task._id }, { $set: { status:TaskManager.TASK_CONST.STATUS.FINISHED, result: output, finished_at: now, updated_at: now } }, { new: true });

    return result;
  }

  async failTask({ task, error }) {
    const result = {
      updated_task: null,
    };

    const now = new Date();

    const activity = TaskManager.assertActivity({ code: task.activity_code })

    const status = _is.retry({ error }) && (_is.activity.retryable({ activity }) && (!task.max_retry_times || task.retry_times + 1 < task.max_retry_times) ) ? TaskManager.TASK_CONST.STATUS.TEMPORARILY_FAILED : TaskManager.TASK_CONST.STATUS.FAILED;

    result.updated_task = await this.task_model.findOneAndUpdate({ _id: task._id }, { $set: { status: status, error: _ERR.errorWithoutStack({ error }), failed_at: now, updated_at: now }, $inc: { retry_times: 1 } }, { new: true });

    return result;
  }

  async process({ task }) {
    const { updated_task } = await this.startTask({ task });

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
  TaskMongoStorage: TaskMongoStorage
};