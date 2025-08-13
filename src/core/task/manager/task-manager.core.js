const { CronJob } = require('cron');
const schema_validator = require('schema-validator-lib');

const _is = require("../../../utils/share/_is.utils.share");
const _ERR = require("../../../utils/share/_ERR.utils.share");
const _CONST = require("../../../utils/share/_CONST.utils.share");

module.exports = (() => {

  const _private = {
    task_limit: 5,
    activity: {},
    storage: null,
    cron_time: '*/5 * * * * *',
    cron_job: null,
    verbose: false,
    is_test: false
  };


  let WAS_RESET_TASK = false;

  const TaskManager = {
    TASK_CONST: _CONST.TASK,
    start: function({ storage, task_limit = _private.task_limit, cron_time = _private.cron_time, is_test = _private.is_test, verbose = _private.verbose }) {
      if (_private.cron_job) {
        throw new Error(`Already start`);
      }

      if (!_is.filled_array(Object.keys(_private.activity))) {
        throw new Error(`Activity list is empty. Add some activity before starting`);
      }

      _private.storage = storage;
      _private.task_limit = task_limit;
      _private.verbose  = verbose;
      _private.is_test = is_test;

      const { TaskService } = require('../service/task-service.core');

      const cron_job = new CronJob(
        cron_time,
        async function() {
          if (!global['task']) {
            global['task'] = true;

            try {
              if (!WAS_RESET_TASK) {
                await TaskService.resetTask();
                WAS_RESET_TASK = true;
              }

              await TaskService.expiredTask();
  
              await TaskService.process();
            } catch (error) {
              _ERR.log({ error })
            }
            
            global['task'] = false;
          }
        },
        null,
        false,
      );

      if (!is_test) {
        cron_job.start();

        _private.cron_job = cron_job;
      }
    },
    isTest: function() {
      return _private.is_test;
    },
    log: (str) => {
      if (_private.verbose) {
        console.log(str);
      }
    },
    throwError: (reason) => {
      throw new _ERR.ERR(reason);
    },
    throwRetryableError: (reason) => {
      throw new _ERR.TEMPORARILY_ERR(reason);
    },
    getTaskLimit: () => {
      return _private.task_limit;
    },
    assertStorage: () => {
      const storage = _private.storage;
      if (!storage) {
        throw new Error(`Storage required`);
      }
      return storage;
    },
    getActivity: function({ code }) {
      return _private.activity[code];
    },
    assertActivity: function({ code }) {
      const activity = TaskManager.getActivity({ code });
      if (!activity) {
        throw new Error(`Activity not found`);
      }
      return activity;
    },
    processTask: async function({ task }) {
      const activity = TaskManager.assertActivity({ code: task.activity_code });
      return activity.process({ task });
    },
    addActivity: function(input = {
      code,
      process,
      setting: {
        mode: _CONST.TASK.MODE.PARALLEL,
        retryable: true,
        priority: 0,
        max_retry_times: null,
      }
    }) {
      if (_private.cron_job) {
        throw new Error(`Cannot add activity after start`);
      }

      if (_private.activity[input.code]) {
        throw new Error(`Activity ${input.code} already exist`);
      }

      const { code, process, setting } = schema_validator.assert_validate({ code: 'ADD_ACTIVITY', input: input })

      _private.activity[code] = Object.freeze({
        code: code,
        ...setting,
        process: process,
      })
    },
    retryableActivities: function() {
      return Object.values(_private.activity).filter(activity => _is.activity.retryable({ activity }))
    },
    parallelActivities: function() {
      return Object.values(_private.activity).filter(activity => _is.activity.parallel({ activity }))
    },
    sequenceActivities: function() {
      return Object.values(_private.activity).filter(activity => _is.activity.sequence({ activity }))
    },
  };

  schema_validator.schema.type.add({ key: 'integer', handler: {
    convert: ({ value }) => Number(value),
    check: ({ value }) => typeof value === 'number' && value % 1 === 0,
  }})

  schema_validator.schema.type.add({ key: 'function', handler: {
    convert: ({ value }) => value,
    check: ({ value }) => typeof value === 'function',
  }})

  schema_validator.schema.type.add({ key: 'async_function', handler: {
    convert: ({ value }) => value,
    check: ({ value }) => value && value.constructor && value.constructor.name === 'AsyncFunction',
  }})

  schema_validator.compile({
    code: 'ADD_ACTIVITY',
    schema: {
      code: { type: 'string', require: true, nullable: false },
      process: { type: 'async_function', require: true, nullable: true },
      setting: {
        type: 'object',
        default: {},
        properties: {
          mode: { type: 'string', default: TaskManager.TASK_CONST.MODE.PARALLEL, enum: Object.values(TaskManager.TASK_CONST.MODE) },
          retryable: { type: 'boolean', default: true },
          priority: { type: 'integer', default: 0, max: 9 },
          max_retry_times: { type: 'integer', default: null, nullable: true, check: ({ info: { root, field }, value }) => {
            const result = { errors: [] };
            if (value && !root.retryable) {
              result.errors.push({ field, invalid: 'activity unretryable' });
            }
            return result;
          } },
        },
      }
    }
  })

  return {
    TaskManager: TaskManager,
  };
})();