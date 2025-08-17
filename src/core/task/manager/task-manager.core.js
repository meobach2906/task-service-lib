const { CronJob } = require('cron');
const schema_validator = require('schema-validator-lib');

const _is = require("../../../utils/share/_is.utils.share");
const _ERR = require("../../../utils/share/_ERR.utils.share");
const _CONST = require("../../../utils/share/_CONST.utils.share");

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
        mode: { type: 'string', default: _CONST.TASK.MODE.PARALLEL, enum: Object.values(_CONST.TASK.MODE) },
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

const TaskManagerFactory = ({ storage, task_limit = 5, cron_time = '*/5 * * * * *', verbose = false } = {}) => {

  const _private = {
    activity: {},
    storage: storage,
    task_limit: task_limit,
    cron_time: cron_time,
    verbose: verbose,
    cron_job: null,
  };


  let WAS_RESET_TASK = false;

  const _public = {
    TASK_CONST: _CONST.TASK,
    start: function({ task_service }) {

      if (_private.cron_job) {
        throw new Error(`Task manager cron job already start`);
      }

      if (!_is.filled_array(Object.keys(_private.activity))) {
        throw new Error(`Activity list is empty. Add some activity before starting`);
      }

      const cron_job = new CronJob(
        _private.cron_time,
        async function() {
          if (!global['task']) {
            global['task'] = true;

            try {
              if (!WAS_RESET_TASK) {
                await task_service.resetTask();
                WAS_RESET_TASK = true;
              }

              await task_service.expiredTask();
  
              await task_service.process();
            } catch (error) {
              _ERR.log({ error })
            }
            
            global['task'] = false;
          }
        },
        null,
        false,
      );

      cron_job.start();

      _private.cron_job = cron_job;
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
      const activity = _public.getActivity({ code });
      if (!activity) {
        throw new Error(`Activity not found`);
      }
      return activity;
    },
    processTask: async function({ task }) {
      const activity = _public.assertActivity({ code: task.activity_code });
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

  return _public;
};

module.exports = {
  TaskManagerFactory,
};