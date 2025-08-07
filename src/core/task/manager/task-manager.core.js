const cron = require('cron');

const _CONST = require("../../../utils/share/_CONST.utils.share");
const _is = require("../../../utils/share/_is.utils.share");
const _ERR = require("../../../utils/share/_ERR.utils.share");

module.exports = (() => {

  const _private = {
    task_limit: 5,
    activity: {},
    storage: null,
    cron_time: '*/5 * * * * *',
    cron_job: null
  };


  let WAS_RESET_TASK = false;

  const _public = {
    start: function({ cron_time = _private.cron_time }) {
      if (_private.cron_job) {
        throw new Error(`Already start`);
      }

      const TaskService = require('../service/task-service.core');

      _public.assertStorage();

      _private.cron_job = new cron.CronJob({
        cronTime: cron_time,
        async onTick() {
          if (!global[cron]) {
            global[cron] = true;

            try {
              if (!WAS_RESET_TASK) {
                await TaskService.resetTask();
                WAS_RESET_TASK = true;
              }
  
              await TaskService.process();
            } catch (error) {
              console.log(`[ERROR: ${_ERR.stringify({ error })}]`);
            }
            
            global[cron] = false;
          }
        },
        start: false
      });

      _private.cron_job.start();
    },
    setTaskLimit: (limit) => {
      _private.task_limit = limit;
    },
    getTaskLimit: () => {
      return _private.task_limit;
    },
    setStorage: ({ storage }) => {
      _private.storage = storage;
    },
    getStorage: () => {
      return _private.storage;
    },
    assertStorage: () => {
      const storage = _public.getStorage();
      if (!storage) {
        throw new Error(`Storage required`);
      }
      return storage;
    },
    getActivity: function({ activity_code }) {
      return _private.activity[activity_code];
    },
    assertActivity: function({ activity_code }) {
      const activity = _public.getActivity();
      if (!activity) {
        throw new Error(`Activity required`);
      }
      return activity;
    },
    processTask: async function({ task }) {
      const activity = _public.assertActivity({ activity_code: task.activity_code });
      return activity.process({ task });
    },
    addActivity: function({
      activity_code,
      process,
      setting = {
        mode: _CONST.TASK.MODE.PARALLEL,
        resetable: true,
        retryable: true,
        priority: 0,
      }
    }) {
      if (_private.activity[activity_code]) {
        throw new Error(`Activity ${activity_code} already exist`);
      }

      _private.activity[activity_code] = Object.freeze({
        activity_code: activity_code,
        ...setting,
        process: async ({ task, tasks = [] }) => {
          return process({ task, tasks })
        }
      })
    },
    resetableActivities: function() {
      return Object.values(_private.activity).filter(activity => _is.activity.resetable({ activity }))
    },
    parallelActivities: function() {
      return Object.values(_private.activity).filter(activity => _is.activity.parallel({ activity }))
    },
    sequenceActivities: function() {
      return Object.values(_private.activity).filter(activity => _is.activity.sequence({ activity }))
    },
  };

  return _public;
})();