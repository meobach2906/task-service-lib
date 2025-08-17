const _ERR = require('../../../utils/share/_ERR.utils.share');

const TaskServiceFactory = ({ task_manager }) => {

  const _private = {
  };

  const _public = {
    createTask: async function({ activity_code, input = {} }) {
      const storage = task_manager.assertStorage();
      const activity = task_manager.assertActivity({ code: activity_code })
      return storage.createTask({ activity, input });
    },
    expiredTask: async function() {
      const storage = task_manager.assertStorage();

      return storage.expiredTask();
    },
    resetTask: async function() {
      const retryable_activities = task_manager.retryableActivities();

      const retryable_activity_codes = retryable_activities.map(activity => activity.code);

      const storage = task_manager.assertStorage();

      await storage.resetTask({ retryable_activity_codes });
    },
    processOne: async function({ task, is_waiting = false }) {
      const storage = task_manager.assertStorage();

      const activity = task_manager.assertActivity({ code: task.activity_code })

      const { updated_task } = await storage.startTask({ task });

      updated_task.input = storage.parseStorageObjectField(updated_task.input)

      const promise = task_manager.processTask({ task: updated_task })
        .then(async result => {
          task_manager.log(`[FINISHED] [PROCESS_TASK] [ID: ${String(task._id)}] [RESULT: ${JSON.stringify(result)}]`);
          return await storage.finishTask({ task: updated_task, result });
        })
        .catch(async error => {
          task_manager.log(`[FAILED] [PROCESS_TASK] [ID: ${String(task._id)}] [ERROR: ${_ERR.stringify({ error })}]`);
          return await storage.failTask({ task: updated_task, activity, error });
        })
      
      if (is_waiting) {
        return await promise;
      }
  
      return { updated_task };
    },
    process: async function() {
      const limit = task_manager.getTaskLimit();

      await Promise.all([
        (async () => {
          const parallel_activities = task_manager.parallelActivities();

          const parallel_activity_codes = parallel_activities.map(activity => activity.code);

          const storage = task_manager.assertStorage();

          const { runnable_tasks } = await storage.parallelTasks({ parallel_activity_codes: parallel_activity_codes, limit });

          for (const runnable_task of runnable_tasks) {
            try {
              await this.processOne({ task: runnable_task });
            } catch (error) {
              task_manager.log(`[FAILED] [ID: ${String(runnable_task._id)}] [ERROR: ${_ERR.stringify({ error })}]`);
            }
          }
        })(),
        (async () => {
          const sequence_activities = task_manager.sequenceActivities();

          const sequence_activity_codes = sequence_activities.map(activity => activity.code);

          const storage = task_manager.assertStorage();

          const { runnable_tasks } = await storage.sequenceTasks({ sequence_activity_codes: sequence_activity_codes, limit });

          for (const runnable_task of runnable_tasks) {
            try {
              await this.processOne({ task: runnable_task });
            } catch (error) {
              task_manager.log(`[FAILED] [ID: ${String(runnable_task._id)}] [ERROR: ${_ERR.stringify({ error })}]`);
            }

          }
        })()
      ]);
    }
  };

  return _public;
};

module.exports = {
  TaskServiceFactory,
};