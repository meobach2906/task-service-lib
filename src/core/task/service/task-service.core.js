const _ERR = require('../../../utils/share/_ERR.utils.share');

const { TaskManager } = require('../manager/task-manager.core');

module.exports = (() => {

  const _private = {
  };

  const _public = {
    createTask: async function({ activity_code, input = {} }) {
      const storage = TaskManager.assertStorage();
      return storage.createTask({ activity_code, input });
    },
    expiredTask: async function() {
      const storage = TaskManager.assertStorage();

      return storage.expiredTask();
    },
    resetTask: async function() {
      const retryable_activities = TaskManager.retryableActivities();

      const retryable_activity_codes = retryable_activities.map(activity => activity.code);

      const storage = TaskManager.assertStorage();

      await storage.resetTask({ retryable_activity_codes });
    },
    process: async function() {
      await Promise.all([
        (async () => {
          const parallel_activities = TaskManager.parallelActivities();

          const parallel_activity_codes = parallel_activities.map(activity => activity.code);

          const storage = TaskManager.assertStorage();

          const { runnable_tasks } = await storage.parallelTasks({ parallel_activity_codes: parallel_activity_codes });

          for (const runnable_task of runnable_tasks) {
            try {
              await storage.process({ task: runnable_task });
            } catch (error) {
              TaskManager.log(`[FAILED] [ID: ${String(runnable_task._id)}] [ERROR: ${_ERR.stringify({ error })}]`);
            }
          }
        })(),
        (async () => {
          const sequence_activities = TaskManager.sequenceActivities();

          const sequence_activity_codes = sequence_activities.map(activity => activity.code);

          const storage = TaskManager.assertStorage();

          const { runnable_tasks } = await storage.sequenceTasks({ sequence_activity_codes: sequence_activity_codes });

          for (const runnable_task of runnable_tasks) {
            try {
              await storage.process({ task: runnable_task });
            } catch (error) {
              TaskManager.log(`[FAILED] [ID: ${String(runnable_task._id)}] [ERROR: ${_ERR.stringify({ error })}]`);
            }

          }
        })()
      ]);
    }
  };

  return {
    TaskService: _public,
  };
})();