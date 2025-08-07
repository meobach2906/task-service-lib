const _ = require('lodash');
const _is = require('../../../utils/share/_is.utils.share');
const _to = require('../../../utils/share/_to.utils.share');
const TaskManager = require('../manager/task-manager.core');

module.exports = (() => {

  const _private = {
  };

  const _public = {
    resetTask: async function() {
      const unresetable_activities = TaskManager.resetableActivities();

      const unresetable_activity_codes = unresetable_activities.map(activity => activity.code);

      const storage = TaskManager.assertStorage();

      await storage.resetTask({ unresetable_activity_codes });
    },
    process: async function() {
      await Promise.all([
        async () => {
          const parallel_activities = TaskManager.parallelActivities();

          const parallel_activity_codes = parallel_activities.map(activity => activity.code);

          const storage = TaskManager.assertStorage();

          const { runnable_tasks } = await storage.parallelTasks({ parallel_activity_codes: parallel_activity_codes });

          for (const runnable_task of runnable_tasks) {
            try {
              await storage.process({ task: runnable_task });
            } catch (error) {
              console.log(`[FAILED] [ID: ${String(runnable_task._id)}]`);
            }
          }
        },
        async () => {
          const sequence_activities = TaskManager.sequenceActivities();

          const sequence_activity_codes = sequence_activities.map(activity => activity.code);

          const storage = TaskManager.assertStorage();

          const { runnable_tasks } = await storage.sequenceTasks({ sequence_activity_codes: sequence_activity_codes });

          for (const runnable_task of runnable_tasks) {
            try {
              await storage.process({ task: runnable_task });
            } catch (error) {
              console.log(`[FAILED] [ID: ${String(runnable_task._id)}]`);
            }

          }
        }
      ]);
    }
  };

  return _public;
})();