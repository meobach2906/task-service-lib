const { TaskServiceFactory } = require('./src/core/task/service/task-service.core');
const { TaskManagerFactory } = require('./src/core/task/manager/task-manager.core');
const TaskStorage = require('./src/core/task/storage');

module.exports = {
  TaskServiceFactory,
  TaskManagerFactory,
  TaskStorage,
};