const { TaskService } = require('./src/core/task/service/task-service.core');
const { TaskManager } = require('./src/core/task/manager/task-manager.core');
const TaskStorage = require('./src/core/task/storage');

module.exports = {
  TaskService,
  TaskManager,
  TaskStorage,
};