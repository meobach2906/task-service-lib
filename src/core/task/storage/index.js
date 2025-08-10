const { TaskMongoStorage } = require('../storage/mongo-storage.task.core');
const { TaskSQLStorage } = require('../storage/sql-storage.task.core');

module.exports = {
  TaskMongoStorage: TaskMongoStorage,
  TaskSQLStorage: TaskSQLStorage,
};