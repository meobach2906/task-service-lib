const { TaskMongoStorage } = require('../storage/mongo-storage.task.core');
const { TaskSQLStorage } = require('../storage/sql-storage.task.core');
const { TaskRedisStorage } = require('../storage/redis-storage.task.core');

module.exports = {
  TaskMongoStorage: TaskMongoStorage,
  TaskSQLStorage: TaskSQLStorage,
  TaskRedisStorage: TaskRedisStorage,
};