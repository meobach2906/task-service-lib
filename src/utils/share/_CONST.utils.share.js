'use strict';

(() => {

  const _CONST = {
    TASK: {
      STORAGE: {
        MONGO: 'MONGO',
        SQL: 'SQL',
        REDIS: 'REDIS'
      },
      STATUS: {
        IDLE: 'IDLE',
        RUNNING: 'RUNNING',
        FAILED: 'FAILED',
        TEMPORARILY_FAILED: 'TEMPORARILY_FAILED',
        FINISHED: 'FINISHED',
      },
      MODE: {
        PARALLEL: 'PARALLEL',
        SEQUENCE: 'SEQUENCE',
        BATCH: 'BATCH',
      }
    }
  };

  if (module && module.exports) {
    module.exports = _CONST;
  } else if (window) {
    _di = window;
    window._CONST = _CONST;
  }
})();