'use strict';

(() => {
  const _is = {};
  const _di = {};

  _is.filled_array = (value) => {
    return Array.isArray(value) && value.length > 0;
  }

  _is.retry = ({ error }) => {
    if (error && error.is_retry) {
      return true;
    }
    if(error && error.reactions && _is.filled_array(error.reactions) && error.reactions.includes('RETRY')) {
      return true;
    }
    return false;
  }

  _is.activity = {};

  _is.activity.retryable = ({ activity }) => {
    return activity.retryable;
  }

  _is.activity.unretryable = ({ activity }) => {
    return !activity.retryable;
  }

  _is.activity.parallel = ({ activity }) => {
    return activity.mode === _di._CONST.TASK.MODE.PARALLEL;
  }

  _is.activity.sequence = ({ activity }) => {
    return activity.mode === _di._CONST.TASK.MODE.SEQUENCE;
  }

  _is.activity.batch = ({ activity }) => {
    return activity.mode === _di._CONST.TASK.MODE.BATCH;
  }

  if (module && module.exports) {   
    _di._CONST = require('./_CONST.utils.share');
    module.exports = _is;
  } else if (window) {
    _di = window;
    window._is = _is;
  }
})();