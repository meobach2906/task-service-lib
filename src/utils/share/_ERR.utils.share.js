'use strict';

(() => {

  class ERR extends Error {
    static CODE = 'ERR'
    constructor(props) {
      super();
      this.reactions = ['FIX_DATA'];
      if (props) {
        Object.assign(this, props)
      }
    }
  }

  class TEMPORARILY_ERR extends ERR {
    static CODE = 'ERR'
    constructor(props) {
      super();
      this.reactions = ['RETRY'];
      if (props) {
        Object.assign(this, props)
      }
    }
  }
  
  const _ERR = {
    ERR,
    TEMPORARILY_ERR,
    log({ error }) {
      if (!(error.stack && error.stack.length > 0)) {
        error.stack = new Error().stack;
      }
      console.log(`[ERROR] ${_ERR.stringify({ error })}`);
    },
    makeErrorObject: ({ error, includes = [], ignore_values = [undefined, null, ''], excludes = [] }) => {
      const obj = Object.assign({}, error);

      for (let prop of includes) {
        if (!ignore_values.includes(error[prop])) {
          obj[prop] = error[prop];
        }
      }
    
      for (let prop of excludes) {
        delete obj[prop]
      }

      return obj;
    },
    stringify: ({ error }) => {
      return JSON.stringify(_ERR.makeErrorObject({ error, includes: ['stack', 'message'] }));
    },
    errorWithoutStack: ({ error }) => {
      return _ERR.makeErrorObject({ error, includes: ['message'] });
    }
  };

  if (module && module.exports) {
    module.exports = _ERR;
  } else if (window) {
    _di = window;
    window._ERR = _ERR;
  }
})();