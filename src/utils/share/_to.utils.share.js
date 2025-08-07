'use strict';

(() => {
  const _to = {};

  if (module && module.exports) {
    module.exports = _to;
  } else if (window) {
    _di = window;
    window._to = _to;
  }
})();