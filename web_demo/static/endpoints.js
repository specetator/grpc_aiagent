(() => {
  const sameOrigin = window.SparkEndpointConfig?.sameOrigin === true;
  const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:';
  window.SparkEndpoints = Object.freeze({
    apiBase: sameOrigin ? location.origin : location.protocol + '//' + location.hostname + ':9101',
    websocket: scheme + '//' + (sameOrigin ? location.host : location.hostname + ':9000') + '/ws'
  });
})();
