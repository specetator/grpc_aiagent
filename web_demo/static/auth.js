// Demo 会把原始密码交给服务端，由服务端执行 PBKDF2 哈希。
// 公网部署必须使用 HTTPS/WSS，并进一步收紧 token 的浏览器存储策略。

const Auth = {
  KEY_TOKEN: 'spark_token',
  KEY_UID: 'spark_uid',
  KEY_NAME: 'spark_name',

  KEY_ADMIN_TOKEN: 'spark_admin_token',
  KEY_ADMIN_UID: 'spark_admin_uid',

  set: function(uid, token, name) {
    localStorage.setItem(this.KEY_UID, uid);
    localStorage.setItem(this.KEY_TOKEN, token);
    if (name) localStorage.setItem(this.KEY_NAME, name);
  },

  get: function() {
    const uid = localStorage.getItem(this.KEY_UID);
    const token = localStorage.getItem(this.KEY_TOKEN);
    const name = localStorage.getItem(this.KEY_NAME);
    if (uid && token) {
      return { user_id: uid, token: token, name: name };
    }
    return null;
  },

  clear: function() {
    localStorage.removeItem(this.KEY_UID);
    localStorage.removeItem(this.KEY_TOKEN);
    localStorage.removeItem(this.KEY_NAME);
  },

  setAdmin: function(uid, token) {
    localStorage.setItem(this.KEY_ADMIN_UID, uid);
    localStorage.setItem(this.KEY_ADMIN_TOKEN, token);
  },

  getAdmin: function() {
    const uid = localStorage.getItem(this.KEY_ADMIN_UID);
    const token = localStorage.getItem(this.KEY_ADMIN_TOKEN);
    if (uid && token) {
      return { user_id: uid, token: token };
    }
    return null;
  },

  clearAdmin: function() {
    localStorage.removeItem(this.KEY_ADMIN_UID);
    localStorage.removeItem(this.KEY_ADMIN_TOKEN);
  }
};
