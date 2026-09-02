const fs = require('fs');
const secret = (name) => fs.readFileSync(`/run/secrets/${name}`, 'utf8').replace(/\n+$/, '');

const ensureUser = (database, user, password, roles) => {
  const target = db.getSiblingDB(database);
  if (target.getUser(user) == null) target.createUser({ user, pwd: password, roles });
};

const admin = db.getSiblingDB('admin');
const rootPassword = secret('mongo_root_password');
if (admin.getUser('root') == null) {
  admin.createUser({ user: 'root', pwd: rootPassword, roles: [{ role: 'root', db: 'admin' }] });
}
if (!admin.auth('root', rootPassword)) throw new Error('Mongo root 인증 실패');
ensureUser('LibreChat', 'librechat_app', secret('librechat_mongo_password'),
  [{ role: 'readWrite', db: 'LibreChat' }]);
ensureUser('energy_mcp', 'energy_mcp_app', secret('energy_mcp_mongo_password'),
  [{ role: 'readWrite', db: 'energy_mcp' }]);
