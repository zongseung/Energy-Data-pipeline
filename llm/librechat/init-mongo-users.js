const fs = require('fs');
const secret = (name) => {
  const value = fs.readFileSync(`/run/secrets/${name}`, 'utf8').replace(/(?:\r?\n)+$/, '');
  if (value.length === 0) throw new Error(`필수 secret 파일이 비어 있습니다: ${name}`);
  return value;
};
const secrets = Object.fromEntries([
  'mongo_root_password',
  'librechat_mongo_password',
  'energy_mcp_mongo_password',
].map((name) => [name, secret(name)]));

const ensureUser = (database, user, password, roles) => {
  const target = db.getSiblingDB(database);
  if (target.getUser(user) == null) target.createUser({ user, pwd: password, roles });
};

const admin = db.getSiblingDB('admin');
const rootPassword = secrets.mongo_root_password;
const authenticateRoot = () => {
  try {
    return Boolean(admin.auth('root', rootPassword));
  } catch (_) {
    return false;
  }
};

if (!authenticateRoot()) {
  let root;
  try {
    root = admin.getUser('root');
  } catch (_) {
    throw new Error('Mongo root 인증 실패');
  }
  if (root != null) throw new Error('Mongo root 인증 실패');
  admin.createUser({ user: 'root', pwd: rootPassword, roles: [{ role: 'root', db: 'admin' }] });
  if (!authenticateRoot()) throw new Error('Mongo root 인증 실패');
}
ensureUser('LibreChat', 'librechat_app', secrets.librechat_mongo_password,
  [{ role: 'readWrite', db: 'LibreChat' }]);
ensureUser('energy_mcp', 'energy_mcp_app', secrets.energy_mcp_mongo_password,
  [{ role: 'readWrite', db: 'energy_mcp' }]);
