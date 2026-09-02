const fs = require('fs');
const password = fs.readFileSync('/run/secrets/mongo_root_password', 'utf8').replace(/\n+$/, '');
const admin = db.getSiblingDB('admin');

if (!admin.auth('root', password) || admin.runCommand({ ping: 1 }).ok !== 1) quit(2);
