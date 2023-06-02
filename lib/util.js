const fs = require('fs');
const path = require('path');

function loadConfig(dir, configName) {
  const configBase = path.join(dir, 'config', configName);
  const conf = Object.assign({}, require(`${configBase}.yml`));

  const localPath = `${configBase}.local.yml`;
  if (fs.existsSync(localPath)) {
    const localConfig = require(localPath);
    Object.assign(conf, localConfig);
  }

  return conf;
}

function toUpperCamelCase(str) {
  str = str.replace(/[-_ ][a-z]/g,
    match => match.charAt(1).toUpperCase());
  return str.charAt(0).toUpperCase() + str.substr(1);
}

module.exports = {
  loadConfig,
  toUpperCamelCase
}
