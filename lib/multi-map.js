module.exports = class MultiMap {
  constructor() {
    this.map = new Map();
  }

  set(key, subKey, value) {
    let subKeys = this.map.get(key);
    if (!subKeys) this.map.set(key, subKeys = new Map());
    subKeys.set(subKey, value);
  }

  setIfEmpty(key, subKey, value) {
    if (!this.has(key, subKey))
      this.set(key, subKey, value);
  }

  get(key, subKey) {
    return this.map.get(key)?.get(subKey);
  }

  has(key, subKey) {
    const subMap = this.map.get(key);
    return subMap && subMap.has(subKey);
  }

  delete(key, subKey) {
    const subMap = this.map.get(key);
    if (subMap) subMap.delete(subKey);
  }

  clear() {
    for (const subMap of this.map.values())
      subMap.clear();
    this.map.clear();
  }

  *keys() {
    for (const [key, subMap] of this.map)
    for (const subKey of subMap.keys())
      yield [key, subKey];
  }

  *values() {
    for (const subMap of this.map.values())
    for (const value of subMap.values())
      yield value;
  }

  *entries() {
    for (const [key, subMap] of this.map)
    for (const [subKey, value] of subMap)
      yield [key, subKey, value];
  }

  [Symbol.iterator]() {
    return this.entries();
  };
}
