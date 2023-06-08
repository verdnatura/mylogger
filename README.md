# MySQL and MariaDB data changes auditor

## Enviroment setup

Because a bug with MariaDB wich it's fix is pending to be merged into main
project branch, a *zongji* fork must be cloned into project root directory.
More info at https://github.com/nevill/zongji/issues/143
```text
git clone https://github.com/juan-ferrer-toribio/zongji.git
cd zongji
git checkout fix-143
```

Apply *zongji.sql* script into DB.

Copy *config.json* to *config.local.json* and place your local configuration
there.

Install dependencies.
```text
npm install
```

## Run application

Launch app.
```text
node index.js
```

## Built With

* [Zongji](https://github.com/nevill/zongji)
* [MySQL2](https://github.com/sidorares/node-mysql2#readme)
