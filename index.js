const MyLogger = require('./mylogger');

async function main() {
  const mylogger = new MyLogger()
  await mylogger.start();

  process.on('SIGINT', async function() {
    console.log('Got SIGINT.');
    try {
      await mylogger.stop();
    } catch (err) {
      console.error(err);
    }
    process.exit();
  });
}
main();
