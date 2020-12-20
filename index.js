const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY;
const KAFKA_BROKERS = process.env.KAFKA_BROKERS;
const CONCURRENCY = Number(process.env.CONCURRENCY) || 8;
const MAX_RETRY = Number(process.env.MAX_RETRY) || 3;
const HOSTNAME = process.env.HOSTNAME; // offered by kubernetes automatically

if (!YOUTUBE_API_KEY || !KAFKA_BROKERS || !HOSTNAME) {
  console.error(`missing environment variables, env: ${JSON.stringify(process.env)}`);
  process.exit(1);
}

const { registerExitHook } = require('./lib/exit-hook');
const { google } = require('googleapis');
const { Kafka } = require('kafkajs');
const AutoRetryQueue = require('./lib/queue');
const { fetchYoutubeChannelInfo } = require('./lib/fetch-youtube-channel');
const { fetchBilibiliChannelInfo } = require('./lib/fetch-bilibili-channel');

const TOPIC_FETCH_CHANNEL_INFO = 'fetch-channel-info';
const TOPIC_FETCH_DONE = 'fetch-done';

const youtubeApi = google.youtube({ version: 'v3', auth: YOUTUBE_API_KEY });
const kafka = new Kafka({
  clientId: HOSTNAME,
  brokers: KAFKA_BROKERS.trim().split(',')
});
const fetchTaskScheduleConsumer = kafka.consumer({ groupId: 'channel-worker' });
const fetchTaskResultProducer = kafka.producer();
const fetchTaskQueue = new AutoRetryQueue(CONCURRENCY, MAX_RETRY);
let taskResultsJson = [];

async function init() {
  setInterval(() => pushFinishedTasksToKafka(), 1000 * 60);

  console.info('connecting to kafka brokers');
  await fetchTaskResultProducer.connect();
  await fetchTaskScheduleConsumer.connect();
  await fetchTaskScheduleConsumer.subscribe({ topic: TOPIC_FETCH_CHANNEL_INFO });

  console.info('start reading scheduled tasks');
  await fetchTaskScheduleConsumer.run({
    eachMessage: async ({ message }) => doFetchTask(JSON.parse(message.value.toString()))
  });
}

registerExitHook(
  async () => {
    await fetchTaskScheduleConsumer.disconnect();
  },
  async () => {
    await fetchTaskResultProducer.disconnect();
  }
);

init();

function doFetchTask(task) {
  fetchTaskQueue.enqueue(
    async () => {
      taskResultsJson.push(
        JSON.stringify({
          ...task,
          data:
            task.domain === 'youtube'
              ? await fetchYoutubeChannelInfo(youtubeApi, task.channelId)
              : await fetchBilibiliChannelInfo(task.channelId)
        })
      );
    },
    (e) =>
      console.error(
        `${task.domain} fetching task for '${task.id}(${task.channelId})' has failed for too many times(${MAX_RETRY}), latest error: ${e.stack}`
      )
  );
}

async function pushFinishedTasksToKafka() {
  if (taskResultsJson.length <= 0) {
    return;
  }
  const results = [...taskResultsJson];
  taskResultsJson = [];
  try {
    await fetchTaskResultProducer.send({
      acks: -1,
      topic: TOPIC_FETCH_DONE,
      messages: results.map((r) => ({ value: r }))
    });
    console.info(`pushed ${results.length} task results to kafka`);
  } catch (e) {
    taskResultsJson = [...taskResultsJson, ...results];
    console.error(
      `failed to push ${results.length} task results to kafka, will retry in the next run`
    );
    console.error(e.stack);
  }
}
