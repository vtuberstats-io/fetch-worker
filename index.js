'use strict';

require('dotenv').config();

const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY;
const KAFKA_BROKERS = process.env.KAFKA_BROKERS;
const CONCURRENCY = Number(process.env.CONCURRENCY) || 8;
const MAX_RETRY = Number(process.env.MAX_RETRY) || 3;
const HOSTNAME = process.env.HOSTNAME; // offered by kubernetes automatically

if (!YOUTUBE_API_KEY) {
  console.error('environment variable YOUTUBE_API_KEY is not specified!');
  process.exit(1);
}
if (!KAFKA_BROKERS) {
  console.error('environment variable KAFKA_BROKERS is not specified!');
  process.exit(1);
}
if (!HOSTNAME) {
  console.error('environment variable HOSTNAME is not specified!');
  process.exit(1);
}

const { google } = require('googleapis');
const { Kafka } = require('kafkajs');
const { default: PQueue } = require('p-queue');

const TOPIC_FETCH_TASK_SCHEDULE = 'fetch-task-schedule';
const TOPIC_FETCH_TASK_DONE = 'fetch-task-done';

const youtubeApi = google.youtube({ version: 'v3', auth: YOUTUBE_API_KEY });
const kafka = new Kafka({
  clientId: HOSTNAME,
  brokers: KAFKA_BROKERS.trim().split(',')
});
const fetchTaskScheduleConsumer = kafka.consumer({ groupId: 'fetch-worker' });
const fetchTaskResultProducer = kafka.producer();
const fetchTaskQueue = new PQueue({ concurrency: CONCURRENCY });
let taskResultsJson = [];

async function init() {
  setInterval(() => pushFinishedTasksToKafka(), 1000 * 60);

  console.log(`connecting to kafka with brokers: ${KAFKA_BROKERS}`);
  await fetchTaskResultProducer.connect();
  await fetchTaskScheduleConsumer.connect();
  await fetchTaskScheduleConsumer.subscribe({ topic: TOPIC_FETCH_TASK_SCHEDULE });
  await doReadScheduleFromKafka();
}

init().catch((err) => console.error(err));

async function pushFinishedTasksToKafka() {
  if (taskResultsJson.length <= 0) {
    return;
  }
  const results = [...taskResultsJson];
  taskResultsJson = [];
  try {
    await fetchTaskResultProducer.send({
      topic: TOPIC_FETCH_TASK_DONE,
      messages: results.map((r) => ({ value: r }))
    });
    console.info(`pushed ${results.length} task results to kafka`);
  } catch (e) {
    console.error(
      `failed to push ${results.length} task results to kafka, will retry in the next run`
    );
    console.error(e);
  }
}

async function doReadScheduleFromKafka() {
  await fetchTaskScheduleConsumer.run({
    eachMessage: async ({ message }) => {
      console.info(`queued task: ${message.value.toString()}`);
      const vtuberMeta = JSON.parse(message.value.toString());
      vtuberMeta.retryCount = 0;
      if (vtuberMeta.youtubeChannelLinks) {
        handleFetchTaskFromYoutube(vtuberMeta);
      }
      if (vtuberMeta.bilibiliChannelLinks) {
        handleFetchTaskFromBilibili(vtuberMeta);
      }
    }
  });
}

function handleFetchTaskFromYoutube(vtuberMeta) {
  queueFetchTask(async () => {
    taskResultsJson.push(
      JSON.stringify({
        type: 'youtube',
        data: await fetchFromYoutube(vtuberMeta.youtubeChannelId)
      })
    );
  });
}

function handleFetchTaskFromBilibili(vtuberMeta) {
  queueFetchTask(async () => {
    taskResultsJson.push(
      JSON.stringify({
        type: 'bilibili',
        data: await fetchFromBilibili(vtuberMeta.bilibiliChannelId)
      })
    );
  });
}

async function fetchFromYoutube(channelId) {
  const response = await youtubeApi.channels.list({
    id: channelId,
    part: 'id,statistics,brandingSettings'
  });
  if (!response?.data?.items) {
    throw new Error(`invalid response: ${JSON.stringify(response)}`);
  }
  return response.data.items[0];
}

async function fetchFromBilibili(channelId) {
  // TODO
}

function queueFetchTask(task) {
  const handler = async (meta) => {
    try {
      await task();
    } catch (e) {
      if (meta.retryCount >= MAX_RETRY) {
        console.error(
          `task '${JSON.stringify(
            meta
          )}' has failed for too many times(${MAX_RETRY}), latest error: '${e}'`
        );
      } else {
        meta.retryCount++;
        console.warn(
          `task '${JSON.stringify(meta)}' failed due to error: '${e}', retrying (${
            meta.retryCount
          }/${MAX_RETRY})`
        );
        fetchTaskQueue.add(() => handler(meta));
      }
    }
  };
  fetchTaskQueue.add(() => handler(vtuberMeta));
}
