'use strict';

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
const AutoRetryQueue = require('./lib/queue');

const TOPIC_FETCH_TASK_SCHEDULE = 'fetch-task-schedule';
const TOPIC_FETCH_TASK_DONE = 'fetch-task-done';

const youtubeApi = google.youtube({ version: 'v3', auth: YOUTUBE_API_KEY });
const kafka = new Kafka({
  clientId: HOSTNAME,
  brokers: KAFKA_BROKERS.trim().split(',')
});
const fetchTaskScheduleConsumer = kafka.consumer({ groupId: 'fetch-worker' });
const fetchTaskResultProducer = kafka.producer();
const fetchTaskQueue = new AutoRetryQueue(CONCURRENCY, MAX_RETRY);
let taskResultsJson = [];

async function init() {
  setInterval(() => pushFinishedTasksToKafka(), 1000 * 60);

  console.info(`connecting to kafka with brokers: ${KAFKA_BROKERS}`);
  await fetchTaskResultProducer.connect();
  await fetchTaskScheduleConsumer.connect();
  await fetchTaskScheduleConsumer.subscribe({ topic: TOPIC_FETCH_TASK_SCHEDULE });

  console.info('start reading scheduled tasks')
  await doReadTaskFromKafka();
}

init().catch((err) => console.error(err));

async function doReadTaskFromKafka() {
  await fetchTaskScheduleConsumer.run({
    eachMessage: async ({ message }) => {
      const fetchTask = JSON.parse(message.value.toString());
      let hit = false;
      if (fetchTask.vtuberMeta.youtubeChannelId) {
        hit = true;
        console.info(`queued youtube fetching task '${message.value.toString()}'`);
        doFetchTask(fetchTask, 'youtube');
      }
      if (fetchTask.vtuberMeta.bilibiliChannelId) {
        hit = true;
        console.info(`queued bilibili fetching task '${message.value.toString()}'`);
        doFetchTask(fetchTask, 'bilibili');
      }
      if (!hit) {
        console.warn(`task '${message.value.toString()}' has no valid channel ids, skipping`);
      }
    }
  });
}

function doFetchTask(task, type) {
  fetchTaskQueue.enqueue(
    async () => {
      taskResultsJson.push(
        JSON.stringify({
          scheduledTimestamp: task.scheduledTimestamp,
          type,
          vtuberMeta: task.vtuberMeta,
          data:
            type === 'youtube'
              ? await fetchFromYoutube(task.vtuberMeta.youtubeChannelId)
              : await fetchFromBilibili(task.vtuberMeta.bilibiliChannelId)
        })
      );
    },
    (e) =>
      console.error(
        `${type} fetching task for '${task.vtuberMeta.vtuberId}' has failed for too many times(${MAX_RETRY}), latest error: '${e}'`
      )
  );
}

async function fetchFromYoutube(channelId) {
  const response = await youtubeApi.channels.list({
    id: channelId,
    part: 'id,statistics,brandingSettings'
  });
  if (!response?.data?.items) {
    throw new Error(`invalid youtube response: ${JSON.stringify(response)}`);
  }
  return response.data.items[0];
}

async function fetchFromBilibili(channelId) {
  // TODO
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
      topic: TOPIC_FETCH_TASK_DONE,
      messages: results.map((r) => ({ value: r }))
    });
    console.info(`pushed ${results.length} task results to kafka`);
  } catch (e) {
    taskResultsJson = [...taskResultsJson, ...results];
    console.error(
      `failed to push ${results.length} task results to kafka, will retry in the next run`
    );
    console.error(e);
  }
}
