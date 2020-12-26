const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY;
const KAFKA_BROKERS = process.env.KAFKA_BROKERS;
const HOSTNAME = process.env.HOSTNAME; // offered by kubernetes automatically

if (!YOUTUBE_API_KEY || !KAFKA_BROKERS || !HOSTNAME) {
  console.error(`missing environment variables, env: ${JSON.stringify(process.env)}`);
  process.exit(1);
}

const { addExitHook } = require('exit-hook-plus');
const { google } = require('googleapis');
const { Kafka } = require('kafkajs');
const { fetchYoutubeChannelInfo } = require('./lib/fetch-youtube-channel');
const { fetchYoutubeVideoInfo } = require('./lib/fetch-youtube-video');

const youtubeApi = google.youtube({ version: 'v3', auth: YOUTUBE_API_KEY });
const kafka = new Kafka({
  clientId: HOSTNAME,
  brokers: KAFKA_BROKERS.trim().split(',')
});
const consumber = kafka.consumer({ groupId: 'fetch-worker' });
const producer = kafka.producer();

const CONSUME_TOPICS = ['fetch-channel-info', 'fetch-video-info'];

async function init() {
  console.info('connecting to kafka brokers');
  await producer.connect();
  addExitHook(async () => await producer.disconnect());
  await consumber.connect();
  addExitHook(async () => await consumber.disconnect());
  for (const topic of CONSUME_TOPICS) {
    await consumber.subscribe({ topic });
  }

  console.info('start reading messages from kafka');
  await consumber.run({
    eachMessage: async ({ topic, message }) => {
      try {
        const value = JSON.parse(message.value.toString());
        switch (topic) {
          case 'fetch-channel-info':
            await handleFetchChannelInfoTask(value);
            break;
          case 'fetch-video-info':
            await handleFetchVideoInfoTask(value);
            break;
          default:
            console.warn(
              `ignoring unknown topic '${topic}' for message '${message.value.toString()}'`
            );
        }
      } catch (e) {
        console.error(`error while processing task '${message.value.toString()}'`);
        throw e;
      }
    }
  });
}

init();

async function handleFetchChannelInfoTask(task) {
  await producer.send({
    acks: 0,
    topic: 'channel-info',
    messages: [
      {
        value: JSON.stringify({
          meta: task,
          data: await fetchYoutubeChannelInfo(youtubeApi, task.channelId)
        })
      }
    ]
  });
}

async function handleFetchVideoInfoTask(task) {
  await producer.send({
    acks: 0,
    topic: 'video-info',
    messages: [
      {
        value: JSON.stringify({
          meta: task,
          data: await fetchYoutubeVideoInfo(youtubeApi, task.videoId) // TODO: bilibili
        })
      }
    ]
  });
}
