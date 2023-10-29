require("dotenv").config();
const { Kafka, Partitioners } = require("kafkajs");

const BROKER_ADDRESS = process.env.KAFKA_SERVER;
const TOPIC_NAME = "test-topic";
const CONSUMER_GROUP_ID = "test-group";

(async () => {
  try {
    const kafkaConfig = {
      clientId: "my-app",
      brokers: [BROKER_ADDRESS],
      connectionTimeout: 10000,
      authenticationTimeout: 10000,
      // logLevel: logLevel.DEBUG,
      ssl: true,
      sasl: {
        mechanism: "plain",
        username: process.env.KAFKA_KEY,
        password: process.env.KAFKA_SECRET,
      },
    };
    const kafka = new Kafka(kafkaConfig);

    const admin = kafka.admin();
    await admin.connect();

    const producer = kafka.producer({
      // not exactly sure what this does but it seems to be required
      createPartitioner: Partitioners.LegacyPartitioner,
    });
    await producer.connect();

    const consumer = kafka.consumer({ groupId: CONSUMER_GROUP_ID });
    await consumer.connect();

    // await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });
    // await consumer.run({
    //   eachMessage: async ({ topic, partition, message }) => {
    //     console.log({
    //       partition,
    //       offset: message.offset,
    //       value: message.value.toString(),
    //     });
    //   },
    // });

    await checkConsumerGroups(admin);
    const topicExists = await getTopicExists(admin, TOPIC_NAME);
    console.log("topic exists:", topicExists);
    // await unstable_createTopicIfNotExist(admin, TOPIC_NAME);

    // await producer.send({
    //   topic: TOPIC_NAME,
    //   messages: [{ value: "Hello KafkaJS user!", key: "key1" }],
    // });

    await producer.disconnect();
    await admin.disconnect();
    await consumer.disconnect();
    process.exit(0);
  } catch (error) {
    console.log(error);
    process.exit(1);
  }
})();

/**
 * @param {import("kafkajs").Admin} adminInstance
 * @param {string} topicName
 */
// eslint-disable-next-line no-unused-vars
async function unstable_createTopicIfNotExist(adminInstance, topicName) {
  const topicExists = await getTopicExists(adminInstance, topicName);
  if (!topicExists) {
    // this seems to throw a KafkaJSConnectionClosedError when creating a topic
    // maybe related to https://github.com/tulios/kafkajs/issues/1231
    // workaround: use confluent cloud web app to create topic
    await adminInstance.createTopics({
      topics: [{ topic: topicName }],
    });
  }
}

/**
 * @param {import("kafkajs").Admin} adminInstance
 * @param {string} topicName
 */
async function getTopicExists(adminInstance, topicName) {
  const topics = await adminInstance.listTopics();
  return topics.includes(topicName);
}

/**
 * @param {import("kafkajs").Admin} adminInstance
 */
async function checkConsumerGroups(adminInstance) {
  const groups = await adminInstance.listGroups();
  const groupIds = groups.groups.map((group) => {
    return group.groupId;
    // group.protocolType also available but not quite sure what that is
  });
  console.log("consumer groups:", groups);
  const describeGroups = await adminInstance.describeGroups(groupIds);
  console.log("describe groups:", describeGroups);
}
