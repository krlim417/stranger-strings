const cassandra = require('cassandra-driver');
const bluebird = require('bluebird');
const uuidv4 = require('uuid/v4');

const client = new cassandra.Client({ contactPoints: ['127.0.0.1'], keyspace: 'events' });

client.connect((err) => {
  if (err) {
    console.log('Error connecting');
  } else {
    console.log('Connected successfully');
  }
});

var counter = 0;

const randNumGenIncl = (min, max) => Math.floor(Math.random() * ((max - min) + 1));

const createEvents = (numberOfEventsToCreate, numberOfEventsToAdd) => {
  const queries = [];
  while (queries.length < numberOfEventsToCreate) {
    const contentEventType = ['click', 'play', 'pause', 'thumbs up', 'thumbs down'];
    const contentType = ['show', 'movie'];
    const randomUser = randNumGenIncl(0, (numberOfEventsToAdd * 0.3));

    const randomContent = randNumGenIncl(0, 2500);
    const randomContentType = contentType[randNumGenIncl(0, contentType.length)];
    const randomContentEvent = contentEventType[randNumGenIncl(0, contentEventType.length)];

    const newContentEvent = [uuidv4(), randomContentEvent, randomUser, randomContent, randomContentType];
    queries.push(newContentEvent);
    if (randomContentEvent === 'pause') {
      const newPlayEvent = [uuidv4(), 'play', randomUser, randomContent, randomContentType];
      queries.push(newPlayEvent);
    }
  }
  return queries;
};

async function addEvents() {
  const createdEvents = await createEvents(500, 10000);
  saveEvents(createdEvents);
}

async function saveEvents(eventArray) {
  const query = 'INSERT INTO userevents (id_event, event, id_user, id_content, type) VALUES (?,?,?,?,?)';
  const batch = [];
  for (let i = 0; i < eventArray.length; i += 1) {
    batch.push({ query, params: [eventArray[i][0], eventArray[i][1], eventArray[i][2], eventArray[i][3], eventArray[i][4]] });
  }
  await client.batch(batch, { prepare: true }).then(() => {
    counter += 500;
    if (counter <= 10000000) {
      addEvents();
      console.log(counter);
    }
  });
}

addEvents();
