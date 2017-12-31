const cassandra = require('cassandra-driver');

const client = new cassandra.Client({ contactPoints: ['127.0.0.1'], keyspace: 'events' });

client.connect((err) => {
  if (err) {
    console.log('Error connecting');
  } else {
    console.log('Connected successfully');
  }
});

const createDataTable = (tableName) => {
  const query = 'CREATE TABLE IF NOT EXISTS testing (id_event text PRIMARY KEY, event varchar, id_user int, id_content int, type varchar);';

  client.execute(query, (err) => {
    if (err) {
      console.log(`Failed to create '${tableName}' table, ${err}`);
    } else {
      console.log(`Successfully created '${tableName}' table`);
    }
  });
};

createDataTable('testing');
