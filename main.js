const MongoClient = require('mongodb').MongoClient;
const { S3 } = require('aws-sdk');

const mongoURL = 'mongodb://localhost:27017/Meta-data';

// AWS S3 configuration
const s3 = new S3({
  accessKeyId: 'your-access-key-id', // please enter your aws access-key-id
  secretAccessKey: 'your-secret-access-key', // please enter your aws secret-access-key
});

const dateToCheck = new Date(ISODate("2021-11-18T00:00:00.000Z"));

const aggregationPipeline =     [
  {
    $match: {
      $or: [
        {
          $and: [
            {
              'moods.date': {
                $gte: dateToCheck, // Check mood records for the target date or later
              },
            },
            {
              'moods.date': {
                $lt: new Date(dateToCheck.getTime() + 24 * 60 * 60 * 1000), // Next day
              },
            },
          ],
        },
        {
          $and: [
            {
              'activities.date': {
                $gte: dateToCheck, // Check activity records for the target date or later
              },
            },
            {
              'activities.date': {
                $lt: new Date(dateToCheck.getTime() + 24 * 60 * 60 * 1000), // Next day
              },
            },
          ],
        },
        {
          $and: [
            {
              'sleepData.date': {
                $gte: dateToCheck, // Check sleep records for the target date or later
              },
            },
            {
              'sleepData.date': {
                $lt: new Date(dateToCheck.getTime() + 24 * 60 * 60 * 1000), // Next day
              },
            },
          ],
        },
      ],
    },
  },
  {
    $lookup: {
      from: 'User-mood',
      localField: '_id',
      foreignField: 'user',
      as: 'moods',
    },
  },
  {
    $lookup: {
      from: 'activity',
      localField: '_id',
      foreignField: 'User',
      as: 'activities',
    },
  },
  {
    $lookup: {
      from: 'sleep',
      localField: '_id',
      foreignField: 'User',
      as: 'sleepData',
    },
  },
  {
    $project: {
      _id: 0,
      userId: 1,
      name:1,
      mood_score: "$moods.mood_score",
      activity: "$activities.activity",
      steps: "$activities.steps",
      distance: "$activities.distance",
      duration: "$activities.duration",
      sleep_score: "sleepData.sleep_score",
      hours_of_sleep: "sleepData.hours_of_sleep",
      hours_in_bed: "sleepData.hours_in_bed",
    },
  },
]


async function main() {
  try {
    const client = await MongoClient.connect(mongoURL, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });

    const db = client.db();

    // Use the aggregation pipeline to transform the data
    const result = await db.collection('user').aggregate(aggregationPipeline).toArray();

    // Convert the result to JSON and upload it to S3
    const jsonData = JSON.stringify(result);

    const params = {
      Bucket: 'your-s3-bucket-name',
      Key: 'data.json',
      Body: jsonData,
    };

    await s3.upload(params).promise();

    console.log('Data uploaded to S3 successfully.');

    client.close();
  } catch (error) {
    console.error('Error:', error);
  }
}

main();
