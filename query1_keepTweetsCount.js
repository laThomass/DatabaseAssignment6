// Creates a tweets count on redis

import { MongoClient } from "mongodb";
import { createClient } from "redis";

console.log("Connecting to Redis");
const redisClient = createClient();

redisClient.on("error", (err) => console.log("Redis Client Error", err));
await redisClient.connect();
console.log("Connected to Redis");


async function getTweets
() {
  const filter = {};
  const sort = {
    id: -1,
  };

  const client = await MongoClient.connect("mongodb://localhost:27017/");
  const coltweets = client.db("ieeevisTweets").collection("tweets");
  const tweetsArray = await coltweets.find(filter, { sort }).toArray();

  await client.close();

  return tweetsArray;
}

async function counttweets() {
  const tweetsArray = await getTweets
  ();
  console.log(`Got ${tweetsArray.length} tweets`);


  await redisClient.set("tweetsCount", 0);

  for (const l of tweetsArray) {
    await redisClient.incr("tweetsCount");  
  }

  const tweetsCount = await redisClient.get("tweetsCount");
  console.log(`There were ${tweetsCount} tweets`);

}

await counttweets();


await redisClient.disconnect();
console.log("Disconnected from Redis");
