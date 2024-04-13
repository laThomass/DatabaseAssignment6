import { MongoClient } from "mongodb";
import { createClient } from "redis";

console.log("Connecting to Redis");
const redisClient = createClient();

redisClient.on("error", (err) => console.log("Redis Client Error", err));
await redisClient.connect();
console.log("Connected to Redis");

//computes number of different users
async function getTweets() {
    const client = await MongoClient.connect("mongodb://localhost:27017/");
    const coltweets = client.db("ieeevisTweets").collection("tweets");
    const tweetsArray = await coltweets.find({}).toArray();
    await client.close();
    return tweetsArray;
}

async function countDistinctUsers() {
    const tweetsArray = await getTweets();
    console.log(`Got ${tweetsArray.length} tweets`);

    await redisClient.del("screenNames");

    for (const tweet of tweetsArray) {
        if (tweet.user && tweet.user.screen_name) {
            await redisClient.sAdd("screenNames", tweet.user.screen_name);
        }
    }

    const distinctUserCount = await redisClient.sCard("screenNames");
    console.log(`There are ${distinctUserCount} distinct users`);

   
}

// Run the countDistinctUsers function
await countDistinctUsers();

// Disconnect the Redis client
await redisClient.disconnect();
console.log("Disconnected from Redis");
