import { MongoClient } from "mongodb";
import { createClient } from "redis";

console.log("Connecting to Redis");
const redisClient = createClient();

redisClient.on("error", (err) => console.log("Redis Client Error", err));
await redisClient.connect();
console.log("Connected to Redis");

//computes total number of favorites in dataset
async function getTweets() {
    const client = await MongoClient.connect("mongodb://localhost:27017/");
    const coltweets = client.db("ieeevisTweets").collection("tweets");
    const tweetsArray = await coltweets.find({}).toArray();
    await client.close();
    return tweetsArray;
}

async function countFavorites() {
    const tweetsArray = await getTweets();
    console.log(`Got ${tweetsArray.length} tweets`);

    await redisClient.set("favoritesSum", 0);

    for (const tweet of tweetsArray) {
        await redisClient.incrBy("favoritesSum", tweet.favorite_count || 0);
    }

    const favoritesCount = await redisClient.get("favoritesSum");
    console.log(`Total number of favorites: ${favoritesCount}`);
}

await countFavorites();

await redisClient.disconnect();
console.log("Disconnected from Redis");
