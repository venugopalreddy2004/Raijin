import express = require("express");
import redis = require("redis");


const app = express();
app.use(express.json())

const redis_host = process.env.REDIS_HOST || 'localhost';
const redis_port = process.env.REDIS_PORT || 6379;

const client = redis.createClient({
    url: `redis://${redis_host}:${redis_port}`
});

client.on('error', (err) => console.error('Redis Client Error:', err));
client.connect();


app.post('/submit',async (req,res)=>{
    try{
        const data = req.body;
        const userId = data.userId;
        const jobId = data.jobId;
        const s3Address = data.s3Address;
        
        console.log(userId, jobId, s3Address);
        res.send("Job submitted to queue!!");
        try{
            await client.lPush("workQueue", JSON.stringify({userId, jobId, s3Address}));
            console.log("Successfully added to queue!!");
        }catch(e){
            console.log(e);
        }
    }catch(err){
        console.log(err);
        res.status(400).send("Bad request");
    }

})

app.listen(8081, ()=>{
    console.log("Server is listen at port 8081")
});