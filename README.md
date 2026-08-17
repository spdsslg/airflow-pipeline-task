## Screenshot of the DAG from Asset view:
![alt text](https://github.com/spdsslg/airflow-pipeline-task/blob/feature/final_dag.png?raw=true)

## How to run
The pipeline is fully dockerised. There is a Dockerfile that during the build installs all necessary dependencies in particular for MongoDB connection in Airflow
```
docker compose build
docker compose up
```
Should run all the services and then airflow UI will be accessible on default port (8080) of localhost.

After triggering `pipeline_mongo` dag the pipeline will be executed.
MongoDB 27017 port is mapped to 27017 localhost and thus MongoDB container can be queried from localhost

## MongoDb queries:

Top 5 frequently occurring comments:
```JavaScript
db.tiktok_reviews_db.aggregate([ 
    { $match: 
        { "content": { $exists: true, $ne: null } } 
    }, 
    { 
        $unwind: "$content" 
    }, 
    { $group: 
        { _id: "$content", contentCount: { $sum: 1 } } 
    }, 
    { $sort: 
        { contentCount: -1 } 
    }, 
    { $limit: 5 }
])
```

All entries where the “content” field is less than 5 characters long:
```JavaScript
db.tiktok_reviews_db.find({
    content: {$exists: true, $ne: null},
    $expr: { $lt: [{$strLenCP: "$content"}, 5] }
})
```

Average rating for each day:
```JavaScript
db.tiktok_reviews_db.aggregate([ 
    { $match: 
        { 
            "score": { $exists: true, $ne: null } ,
            "at": { $exists: true}
        } 
    }, 
    { $project: 
        {
            reviewYearMonthDay: { $dateToString: { format: "%Y-%m-%d", date: "$at" }},
            "score": 1
        }
    },
    { $group: 
        { _id: "$reviewYearMonthDay", avgScore: { $avg: "$score" } } 
    }, 
    { $sort: 
        { _id: 1 } 
    }
])
```