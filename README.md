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