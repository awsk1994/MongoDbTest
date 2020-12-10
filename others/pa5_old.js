const mongo = require('mongodb');           // TODO: npm install mongodb??
const MongoClient = mongo.MongoClient;
const url = 'mongodb://localhost:27017';

// Helper Function
function queryFn(dbName, tableName, fn){
    MongoClient.connect(url, { useNewUrlParser: true }, (err, client) => {
        if (err) throw err;
        const dbo = client.db(dbName)
        const r = dbo.collection(tableName);
        fn(r, client);
    });
}

// Queries
function q1(r, client){
    r.findOne({name: "Caffe Dante"}, {restaurant_id: 1}, function(err, result){
        if(err) throw err;
        console.log(result);
        client.close();
    });
}

function q2(r, client){
    r.find({name: /.*Steak.*/}, {projection: {_id: 0, restaurant_id: 1, name: 1}})
    .toArray(function(err, result){
        if(err) throw err;
        console.log(result);
        client.close();
    });
}

function q3(r, client){
    r.find({cuisine: {$in: ["Italian", "American"]}, borough: "Brooklyn"}, {projection: {_id: 0, name: 1}})
        .toArray(function(err, result){
            if(err) throw err;
            console.log(result);
            client.close();
        });
}

function q4(r, client){
    let query = [
    {
        "$match" : {
            "cuisine" : /.*America.*/
        }
    },
    {
        "$group" : {
            "_id" : "$borough",
            "totalCnt" : {
                "$sum" : 1
            }
        }
    },
    {
        "$project" : {
            "borough" : 1,
            "totalCnt" : 1
        }
    },
    {
        "$sort" : {
            "totalCnt" : -1
        }
    }];

    r.aggregate(query).toArray(function(err, result){
        if(err) throw err;
        console.log(result);
        client.close();
    });
}

function q5(r, client){
    let query = [
	{
		"$match" : {
			"cuisine" : /.*Chinese.*/,
			"borough" : /.*Manhattan.*/
		}
	},
	{
		"$unwind" : "$grades"
	},
	{
		"$unwind" : "$grades.score"
	},
	{
		"$group" : {
			"_id" : "$_id",
			"totalScore" : {
				"$sum" : "$grades.score"
			},
			"name" : {
				"$first" : "$name"
			}
		}
	},
	{
		"$project" : {
			"name" : 1,
			"totalScore" : 1
		}
	},
	{
		"$sort" : {
			"totalScore" : -1
		}
	},
	{
		"$limit" : 5
	}];
    r.aggregate(query).toArray(function(err, result){
        if(err) throw err;
        console.log(result);
        client.close();
    });
}

function q6(r, client){
    let query = [
        {
            "$unwind" : "$grades"
        },
        {
            "$unwind" : "$grades.score"
        },
        {
            "$match" : {
                "$and" : [
                    {
                        "address.coord" : {
                            "$geoWithin" : {
                                "$box" : [
                                    [
                                        -74,
                                        40.5
                                    ],
                                    [
                                        -73.5,
                                        40.7
                                    ]
                                ]
                            }
                        }
                    },
                    {
                        "grades.score" : {
                            "$gte" : 70
                        }
                    }
                ]
            }
        },
        {
            "$group" : {
                "_id" : "$_id"
            }
        },
        {
            "$project" : {
                "_id" : 1,
                "name" : 1
            }
        }
    ];
    r.aggregate(query).toArray(function(err, result){
        if(err) throw err;
        console.log(result.length);
        client.close();
    });
}

function q7(r, client){
    let query = [
        {"$sort": {"pop": -1}},
        {"$project": {_id: 1, city: 1, state: 1}},
        {"$limit": 10}
    ];
    r.aggregate(query).toArray(function(err, result){
        if(err) throw err;
        console.log(result);
        client.close();
    });
}

function q8(r, client){
    let query = [
        {"$group": {
            "_id": {"city": "$city", "state": "$state"},
            "pop": {"$sum": "$pop"},
            "state": {"$first": "$state"},
            "city": {"$first": "$city"}
        }},
        { "$sort": {"pop": -1}},
        {"$group": {
                "_id": "$state",
                "pop": {"$first": "$pop"},
                "city": {"$first": "$city"},
                "state": {"$first": "$state"}
            }
        },
        {"$project": {city: 1, state: 1, _id: 0}}
    ];
    r.aggregate(query).toArray(function(err, result){
        if(err) throw err;
        console.log(result);
        client.close();
    });
}

function q9(r, client){
    let query = [
        {"$group": {
            "_id": {"city": "$city", "state": "$state"},
            "pop": {"$sum": "$pop"},
            "state": {"$first": "$state"},
            "city": {"$first": "$city"}
        }},
        {"$group": {
                "_id": "$state",
                "avePop": {"$avg": "$pop"},
                "state": {"$first": "$state"},
                "totalPop": {"$sum": "$pop"}
            }
        },
        {"$match": {"avePop": {"$gt": 10000}}},
        {"$project": {
            state: 1, totalPop: 1, _id: 0
        }}
    ];
    r.aggregate(query).toArray(function(err, result){
        if(err) throw err;
        console.log(result);
        client.close();
    });
}

function q10(r, client){
    r.createIndex({loc:"2dsphere"});
    r.find({
        loc:
          { $near :
             {
               $geometry: { type: "Point",  coordinates: [ -70, 40]},
               $maxDistance: 500000
             }
          }
      }, {projection: {_id: 0, city: 1}})
      .limit(5)
      .toArray(function(err, result){
        if(err) throw err;
        console.log(result);
        client.close();
    });
}

// Execution
queryFn("test", "restaurants", q1);
queryFn("test", "restaurants", q2);
queryFn("test", "restaurants", q3);
queryFn("test", "restaurants", q4);
queryFn("test", "restaurants", q5);
queryFn("test", "restaurants", q6);
queryFn("test", "zips", q7);
queryFn("test", "zips", q8);
queryFn("test", "zips", q9);
queryFn("test", "zips", q10);
