const mongo = require('mongodb');
const MongoClient = mongo.MongoClient;
const url = 'mongodb://localhost:27017';

async function execute(){
  const client = await MongoClient.connect(url, { useNewUrlParser: true })
    .catch(err => { console.log(err); });
  if (!client) {
    return;
  }

  try {
    const db = client.db("test");
    let query = await q1(db);
    console.log("q1:");
    console.log(query);
    
    query = await q2(db);
    console.log("q2:");
    console.log(query);

    query = await q3(db);
    console.log("q3:");
    console.log(query);

    query = await q4(db);
    console.log("q4:");
    console.log(query);

    query = await q5(db);
    console.log("q5:");
    console.log(query);

    cursor = await q6(db);
    console.log("q6:");
    console.log(cursor);

    cursor = await q7(db);
    console.log("q7:");
    console.log(cursor);
    
    cursor = await q8(db);
    console.log("q8:");
    console.log(cursor);

    cursor = await q9(db);
    console.log("q9:");
    console.log(cursor);

    cursor = await q10(db);
    console.log("q10:");
    console.log(cursor);
  } catch (err) {
      console.error(err);
  } finally {
      client.close();
  }
};
execute()

// Queries
function q1(db){
  let cursor = db.collection('restaurants').findOne({name: "Caffe Dante"}, {restaurant_id: 1});
  return cursor;
}

function q2(db){
  let cursor = db.collection('restaurants').find({name: /.*Steak.*/}, {projection: {_id: 0, restaurant_id: 1, name: 1}})
    .toArray();
  return cursor;
}

function q3(db){
  let cursor = db.collection('restaurants').find({cuisine: {$in: ["Italian", "American"]}, borough: "Brooklyn"}, {projection: {_id: 0, name: 1}})
    .toArray();
    return cursor;
}

function q4(db){
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

    let cursor = db.collection('restaurants').aggregate(query).toArray();
    return cursor;
}

function q5(db){
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
    let cursor = db.collection('restaurants').aggregate(query).toArray();
    return cursor;
}

function q6(db){
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
    let cursor = db.collection('restaurants').aggregate(query).toArray();
    return cursor;
}

function q7(db){
    let query = [
        {"$sort": {"pop": -1}},
        {"$project": {_id: 1, city: 1, state: 1}},
        {"$limit": 10}
    ];
    let cursor = db.collection('zips').aggregate(query).toArray();
    return cursor;
}

function q8(db){
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
    let cursor = db.collection('zips').aggregate(query).toArray();
    return cursor
}

function q9(db){
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
    let cursor = db.collection('zips').aggregate(query).toArray();
    return cursor;
}

function q10(db){
    db.collection('zips').createIndex({loc:"2dsphere"});
    let cursor = db.collection('zips').find({
        loc:
          { $near :
             {
               $geometry: { type: "Point",  coordinates: [ -70, 40]},
               $maxDistance: 500000
             }
          }
      }, {projection: {_id: 0, city: 1}})
      .limit(5)
      .toArray();
    return cursor;
}
