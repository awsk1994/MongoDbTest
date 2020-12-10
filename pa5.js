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
    let collection = db.collection('restaurants');
    let cursor = await q1(collection);
    console.log("q1:");
    console.log(cursor);
    
    cursor = await q2(collection);
    console.log("q2:");
    console.log(cursor);

    cursor = await q3(collection);
    console.log("q3:");
    console.log(cursor);

    cursor = await q4(collection);
    console.log("q4:");
    console.log(cursor);

    cursor = await q5(collection);
    console.log("q5:");
    console.log(cursor);

    cursor = await q6(collection);
    console.log("q6:");
    console.log(cursor);

    collection = db.collection('zips');
    cursor = await q7(collection);
    console.log("q7:");
    console.log(cursor);
    
    cursor = await q8(collection);
    console.log("q8:");
    console.log(cursor);

    cursor = await q9(collection);
    console.log("q9:");
    console.log(cursor);

    cursor = await q10(collection);
    console.log("q10:");
    console.log(cursor);
  } catch (err) {
      console.err(err);
  } finally {
      client.close();
  }
};
execute()

// Queries
function q1(r){
  return r.findOne({name: "Caffe Dante"}, {restaurant_id: 1});
}

function q2(r){
  return r.find({name: /.*Steak.*/}, {projection: {_id: 0, restaurant_id: 1, name: 1}})
    .toArray();
}

function q3(r){
  return r.find({cuisine: {$in: ["Italian", "American"]}, borough: "Brooklyn"}, {projection: {_id: 0, name: 1}})
    .toArray();
}

function q4(r){
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

    return r.aggregate(query).toArray();
}

function q5(r){
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
  return r.aggregate(query).toArray();
}

function q6(r){
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
  return r.aggregate(query).toArray();
}

function q7(r){
    let query = [
        {"$sort": {"pop": -1}},
        {"$project": {_id: 1, city: 1, state: 1}},
        {"$limit": 10}
    ];
    return r.aggregate(query).toArray();
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
    return r.aggregate(query).toArray();
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
    return r.aggregate(query).toArray();
}

function q10(r, client){
    r.createIndex({loc:"2dsphere"});
    return r.find({
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
}
